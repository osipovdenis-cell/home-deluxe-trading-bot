"""Independent post-close worker. No order methods, shared AI locks or trading writes."""
import json
import os
import threading
import time
from dataclasses import dataclass
from bot import idea_store
from bot.idea_data import collect, readonly
from bot.idea_evaluation import evaluate, week_start, PROTOCOL
from bot.idea_rules import FEATURES

PROMPT='''You analyse closed rocket trades, never make trading decisions. Return only JSON
with trade_id (integer), summary (2-3 sentences in Russian), ideas (0-3 items).
Each idea has exactly type, condition, action, rationale (Russian).
Condition is AND of 1-5 objects {feature,op,value}; op is >,<,>=,<=,== and value finite numeric.
Use ONLY non-null entry.features values. Never use symbol identity, post-entry price,
future extrema or hindsight as predicates. After-entry data is explanation only, not evidence of a rule.
Types/actions: entry_filter: {"skip":true}; exit_params: {"stop_percent":X,"protect_percent":Y,"trail_pp":Z},
positive percentages .05..30; entry_delay: {"wait_seconds":integer 1..3600}, fixed delay only when initial
condition matches (no invented future condition). signal_type: 1 anomaly, 0 ordinary leader;
ai_decision_code: -1 SKIP, 0 WAIT, 1 BUY; confirmation/filter: 1 passed, 0 failed, null unknown.
Return empty ideas if no defensible proposal. Do not invent missing data or request execution.
Allowed features: '''+', '.join(FEATURES)

@dataclass(frozen=True)
class Config:
    enabled: bool = True
    daily_requests: int = 24
    daily_usd: float = 1.
    input_usd_per_million: float = 0.
    output_usd_per_million: float = 0.
    max_output_tokens: int = 1800
    max_input_bytes: int = 120000

    @classmethod
    def env(cls):
        c=cls(enabled=os.getenv('ROCKET_IDEAS_ENABLED','true').lower()=='true',
            daily_requests=int(os.getenv('ROCKET_IDEAS_DAILY_REQUESTS','24')),
            daily_usd=float(os.getenv('ROCKET_IDEAS_DAILY_USD','1')),
            input_usd_per_million=float(os.getenv('ROCKET_IDEAS_INPUT_USD_PER_MILLION','0')),
            output_usd_per_million=float(os.getenv('ROCKET_IDEAS_OUTPUT_USD_PER_MILLION','0')))
        import math
        if any(not math.isfinite(v) or v<0 for v in (c.daily_requests,c.daily_usd,c.input_usd_per_million,c.output_usd_per_million)):
            raise ValueError('invalid idea budget')
        return c


def weekly_message(db, results, boundary):
    counts=dict(db.execute('SELECT status,COUNT(*) FROM reviews GROUP BY status'))
    new=db.execute('SELECT COUNT(*) FROM ideas WHERE created>=? AND created<?',(boundary-604800,boundary)).fetchone()[0]
    total=db.execute('SELECT COUNT(*) FROM ideas').fetchone()[0]
    out=[f'🔎 Идеи ракет — недельная проверка (UTC {time.strftime("%d.%m",time.gmtime(boundary))})',
         f'Разобрано: {counts.get("done",0)}; ждут: {counts.get("pending",0)}. Новых идей: {new}; в журнале: {total}.']
    for r in sorted(results,key=lambda r:r['metrics']['delta_sum_percent'],reverse=True)[:5]:
        m=r['metrics'];label={'passed':'прошла: кандидат в тень','failed':'не прошла','insufficient_data':'мало данных'}[m['status']]
        ci=m['bootstrap95'];interval='нет' if ci is None else f'[{ci[0]:+.3f}; {ci[1]:+.3f}]'
        mean='нет' if m['delta_mean_percent'] is None else f"{m['delta_mean_percent']:+.3f}"
        out.append(f'#{r["id"]}: {label}; затронуто {m["affected"]}/{m["events"]}; Δ суммы {m["delta_sum_percent"]:+.2f} п.п.; Δ/сделку {mean}; 95% {interval}; без 2 лучших {m["without_best2_delta"]:+.2f} п.п.')
        if m['candidate_for_shadow']:out.append(json.dumps(r['rule'],ensure_ascii=False)[:380])
    repeats=list(db.execute('SELECT id,repeats FROM ideas ORDER BY repeats DESC,id LIMIT 5'))
    out.append('Частые идеи: '+(', '.join(f'#{r[0]} ×{r[1]}' for r in repeats) or 'пока нет'))
    out.append('Только ретроспективный анализ. Это не доходность банка. Параметры торговли не меняются. Полные правила и исключения — в файле отчёта.')
    return '\n'.join(out)[:3900]


class IdeaWorker:
    def __init__(self,path,api_key,model,send=None,config=None,analyse=None):
        self.path=path;self.api_key=api_key;self.model=model;self.send=send
        self.config=config or Config.env();self.analyse=analyse;self.stop=threading.Event();self.thread=None

    def start(self):
        if self.config.enabled:
            self.thread=threading.Thread(target=self.run,name='rocket-ideas',daemon=True);self.thread.start()

    def close(self):
        self.stop.set()
        if self.thread:self.thread.join(timeout=2)

    def request(self,text):
        if self.analyse:return self.analyse(text)
        from bot.ai import AIAnalyst
        # A private HTTP client; never wait on the entry analyst's lock/backoff.
        ai=AIAnalyst(self.api_key,self.model)
        try:
            response=ai._post('/v1/responses',json=dict(model=self.model,store=False,
                instructions=PROMPT,input=text,max_output_tokens=self.config.max_output_tokens,
                text={'format':{'type':'json_object'}}))
            response.raise_for_status()
            return json.loads(ai._extract_output_text(response.json()))
        finally:ai.close()

    def review_one(self,db,now):
        c=self.config
        if not (self.api_key or self.analyse) or c.input_usd_per_million<=0 or c.output_usd_per_million<=0:
            with db:db.execute("INSERT OR REPLACE INTO meta VALUES('ai_status',?)",('paused: configure API key and explicit token price ceilings',))
            return
        row=db.execute("SELECT * FROM reviews WHERE status='pending' AND retry_at<=? ORDER BY created,trade_id LIMIT 1",(now,)).fetchone()
        if row is None:return
        text=row['payload'];size=len((PROMPT+text).encode('utf-8'))
        # One token per UTF-8 byte plus framing allowance is a conservative reservation.
        reserve=((size+2048)*c.input_usd_per_million+c.max_output_tokens*c.output_usd_per_million)/1e6
        start=int(now//86400)*86400
        count,spent=db.execute('SELECT COUNT(*),COALESCE(SUM(reserved_usd),0) FROM requests WHERE at>=?',(start,)).fetchone()
        if size>c.max_input_bytes:
            with db:db.execute("UPDATE reviews SET error='payload exceeds input limit; data retained',retry_at=? WHERE trade_id=?",(now+86400,row['trade_id']))
            return
        if count>=c.daily_requests or spent+reserve>c.daily_usd:
            with db:db.execute("INSERT OR REPLACE INTO meta VALUES('ai_status','daily budget exhausted')")
            return
        with db:
            request_id=db.execute('INSERT INTO requests(at,reserved_usd,status) VALUES(?,?,?)',(now,reserve,'reserved')).lastrowid
            db.execute('UPDATE reviews SET attempts=attempts+1,retry_at=? WHERE trade_id=?',(now+300,row['trade_id']))
        try:
            response=self.request(text)
            idea_store.finish(db,row['trade_id'],response,now)
            with db:
                db.execute("UPDATE requests SET status='done' WHERE id=?",(request_id,))
                db.execute("INSERT OR REPLACE INTO meta VALUES('ai_status','running')")
        except Exception as error:
            with db:
                db.execute("UPDATE requests SET status='failed' WHERE id=?",(request_id,))
                db.execute('UPDATE reviews SET error=?,retry_at=? WHERE trade_id=?',
                    (type(error).__name__,now+min(86400,300*2**min(row['attempts'],8)),row['trade_id']))

    def weekly(self,db,now):
        boundary=week_start(now)
        # First report follows a completed collection week; never claim a previous run.
        started=float(db.execute("SELECT value FROM meta WHERE key='started'").fetchone()[0])
        if boundary<=started:return
        if not db.execute('SELECT 1 FROM weekly WHERE week=?',(boundary,)).fetchone():
            events=[json.loads(r[0]) for r in db.execute('SELECT payload FROM events')]
            results=[]
            for idea in db.execute('SELECT * FROM ideas ORDER BY id'):
                sources=set()
                for r in db.execute('SELECT event_keys FROM idea_sources WHERE idea_id=?',(idea['id'],)):sources.update(json.loads(r[0]))
                rule=json.loads(idea['rule']);results.append(dict(id=idea['id'],rule=rule,metrics=evaluate(rule,events,sources,now)))
            message=weekly_message(db,results,boundary)
            with db:db.execute('INSERT INTO weekly(week,created,payload,message) VALUES(?,?,?,?)',(boundary,now,json.dumps(results),message))
        if self.send:
            for r in db.execute('SELECT week,message FROM weekly WHERE sent_at IS NULL ORDER BY week').fetchall():
                self.send(r['message'])
                with db:db.execute('UPDATE weekly SET sent_at=? WHERE week=?',(now,r['week']))

    def run(self):
        db=None
        try:
            db=idea_store.connect(self.path+idea_store.SUFFIX)
            with db:
                db.execute("INSERT OR IGNORE INTO meta VALUES('started',?)",(str(time.time()),))
                db.execute("INSERT OR REPLACE INTO meta VALUES('protocol',?)",(json.dumps(PROTOCOL),))
            while not self.stop.is_set():
                now=time.time()
                try:
                    collect(self.path,db,now)
                    self.review_one(db,now)
                    self.weekly(db,now)
                    with db:db.execute("INSERT OR REPLACE INTO meta VALUES('last_tick',?)",(str(now),))
                except Exception as error:
                    with db:db.execute("INSERT OR REPLACE INTO meta VALUES('last_error',?)",(type(error).__name__,))
                self.stop.wait(60)
        except Exception as error:
            print('Rocket ideas worker: '+type(error).__name__,flush=True)
        finally:
            if db:db.close()


def report(path):
    from pathlib import Path
    if not path or not Path(path+idea_store.SUFFIX).exists():return {'state':'not_started'}
    with readonly(path+idea_store.SUFFIX) as db:
        result=idea_store.export(db);result['health']=dict(db.execute('SELECT key,value FROM meta'))
        result['counts']=dict(db.execute('SELECT status,COUNT(*) FROM reviews GROUP BY status'))
        return result
