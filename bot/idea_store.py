"""Durable analysis queue and auditable normalized idea journal in a separate DB."""
import json
import sqlite3
from bot.idea_rules import validate

SUFFIX='.rocket_ideas.sqlite3'

def connect(path):
    db=sqlite3.connect(path,timeout=2);db.row_factory=sqlite3.Row
    db.execute('PRAGMA journal_mode=WAL')
    db.executescript('''
    CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY,value TEXT NOT NULL);
    CREATE TABLE IF NOT EXISTS reviews(trade_id INTEGER PRIMARY KEY,created REAL NOT NULL,
      payload TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,
      retry_at REAL NOT NULL DEFAULT 0,summary TEXT,error TEXT);
    CREATE TABLE IF NOT EXISTS ideas(id INTEGER PRIMARY KEY,created REAL NOT NULL,source_trade_id INTEGER NOT NULL,
      rule TEXT NOT NULL,normalized_key TEXT UNIQUE NOT NULL,repeats INTEGER NOT NULL DEFAULT 0);
    CREATE TABLE IF NOT EXISTS idea_sources(idea_id INTEGER,trade_id INTEGER,event_keys TEXT NOT NULL,
      PRIMARY KEY(idea_id,trade_id));
    CREATE TABLE IF NOT EXISTS requests(id INTEGER PRIMARY KEY,at REAL,reserved_usd REAL,status TEXT);
    CREATE TABLE IF NOT EXISTS weekly(week INTEGER PRIMARY KEY,created REAL,payload TEXT,message TEXT,sent_at REAL);
    CREATE TABLE IF NOT EXISTS events(key TEXT PRIMARY KEY,payload TEXT NOT NULL);
    ''');return db

def enqueue(db,trade_id,payload,now):
    with db:db.execute('INSERT OR IGNORE INTO reviews(trade_id,created,payload) VALUES(?,?,?)',
                       (trade_id,now,json.dumps(payload,allow_nan=False)))

def finish(db,trade_id,response,now):
    if not isinstance(response,dict) or set(response)!={'trade_id','summary','ideas'} or type(response['trade_id']) is not int or response['trade_id']!=trade_id:
        raise ValueError('wrong trade response')
    if not isinstance(response['summary'],str) or len(response['summary'])>2000 or not isinstance(response['ideas'],list) or len(response['ideas'])>3:
        raise ValueError('invalid response')
    valid=[];rejected=0
    for raw in response['ideas']:
        try:valid.append(validate(raw))
        except (ValueError,TypeError,KeyError):rejected+=1
    payload=json.loads(db.execute('SELECT payload FROM reviews WHERE trade_id=?',(trade_id,)).fetchone()[0])
    with db:
        for rule,key in valid:
            db.execute('INSERT OR IGNORE INTO ideas(created,source_trade_id,rule,normalized_key) VALUES(?,?,?,?)',
                       (now,trade_id,json.dumps(rule),key))
            ident=db.execute('SELECT id FROM ideas WHERE normalized_key=?',(key,)).fetchone()[0]
            db.execute('INSERT OR IGNORE INTO idea_sources VALUES(?,?,?)',(ident,trade_id,json.dumps(payload['identity_keys'])))
            db.execute('UPDATE ideas SET repeats=(SELECT COUNT(*) FROM idea_sources WHERE idea_id=?) WHERE id=?',(ident,ident))
        db.execute("UPDATE reviews SET status='done',summary=?,error=? WHERE trade_id=?",
                   (response['summary'],f'rejected_rules:{rejected}' if rejected else None,trade_id))

def export(db):
    return dict(reviews=[dict(r) for r in db.execute('SELECT trade_id,created,status,attempts,summary,error FROM reviews ORDER BY trade_id DESC LIMIT 100')],
        ideas=[dict(r) for r in db.execute('SELECT * FROM ideas ORDER BY id')],
        weekly=[dict(r) for r in db.execute('SELECT * FROM weekly ORDER BY week DESC LIMIT 4')],
        collection_started_at=(db.execute("SELECT value FROM meta WHERE key='started'").fetchone() or [None])[0])
