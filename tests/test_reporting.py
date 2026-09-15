import json
import math
import sqlite3
import unittest
from dataclasses import replace
from unittest.mock import Mock, patch

from bot.audit import AuditLog
from bot.probability import FEATURE_NAMES, ProbabilityModel
from bot.reporting import ModelJournal, rocket_totals, scalp_totals
from bot.scalp_shadow import ScalpShadow
from bot.trading import PaperTrader


def model(probability):
    n=len(FEATURE_NAMES)
    return ProbabilityModel(FEATURE_NAMES,(0,)*n,(0,)*n,(1,)*n,(0,)*n,
                            math.log(probability/(1-probability)),500,100,30,40,.2,())


class ReportingTests(unittest.TestCase):
    def test_rocket_totals_use_close_time_and_separate_open_and_scalp(self):
        trader=PaperTrader(':memory:',200,50,4,70,.5,.7,1,1.5,1,0,.2,ordinary_max_open_positions=0)
        try:
            db=trader.connection
            for opened,closed,kind,status,pnl,symbol in (
                (1,99999,'лидер','CLOSED',2,'OLD'),
                (1,50000,'лидер','CLOSED',-1,'DAY'),
                (1,100,'лидер','CLOSED',-3,'ALL'),
                (99990,None,'лидер','OPEN',0,'OPEN'),
                (99990,99999,'ранний','CLOSED',99,'SCALP'),
            ):
                db.execute('INSERT INTO paper_positions(opened_at,closed_at,signal_kind,status,realized_pnl_usdt,symbol,entry_price,highest_price,initial_quantity,remaining_quantity,position_usdt,ai_score) VALUES(?,?,?,?,?,?,100,100,1,1,100,80)',
                           (opened,closed,kind,status,pnl,symbol))
            text=rocket_totals(trader,{},100000)
            self.assertIn('2 часа: входов 1; закрыто 1, в плюс 1; PnL +2.000',text)
            self.assertIn('24 часа: входов 1; закрыто 2, в плюс 1; PnL +1.000',text)
            self.assertIn('С запуска: входов 4; закрыто 3, в плюс 1; PnL -2.000',text)
            self.assertIn('Открыто сейчас 1',text)
            self.assertIn('без цены 1',text)
            self.assertNotIn('99.000',text)
        finally:
            trader.close()

    def test_scalp_totals_are_not_limited_to_training_5000(self):
        db=sqlite3.connect(':memory:')
        try:
            shadow=ScalpShadow(db)
            state=json.dumps(dict(legs={'current:all07':dict(net=1,exited=30)}))
            db.executemany('INSERT INTO scalp_shadow(version,symbol,created,finished,status,features,state) VALUES(?,?,10,920,\'DONE\',\'{}\',?)',
                           [(shadow.VERSION,str(i),state) for i in range(5001)])
            state=json.dumps(dict(legs={'current:all07':dict(net=1,exited=99998)}))
            db.execute('INSERT INTO scalp_shadow(version,symbol,created,finished,status,features,state) VALUES(?,?,99000,99999,\'DONE\',\'{}\',?)',
                       (shadow.VERSION,'NEW',state))
            db.execute('INSERT INTO scalp_shadow(version,symbol,created,finished,status,features,state) VALUES(?,?,99000,99999,\'INCOMPLETE\',\'{}\',?)',
                       (shadow.VERSION,'BAD',state))
            text=scalp_totals(shadow,100000)
            self.assertIn('закрыто 5002, в плюс 5002; PnL +2501.000',text)
            self.assertIn('закрыто 1, в плюс 1; PnL +0.500',text)
            self.assertIn('неполных 1',text)
        finally:
            db.close()

    def test_versions_survive_restart_and_do_not_fake_retraining(self):
        db=sqlite3.connect(':memory:')
        journal=ModelJournal(db)
        key=journal.register('test',10,model(.2),.3)
        journal=ModelJournal(db)
        self.assertEqual(journal.register('test',20,model(.2),.3),key)
        self.assertEqual(db.execute('SELECT trained_at FROM probability_versions').fetchone()[0],10)
        self.assertIsNone(journal.forecast(key,{},9))
        db.close()

    def test_compares_frozen_versions_on_identical_future_cases(self):
        db=sqlite3.connect(':memory:')
        journal=ModelJournal(db)
        old=journal.register('test',10,model(.2),.3)
        new=journal.register('test',20,model(.8),.3)
        forecast=journal.forecast(new,{},21)
        self.assertEqual(forecast['previous_id'],old)
        self.assertAlmostEqual(forecast['previous_probability'],.2)
        self.assertAlmostEqual(forecast['probability'],.8)
        text=journal.report('test',[(1,forecast)],30)
        self.assertIn('новая версия 0.040, предыдущая 0.640',text)
        self.assertIn('изменение Brier -0.600',text)
        self.assertIn('устойчивое улучшение ещё не доказано',text)
        # A still newer model must not hide the delayed evaluation forever.
        journal.register('test',40,model(.9),.3)
        text=journal.report('test',[(1,forecast)],50)
        self.assertIn('Новые созревшие прогнозы этой версии: 0',text)
        self.assertIn('Последняя версия с созревшими прогнозами: '+new,text)
        self.assertIn('новая версия 0.040, предыдущая 0.640',text)
        db.close()

    def test_empty_periods_and_model_status_are_honest(self):
        audit=AuditLog(':memory:')
        try:
            text=scalp_totals(audit.scalp_shadow,100000)
            self.assertIn('2 часа:',text)
            self.assertIn('24 часа:',text)
            self.assertIn('С запуска:',text)
            self.assertEqual(text.count('Полных результатов закрытых опытов пока нет'),3)
            self.assertIn('Версий обученной модели пока нет',audit.scalp_shadow.learning_status(100000))
            self.assertIn('последние 30 дней',audit.build_learning_report(100000).telegram_text())
            self.assertIn('последние 24 ч',audit.build_confirmation_audit(100000).telegram_text())
            self.assertIn('последние 2 ч',audit.build_confirmation_audit(100000,7200).telegram_text())
        finally:
            audit.close()

    def test_summary_delivery_is_separate_and_cannot_open_trades(self):
        from bot.main import send_overall_reports
        audit=AuditLog(':memory:')
        telegram=Mock()
        try:
            send_overall_reports(100000,{},audit,None,telegram,'chat')
            self.assertEqual(telegram.send.call_count,2)
            self.assertIn('Общий итог теневого скальпинга',telegram.send.call_args_list[0].args[1])
            self.assertIn('состояние обучения',telegram.send.call_args_list[1].args[1])
            self.assertEqual(audit.scalp_shadow.active_symbols(),())
        finally:
            audit.close()

    def test_scalp_forecast_metadata_is_frozen_at_entry(self):
        audit=AuditLog(':memory:')
        try:
            shadow=audit.scalp_shadow
            shadow.cache=model(.6)
            shadow.cache_at=100
            shadow.model_id=shadow.journal.register('scalp-v1',100,shadow.cache,.3)
            shadow.candidate('TEST','скальпинг',101,{},False,.2)
            shadow.quote(102,'TEST',100,100.01)
            state=json.loads(audit.connection.execute('SELECT state FROM scalp_shadow').fetchone()[0])
            self.assertAlmostEqual(state['probability'],60)
            self.assertAlmostEqual(state['model_evaluation']['probability'],.6)
            self.assertEqual(state['model_evaluation']['predicted_at'],102)
            self.assertEqual(state['model_evaluation']['model_id'],shadow.model_id)
        finally:
            audit.close()

    def test_general_model_forecast_metadata_migration_and_capture(self):
        from bot.market import ConfirmationEvent
        audit=AuditLog(':memory:')
        try:
            with patch.object(audit,'_probability_samples',return_value=[(1,{})]), \
                    patch('bot.audit.train_probability_model',return_value=model(.7)):
                key=audit.record_confirmation_event(ConfirmationEvent(100,120,'TEST',100,101,True,'ok',signal_kind='скальпинг'))
            meta=json.loads(audit.connection.execute('SELECT shadow_model_meta FROM confirmation_events WHERE id=?',(key,)).fetchone()[0])
            self.assertEqual(meta['predicted_at'],120)
            self.assertAlmostEqual(meta['probability'],.7)
            self.assertTrue(meta['model_id'].startswith('mixed-legacy:'))
        finally:
            audit.close()
