import json
import sqlite3
import unittest
from unittest.mock import Mock, patch

import httpx
from bot.ai import AIAnalyst, AIUnavailable
from bot.audit import AuditLog
from bot.main import analyze_momentum_with_retries


class AICooldownTests(unittest.TestCase):
    def analyst(self, responses):
        ai=AIAnalyst('test-only', 'unchanged-model')
        ai.client.close()
        self.transport=Mock(side_effect=responses)
        ai.client=httpx.Client(base_url='https://api.openai.com',
            transport=httpx.MockTransport(self.transport))
        self.addCleanup(ai.close)
        return ai

    @staticmethod
    def success():
        text=json.dumps(dict(score=50,decision='WAIT',verdict='test',reason='test',risk='test'))
        return httpx.Response(200,json={'output':[{'type':'message',
            'content':[{'type':'output_text','text':text}]}]})

    def test_retry_after_shared_with_performance_no_inline_retries_then_recovers(self):
        ai=self.analyst([httpx.Response(429,headers={'Retry-After':'120'},
            json={'error':{'code':'rate_limit_exceeded','message':'DO_NOT_EXPORT'}}),self.success()])
        with patch('bot.ai.time.time',return_value=100), patch('bot.main.time.sleep') as sleep:
            result,error,attempts=analyze_momentum_with_retries(ai,'X',1,1,5)
            self.assertIsNone(result);self.assertEqual(attempts,1)
            self.assertIsInstance(error,AIUnavailable)
            self.assertNotIn('DO_NOT_EXPORT',str(error))
            self.assertEqual(ai.health()['next_retry_at'],220)
            self.assertEqual(self.transport.call_count,1);sleep.assert_not_called()
        with patch('bot.ai.time.time',return_value=101):
            with self.assertRaises(AIUnavailable) as caught:
                ai.analyze_performance({})
            self.assertFalse(caught.exception.attempted)
            result,error,attempts=analyze_momentum_with_retries(ai,'X',1,1,5)
            self.assertEqual(attempts,0)
            self.assertEqual(self.transport.call_count,1)
        with patch('bot.ai.time.time',return_value=221):
            result,error,attempts=analyze_momentum_with_retries(ai,'X',1,1,5)
            self.assertEqual(result.decision,'WAIT');self.assertIsNone(error)
            self.assertEqual(ai.health()['next_retry_at'],0)
            self.assertEqual(ai.health()['cooldown_skips'],2)

    def test_quota_error_is_diagnosed_without_repeated_requests_or_raw_text(self):
        ai=self.analyst([httpx.Response(429,json={'error':{'code':'insufficient_quota',
            'message':'SECRET_KEY_OR_ACCOUNT'}})])
        with patch('bot.ai.time.time',return_value=100):
            _,error,_=analyze_momentum_with_retries(ai,'X',1,1,5)
        self.assertEqual(error.kind,'quota')
        self.assertGreaterEqual(ai.health()['next_retry_at'],3700)
        audit=AuditLog(':memory:');self.addCleanup(audit.close)
        audit.record_ai_health(ai.health())
        self.assertEqual(audit.ai_health()['last_code'],'insufficient_quota')
        self.assertNotIn('SECRET',json.dumps(audit.ai_health()))
        self.assertIn('insufficient_quota',audit.ai_health_text(101))

    def test_backoff_grows_unknown_429_remains_unknown_and_dates_are_supported(self):
        ai=self.analyst([httpx.Response(429,json={'error':{'code':['bad']}}),
                         httpx.Response(429,text='not json')])
        with patch('bot.ai.random.uniform',return_value=0):
            with patch('bot.ai.time.time',return_value=100):
                analyze_momentum_with_retries(ai,'X',1,1,5)
                self.assertEqual(ai.health()['next_retry_at'],130)
            with patch('bot.ai.time.time',return_value=131):
                analyze_momentum_with_retries(ai,'X',1,1,5)
                self.assertEqual(ai.health()['next_retry_at'],191)
        self.assertEqual(ai.health()['last_code'],'unknown_429')
        self.assertEqual(AIAnalyst._retry_after('Thu, 01 Jan 1970 00:03:00 GMT',100),80)
        self.assertEqual(AIAnalyst._retry_after('NaN',100),0)

    def test_auth_error_is_not_retried_and_not_reported_as_success(self):
        ai=self.analyst([httpx.Response(401,json={})])
        _,error,attempts=analyze_momentum_with_retries(ai,'X',1,1,5)
        self.assertEqual(attempts,1);self.assertIsNotNone(error)
        self.assertEqual(ai.health()['successes'],0)
        self.assertEqual(ai.health()['last_kind'],'http-401')
