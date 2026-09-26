import unittest
from bot.rocket_entry_guard import fading_buy_guard


class FadingBuyTests(unittest.TestCase):
    def probe(self, before=6500, after=431, r5=0, r10=-.046):
        return dict(fresh=True,allowed=True, before_context={'flow_buy_5s_usdt':before,'spread_bps':10},
                    after_flow={'buy_5s_usdt':after,'sell_5s_usdt':0,'spread_bps':10},
                    changes={'5':r5,'10':r10,'15':.1,'60':.1})

    def test_buys_fall_and_stall_or_decline_defers(self):
        for r5,r10 in [(0,.1),(.1,-.01),(-.1,.1)]:
            self.assertFalse(fading_buy_guard(self.probe(r5=r5,r10=r10))[0])

    def test_buy_fall_alone_does_not_veto_and_recovery_can_pass(self):
        self.assertTrue(fading_buy_guard(self.probe(r5=.1,r10=.2))[0])
        self.assertTrue(fading_buy_guard(self.probe(after=6500,r5=.1))[0])
        self.assertTrue(fading_buy_guard(self.probe(after=7000,r5=.1))[0])

    def test_unknown_data_defer(self):
        for p in [None,{},dict(fresh=False), self.probe(after=float('nan')),
                  self.probe(before=None),self.probe(r10=None),self.probe(after=-1)]:
            self.assertFalse(fading_buy_guard(p)[0])

    def test_fresh_quality_veto_is_mandatory_for_direct_entry(self):
        for verdict in (False,None):
            probe=self.probe(r5=.1,r10=.1)
            probe.update(allowed=verdict,reason='частота исполненных сделок не ускоряется')
            allowed,reason=fading_buy_guard(probe)
            self.assertFalse(allowed)
            self.assertIn('частота',reason)

    def test_qi_growth_cannot_mask_sell_dominance_or_widened_spread(self):
        probe=self.probe(after=2074.757413,r5=.5444,r10=.2833)
        probe['before_context']['spread_bps']=2.364066
        probe['after_flow'].update(sell_5s_usdt=2974.072571,spread_bps=16.50554)
        allowed,reason=fading_buy_guard(probe)
        self.assertFalse(allowed);self.assertIn('покупки за 5с',reason)
        probe['after_flow']['sell_5s_usdt']=100
        allowed,reason=fading_buy_guard(probe)
        self.assertFalse(allowed);self.assertIn('спред расширился',reason)
        probe['after_flow']['spread_bps']=2
        self.assertTrue(fading_buy_guard(probe)[0])

    def test_live_policy_requires_all_c_windows_and_does_not_trust_saved_verdict(self):
        from bot.rocket_entry_variants import EXECUTION_POLICY
        for window in ('15','60'):
            for value in (None,0,-.1,float('nan')):
                probe=self.probe(r5=.1,r10=.1)
                probe['changes'][window]=value
                probe['entry_variants']={'decisions':{'C':True}}
                self.assertFalse(fading_buy_guard(probe)[0])
        probe=self.probe(r5=.1,r10=.1)
        self.assertTrue(fading_buy_guard(probe)[0])
        self.assertEqual(probe['entry_policy'],EXECUTION_POLICY)
        # Volume remains a separate existing gate; C doesn't add a new threshold.
        probe['before_context']['volume_ratio_5m']=.8
        self.assertTrue(fading_buy_guard(probe)[0])
