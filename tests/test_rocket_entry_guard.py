import unittest
from bot.rocket_entry_guard import fading_buy_guard


class FadingBuyTests(unittest.TestCase):
    def probe(self, before=6500, after=431, r5=0, r10=-.046):
        return dict(fresh=True, before_context={'flow_buy_5s_usdt':before},
                    after_flow={'buy_5s_usdt':after}, changes={'5':r5,'10':r10})

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
