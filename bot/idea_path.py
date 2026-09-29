"""Bounded one-second ask samples and full sampled bid minute extrema."""
def append_quotes(state, rows, start, end):
    bars=state.setdefault('idea_minutes',[]);quotes=state.setdefault('idea_quotes',[])
    last=state.get('idea_last_at',start-1e-6)
    for at,bid,ask in rows:
        if not last<at<=end or at<start:continue
        index=int((at-start)//60)
        if not bars or bars[-1]['minute_from_entry']!=index:
            bars.append(dict(minute_from_entry=index,first_at=at,last_at=at,open=bid,high=bid,low=bid,close=bid))
        b=bars[-1];b.update(last_at=at,high=max(b['high'],bid),low=min(b['low'],bid),close=bid)
        if not quotes or int(at)>int(quotes[-1][0]):quotes.append([at,bid,ask])
        last=at
    state['idea_last_at']=last


def delayed_replay(event,wait):
    """Require timed asks. Never use a minute close as a delayed buy price."""
    from bot.idea_rules import replay
    q=event.get('quotes',[])
    if not q:return None
    target=q[0][0]+wait
    first=next((r for r in q if r[0]>=target),None)
    if first is None or first[0]-target>2:return None
    # Only complete bars wholly AFTER the fill. The containing bar has unknown
    # ordering relative to the delayed ask, so a bid anchor sample is required.
    # Preserve all extrema by requiring fill exactly at a recorded bar boundary.
    bars=event.get('minutes',[])
    index=next((i for i,b in enumerate(bars) if abs(b['first_at']-first[0])<1e-6),None)
    if index is None:return None
    return replay(bars[index:],first[2],event.get('policy',{}),event.get('cost'))
