"""Read legacy global and new per-symbol recording interruptions together."""
def read_gaps(db, symbol, start, end):
    tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name IN ('rocket_path_gaps','rocket_symbol_gaps')")}
    result = []
    if 'rocket_path_gaps' in tables:
        result.extend(db.execute('SELECT started,ended FROM rocket_path_gaps WHERE ended>=? AND started<=?', (start,end)))
    if 'rocket_symbol_gaps' in tables:
        result.extend(db.execute('SELECT started,ended FROM rocket_symbol_gaps WHERE symbol=? AND ended>=? AND started<=?', (symbol,start,end)))
    return result
