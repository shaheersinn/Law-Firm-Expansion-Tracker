"""Offline stub for feedparser — returns empty feed so scrapers degrade gracefully."""
class _FeedParserDict(dict):
    entries = []
def parse(url_or_str, *a, **kw):
    return _FeedParserDict(entries=[], feed={}, status=200)
