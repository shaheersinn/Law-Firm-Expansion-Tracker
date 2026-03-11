"""
Minimal feedparser-compatible stub using stdlib xml + urllib.
Drop-in replacement when feedparser isn't pip-installable.
Handles Atom, RSS 2.0, RSS 1.0.
"""
import xml.etree.ElementTree as ET
from urllib import request as _req

_NS = {
    "atom":    "http://www.w3.org/2005/Atom",
    "dc":      "http://purl.org/dc/elements/1.1/",
    "content": "http://purl.org/rss/1.0/modules/content/",
    "media":   "http://search.yahoo.com/mrss/",
}

def _text(el, *tags):
    for tag in tags:
        node = el.find(tag, _NS)
        if node is not None and node.text:
            return node.text.strip()
        for ns in _NS.values():
            node = el.find(f"{{{ns}}}{tag.split(':')[-1]}")
            if node is not None and node.text:
                return node.text.strip()
    return ""

def _link(el):
    # RSS: <link>
    node = el.find("link")
    if node is not None:
        if node.text:
            return node.text.strip()
        # Atom style: <link href="..."/>
        href = node.get("href", "")
        if href:
            return href
    # Atom: <link rel="alternate" href="..."/>
    for link_el in el.findall("{http://www.w3.org/2005/Atom}link"):
        href = link_el.get("href", "")
        if href:
            return href
    return ""

class _Entry:
    def __init__(self, d): self._d = d
    def get(self, k, default=None): return self._d.get(k, default)
    def __getattr__(self, k): return self._d.get(k, "")
    def __contains__(self, k): return k in self._d

class _Feed:
    def __init__(self, entries=None, status=200):
        self.entries = [_Entry(e) for e in (entries or [])]
        self.status  = status
        self.bozo    = False

def parse(url_or_str, request_headers=None, **kw):
    headers = {"User-Agent": "Mozilla/5.0 (compatible; FeedFetcher/1.0)"}
    if request_headers:
        headers.update(request_headers)
    try:
        req = _req.Request(url_or_str, headers=headers)
        with _req.urlopen(req, timeout=15) as resp:
            raw = resp.read()
    except Exception:
        return _Feed([], status=0)

    try:
        root = ET.fromstring(raw)
    except ET.ParseError:
        return _Feed([], status=0)

    tag = root.tag.lower()
    entries = []

    if "rss" in tag or root.tag == "rss":
        channel = root.find("channel") or root
        for item in channel.findall("item")[:50]:
            e = {
                "title":   _text(item, "title") or "",
                "link":    _link(item) or "",
                "summary": _text(item, "description", "summary") or "",
                "published": _text(item, "pubDate", "dc:date") or "",
            }
            entries.append(e)
    elif "feed" in tag or "{http://www.w3.org/2005/Atom}feed" in root.tag:
        for item in root.findall("{http://www.w3.org/2005/Atom}entry")[:50]:
            e = {
                "title":   _text(item, "atom:title") or "",
                "link":    _link(item) or "",
                "summary": _text(item, "atom:summary", "atom:content") or "",
                "published": _text(item, "atom:published", "atom:updated") or "",
            }
            entries.append(e)
    else:
        # RDF/RSS 1.0
        for item in root.findall("{http://purl.org/rss/1.0/}item")[:50]:
            e = {
                "title":   item.findtext("{http://purl.org/rss/1.0/}title", ""),
                "link":    item.findtext("{http://purl.org/rss/1.0/}link", ""),
                "summary": item.findtext("{http://purl.org/rss/1.0/}description", ""),
                "published": "",
            }
            entries.append(e)

    return _Feed(entries, status=200)
