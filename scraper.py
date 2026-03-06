import hashlib
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from urllib.parse import parse_qs, unquote, urljoin, urlparse

import cloudscraper
import requests
from bs4 import BeautifulSoup

LOG_FILE = "scraper_log.json"
OUTPUT_FILE = "matches.json"
MAX_POSTS_PER_SOURCE = int(os.getenv("MAX_POSTS_PER_SOURCE", "12"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
STRICT_SKIP_ALREADY_SCRAPED = os.getenv("STRICT_SKIP_ALREADY_SCRAPED", "1") == "1"
RECHECK_AFTER_HOURS = float(os.getenv("RECHECK_AFTER_HOURS", "0"))
UA = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0 Safari/537.36"
    )
}
VIDEO_HINTS = (
    "ok.ru", "youtube.com", "youtu.be", "dailymotion.com", "dai.ly", "mega.nz", "stream", "filemoon", "mixdrop",
    "voe", "mp4upload", "vk.com", "vkvideo.ru", "hgcloud.to", "hglink.to", "dood.", "vimeo.com", "vortexvisionworks.com",
)
BAD_HINTS = (
    "facebook.com/plugins", "googletagmanager.com", "doubleclick.net", "googlesyndication.com", "adservice", "about:blank", "javascript:",
)
DIRECT_MEDIA_EXTS = (".m3u8", ".mp4", ".m4v", ".mov", ".webm", ".mkv", ".avi", ".ts")
DIRECT_MEDIA_HINTS = ("/hls/", "master.m3u8", "playlist.m3u8", "index.m3u8")
SOURCE_INFO = {
    1: {"name": "soccerfull.net", "url": "https://soccerfull.net"},
    2: {"name": "footreplays.com", "url": "https://www.footreplays.com"},
    3: {"name": "timesoccertv.com", "url": "https://timesoccertv.com"},
    4: {"name": "footballorgin.com", "url": "https://www.footballorgin.com"},
}
MISSING_TEXT_VALUES = {"", "null", "none", "n/a", "na", "unknown", "tbd", "-"}
BAD_LABEL_HINTS = (
    "disclaimer",
    "ad warning",
    "external server",
    "does not host",
    "watch and download",
    "kick-off at",
    "the referee",
    "game played at",
    "this is a match in",
    "available on",
    "watch the full show",
)
LABEL_PATTERNS = (
    ("first half", "First Half"),
    ("1st half", "First Half"),
    ("second half", "Second Half"),
    ("2nd half", "Second Half"),
    ("full match", "Full Match"),
    ("highlights", "Highlights"),
    ("highlight", "Highlights"),
    ("penalties", "Penalties"),
    ("extra time", "Extra Time"),
    ("post-match", "Post-match"),
    ("post match", "Post-match"),
    ("pre-match", "Pre-match"),
    ("pre match", "Pre-match"),
)
GENERIC_EMBED_HOST_BASES = {
    "vidhideplus.com": "https://vidhideplus.com/embed/",
    "vidhidehub.com": "https://vidhidehub.com/embed/",
    "dood.li": "https://dood.li/e/",
    "dhtpre.com": "https://dhtpre.com/embed/",
    "hglink.to": "https://hglink.to/e/",
    "hgcloud.to": "https://hgcloud.to/e/",
    "dhcplay.com": "https://dhcplay.com/e/",
    "cybervynx.com": "https://cybervynx.com/e/",
}


def load_json_file(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {} if "log" in path else []
    return {} if "log" in path else []


def save_json_file(path, data):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def gen_id(url):
    return hashlib.md5(url.strip().encode("utf-8")).hexdigest()


def now_utc():
    return datetime.now(timezone.utc)


def clean(x):
    return re.sub(r"\s+", " ", str(x or "")).strip()


def is_relative_date_text(text):
    t = clean(text).lower()
    return "ago" in t or "just now" in t or "yesterday" in t


def nurl(url, base=""):
    url = clean(unescape(url))
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if base and (url.startswith("/") or url.startswith("?")):
        return urljoin(base, url)
    return url


def extract_iframe_src(raw, base=""):
    text = clean(unescape(raw))
    if not text:
        return ""
    decoded = unquote(text)
    if "<iframe" not in decoded.lower():
        return ""
    m = re.search(r"""src\s*=\s*(['"])(.*?)\1""", decoded, re.I)
    if not m:
        return ""
    return nurl(m.group(2), base)


def normalize_embed_html(raw, base=""):
    text = clean(unescape(raw or ""))
    if not text:
        return ""
    decoded = unquote(text)
    src = extract_iframe_src(decoded, base)
    if not src:
        return ""
    return build_embed_html(src)


def can_inline_embed_url(url):
    u = nurl(url)
    if not u:
        return False
    p = urlparse(u)
    h = p.netloc.lower().lstrip("www.")
    path = (p.path or "").lower()
    if not h:
        return False
    if youtube_id(u):
        return True
    if any(
        hint in h
        for hint in (
            "ok.ru",
            "mega.nz",
            "mixdrop",
            "filemoon",
            "voe",
            "dood.",
            "hgcloud.to",
            "hglink.to",
            "dailymotion.com",
            "dai.ly",
            "vimeo.com",
            "p2pplay.online",
            "vortexvisionworks.com",
        )
    ):
        return True
    if "soccerfull.net" in h and path.startswith("/play/"):
        return True
    return (
        "/embed/" in path
        or "/videoembed/" in path
        or path.startswith("/e/")
        or "player.html" in path
    )


def build_embed_html(url):
    u = nurl(url)
    if not u:
        return ""
    safe = u.replace("&", "&amp;").replace('"', "&quot;").replace("'", "&#39;")
    return (
        f'<iframe src="{safe}" '
        'allow="autoplay; fullscreen; picture-in-picture; encrypted-media" '
        'allowfullscreen="true" frameborder="0" scrolling="no"></iframe>'
    )


def is_same_site_page(url, base=""):
    u = nurl(url, base)
    b = nurl(base)
    if not u or not b:
        return False
    up = urlparse(u)
    bp = urlparse(b)
    if not up.netloc or not bp.netloc:
        return False
    if up.netloc.lower().lstrip("www.") != bp.netloc.lower().lstrip("www."):
        return False
    path = (up.path or "").lower()
    if is_direct_media(u):
        return False
    if youtube_id(u):
        return False
    return not (
        "/embed/" in path
        or "/videoembed/" in path
        or path.startswith("/e/")
        or path.startswith("/play/")
        or "player.html" in path
    )


def host(url):
    try:
        return urlparse(url).netloc.lower().lstrip("www.")
    except Exception:
        return ""


def infer_source_from_url(url):
    h = host(url)
    if "soccerfull.net" in h:
        return 1
    if "footreplays.com" in h:
        return 2
    if "timesoccertv.com" in h:
        return 3
    if "footballorgin.com" in h:
        return 4
    return None


def ensure_source_fields(match):
    sid = match.get("source_id")
    if not sid:
        sid = infer_source_from_url(match.get("url", ""))
    if not sid:
        return match
    info = SOURCE_INFO.get(sid, {})
    match["source_id"] = sid
    match["source_tag"] = f"source_{sid}"
    match["source_name"] = match.get("source_name") or info.get("name", "")
    match["source_url"] = match.get("source_url") or info.get("url", "")
    meta = match.get("metadata") if isinstance(match.get("metadata"), dict) else {}
    meta.setdefault("source_id", sid)
    meta.setdefault("source_tag", f"source_{sid}")
    meta.setdefault("source_name", match.get("source_name", ""))
    meta.setdefault("source_url", match.get("source_url", ""))
    match["metadata"] = meta
    return match


def parse_source_id(value):
    try:
        sid = int(value)
    except (TypeError, ValueError):
        return None
    return sid if sid in SOURCE_INFO else None


def parse_source_id_from_tag(tag):
    text = clean(tag).lower()
    m = re.search(r"source[\s_-]*(\d+)", text)
    if not m:
        return None
    return parse_source_id(m.group(1))


def infer_source_from_links(links):
    for lk in links or []:
        if not isinstance(lk, dict):
            continue
        sid = infer_source_from_url(clean(lk.get("url")))
        if sid:
            return sid
    return None


def text_key(value):
    text = clean(value).lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def dedupe_text(values):
    out, seen = [], set()
    for v in values:
        t = clean(v)
        if not t:
            continue
        k = t.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(t)
    return out


def infer_canonical_label(text):
    raw = clean(unescape(text))
    if not raw:
        return ""
    lower = raw.lower()
    language = ""
    m = re.search(r"\[([a-z]{2,3})\]", raw, re.I)
    if m:
        language = f"[{m.group(1).upper()}] "
    for needle, canonical in LABEL_PATTERNS:
        if needle in lower:
            return f"{language}{canonical}".strip()
    return ""


def normalize_link_label(label, fallback="", fallback_index=None):
    text = clean(unescape(label or ""))
    fallback_text = clean(unescape(fallback or ""))

    if text:
        lower = text.lower()
        if any(hint in lower for hint in BAD_LABEL_HINTS):
            text = ""
        else:
            canonical = infer_canonical_label(text)
            looks_compact = (
                len(text) <= 42
                or any(token in text for token in ("|", "[", "]", "/", " - "))
            )
            looks_like_match_title = " vs " in lower or re.search(r"\b[a-z0-9]+\s+v\s+[a-z0-9]+\b", lower)
            if looks_like_match_title and canonical:
                text = canonical
            elif len(text) > 80 and canonical:
                text = canonical
            elif len(text) > 80 and not looks_compact:
                text = ""

    if not text and fallback_text and fallback_text.lower() not in {"replay", "match page"}:
        text = normalize_link_label(fallback_text)

    if not text and fallback_index is not None:
        return str(fallback_index)

    return text[:80]


def label_needs_replacement(label):
    normalized = normalize_link_label(label)
    if not normalized:
        return True
    return normalized.lower() in {"replay", "match page"}


def is_video(url):
    l = (url or "").lower()
    return any(h in l for h in VIDEO_HINTS)


def is_direct_media(url):
    l = clean(url).lower()
    if not l:
        return False
    if any(h in l for h in DIRECT_MEDIA_HINTS):
        return True
    if re.match(r"^[a-z0-9+/=_-]+\.m3u8(?:[?#].*)?$", l):
        return True
    return any(l.endswith(ext) or f"{ext}?" in l or f"{ext}#" in l for ext in DIRECT_MEDIA_EXTS)


def is_bad(url):
    l = (url or "").lower()
    return any(h in l for h in BAD_HINTS)


def youtube_id(url):
    u = nurl(url)
    if not u:
        return ""
    p = urlparse(u)
    h = p.netloc.lower()
    seg = [x for x in p.path.split("/") if x]
    if "youtu.be" in h and seg:
        return seg[0]
    if "youtube.com" in h or "youtube-nocookie.com" in h:
        q = parse_qs(p.query or "")
        if q.get("v"):
            return clean(q["v"][0])
        if "embed" in seg:
            i = seg.index("embed")
            if i + 1 < len(seg):
                return seg[i + 1]
        if seg and seg[0] in {"shorts", "live"} and len(seg) > 1:
            return seg[1]
    return ""


def prefer_embed_url(url, base=""):
    iframe_src = extract_iframe_src(url, base)
    if iframe_src:
        return prefer_embed_url(iframe_src, base)
    u = nurl(url, base)
    if not u:
        return ""
    p = urlparse(u)
    h = p.netloc.lower().lstrip("www.")
    path = p.path or ""

    if "soccerfull.net" in h:
        m = re.search(r"/hls/(\d+)\.m3u8\b", path, re.I)
        if m:
            return f"https://soccerfull.net/play/{m.group(1)}"

    if "ok.ru" in h:
        if "/videoembed/" in path:
            return u
        m = re.search(r"/video/(\d+)", path)
        if m:
            return f"https://ok.ru/videoembed/{m.group(1)}"

    if "mega.nz" in h:
        if path.startswith("/embed/"):
            return u
        m = re.match(r"^/file/([^/]+)$", path)
        if m and p.fragment:
            return f"https://mega.nz/embed/{m.group(1)}#{p.fragment}"

    if "dailymotion.com" in h or h == "dai.ly":
        if "geo.dailymotion.com" in h and "player.html" in path and "video=" in (p.query or ""):
            return u
        vid = ""
        if h == "dai.ly":
            seg = [x for x in path.split("/") if x]
            if seg:
                vid = seg[0]
        else:
            m = re.search(r"/(?:video|embed/video)/([a-zA-Z0-9]+)", path)
            if m:
                vid = m.group(1)
        if vid:
            return f"https://geo.dailymotion.com/player.html?video={vid}"

    if "vimeo.com" in h and "player.vimeo.com" not in h:
        m = re.search(r"/(\d+)", path)
        if m:
            return f"https://player.vimeo.com/video/{m.group(1)}"

    embed_base = GENERIC_EMBED_HOST_BASES.get(h)
    if embed_base:
        seg = [x for x in path.split("/") if x]
        if seg:
            last = seg[-1]
            if h in {"vidhideplus.com", "vidhidehub.com", "dhtpre.com"} and path.startswith("/embed/"):
                return u
            if h in {"dood.li", "hglink.to", "hgcloud.to", "dhcplay.com", "cybervynx.com"} and path.startswith("/e/"):
                return u
            return f"{embed_base}{last}"

    yt = youtube_id(u)
    if yt:
        return f"https://www.youtube-nocookie.com/embed/{yt}"

    return u


def is_page_style_replay_url(url):
    u = nurl(url)
    if not u:
        return False
    p = urlparse(u)
    h = p.netloc.lower().lstrip("www.")
    path = (p.path or "").lower()
    if not h or is_direct_media(u) or youtube_id(u) or can_inline_embed_url(u):
        return False
    return any(
        site in h
        for site in (
            "soccerfull.net",
            "footreplays.com",
            "timesoccertv.com",
            "footballorgin.com",
        )
    ) and path not in {"", "/"}


def iso(dt):
    return dt.isoformat() if dt else ""


def img_url(tag):
    if not tag:
        return ""
    for a in ("data-original", "data-src", "data-lazy-src", "data-img-url", "fifu-data-src", "src"):
        v = tag.get(a)
        if v and not str(v).startswith("data:image/svg"):
            return v
    return ""


def meta_img(soup):
    for k, v in (("property", "og:image"), ("name", "og:image"), ("property", "twitter:image"), ("name", "twitter:image")):
        m = soup.find("meta", attrs={k: v})
        if m and m.get("content"):
            return m["content"]
    return ""


def parse_rel(text, ref=None):
    text = clean(text).lower()
    if not text:
        return None
    ref = ref or now_utc()
    if "ago" not in text and "just now" not in text and "yesterday" not in text:
        return None

    anchor = ref
    m_at = re.search(r"\bat\s*(\d{1,2})(?::?(\d{2}))\s*(?:hrs?|h)?\b", text)
    if m_at:
        h, m = int(m_at.group(1)), int(m_at.group(2) or "0")
        if 0 <= h <= 23 and 0 <= m <= 59:
            anchor = ref.replace(hour=h, minute=m, second=0, microsecond=0)

    if "just now" in text:
        return anchor
    if "yesterday" in text:
        return anchor - timedelta(days=1)

    units = {
        "second": 1, "sec": 1, "minute": 60, "min": 60, "hour": 3600,
        "day": 86400, "week": 604800, "month": 2592000, "year": 31536000,
    }
    m = re.search(r"\b(\d+)\s*(second|sec|minute|min|hour|day|week|month|year)s?\s+ago\b", text)
    if m:
        amt, unit = int(m.group(1)), m.group(2)
    else:
        m = re.search(r"\b(an?|one)\s+(minute|hour|day|week|month|year)\s+ago\b", text)
        if not m:
            return None
        amt, unit = 1, m.group(2)
    return anchor - timedelta(seconds=amt * units[unit])


def parse_abs(text):
    text = clean(text)
    if not text:
        return None
    text = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", text, flags=re.I)
    text = re.sub(r"^[A-Za-z ]*:\s*", "", text)
    text = clean(text.replace("|", " "))

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        pass

    km = re.search(r"KICK-OFF at\s+(\d{1,2}:\d{2})\s*\(UTC\)\s*on\s*([0-9]{1,2}\s+[A-Za-z]+\s+\d{4})", text, re.I)
    if km:
        d = parse_abs(f"{km.group(2)} {km.group(1)}")
        return d.replace(tzinfo=timezone.utc) if d else None

    for fmt in (
        "%d/%m/%Y %H:%M", "%d/%m/%Y", "%b %d, %Y", "%B %d, %Y", "%d %B %Y %H:%M", "%d %B %Y",
        "%d %b %Y", "%Y-%m-%d %H:%M", "%Y-%m-%d", "%m/%d/%Y",
    ):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            pass

    m = re.search(r"(?:[A-Za-z]+,\s*)?([A-Za-z]+\s+\d{1,2},\s*\d{4})(?:\s+(\d{1,2}:\d{2}))?", text)
    if m:
        dp, tp = m.group(1), m.group(2) or "00:00"
        for fmt in ("%B %d, %Y %H:%M", "%b %d, %Y %H:%M"):
            try:
                return datetime.strptime(f"{dp} {tp}", fmt)
            except ValueError:
                pass
    return None


def parse_dt(text, ref=None):
    ref = ref or now_utc()
    d = parse_rel(text, ref)
    if d:
        if d.tzinfo is None:
            d = d.replace(tzinfo=ref.tzinfo or timezone.utc)
        return d, True
    d = parse_abs(text)
    if d:
        if d.tzinfo is None:
            d = d.replace(tzinfo=ref.tzinfo or timezone.utc)
        return d, False
    return None, False


def normalize_dt_value(value, ref=None):
    ref = ref or now_utc()
    if isinstance(value, datetime):
        dt = value
    else:
        text = clean(value)
        if text.lower() in MISSING_TEXT_VALUES or is_relative_date_text(text):
            return None
        dt, _ = parse_dt(text, ref)
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    dt = dt.astimezone(timezone.utc).replace(microsecond=0)
    return dt.isoformat()


def first_normalized_dt(values, ref=None):
    for v in values:
        iso_value = normalize_dt_value(v, ref=ref)
        if iso_value:
            return iso_value
    return None


def dt_unix(value):
    text = clean(value)
    if text.lower() in MISSING_TEXT_VALUES:
        return 0
    try:
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp())
    except Exception:
        pass
    iso_value = normalize_dt_value(text)
    if not iso_value:
        return 0
    try:
        return int(datetime.fromisoformat(iso_value.replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0


def jsonld_objs(soup):
    out = []
    for s in soup.select('script[type="application/ld+json"]'):
        raw = s.string or s.get_text(strip=True)
        if not raw:
            continue
        try:
            data = json.loads(raw)
        except Exception:
            continue
        if isinstance(data, dict) and isinstance(data.get("@graph"), list):
            out.extend(data["@graph"])
        elif isinstance(data, list):
            out.extend(data)
        elif isinstance(data, dict):
            out.append(data)
    return [o for o in out if isinstance(o, dict)]


def jsonld_first(objs, types):
    for o in objs:
        t = o.get("@type")
        if isinstance(t, list):
            t = t[0] if t else ""
        if t in types:
            return o
    return {}


def jsonld_img(article, objs):
    img = article.get("image")
    if isinstance(img, str):
        return img
    if isinstance(img, dict):
        if img.get("url"):
            return img["url"]
        iid = img.get("@id")
        if iid:
            for o in objs:
                if o.get("@id") == iid and o.get("url"):
                    return o["url"]
    if isinstance(img, list):
        for i in img:
            if isinstance(i, str):
                return i
            if isinstance(i, dict) and i.get("url"):
                return i["url"]
    return ""


def inline_single_video_urls(html):
    urls = set()
    for m in re.finditer(r'"single_video_url"\s*:\s*"((?:\\.|[^"])*)"', html):
        raw = m.group(1)
        try:
            val = json.loads(f'"{raw}"')
        except Exception:
            val = raw.replace('\\"', '"').replace("\\/", "/")
        val = nurl(val)
        if "<iframe" in val.lower():
            s = BeautifulSoup(val, "html.parser")
            for f in s.select("iframe[src]"):
                u = nurl(f.get("src"))
                if u:
                    urls.add(u)
        elif val:
            urls.add(val)
    return list(urls)


def node_text(node):
    if not node:
        return ""
    return clean(node.get_text(" ", strip=True))


def node_time_value(node, ref=None):
    ref = ref or now_utc()
    raw_text = node_text(node)
    attr_candidates = []
    if node:
        for attr in ("datetime", "dateTime", "content", "title", "data-datetime", "data-time"):
            value = clean(node.get(attr))
            if value:
                attr_candidates.append(value)
    exact_iso = first_normalized_dt(attr_candidates, ref=ref)
    if exact_iso:
        return {
            "display": raw_text,
            "raw": clean(attr_candidates[0]),
            "iso": exact_iso,
            "estimated": False,
        }
    parsed_dt, estimated = parse_dt(raw_text, ref)
    return {
        "display": raw_text,
        "raw": raw_text,
        "iso": iso(parsed_dt),
        "estimated": estimated if parsed_dt else False,
    }


FOOTBALLORGIN_PLAYER_IFRAME_SELECTORS = (
    ".player-api iframe[src]",
    ".single-player-video-wrapper iframe[src]",
    ".video-player-content iframe[src]",
    ".video-player-wrap iframe[src]",
    ".plyr__video-embed iframe[src]",
)
FOOTBALLORGIN_PLAYER_LINK_SELECTORS = (
    ".player-api a[href]",
)


def footballorgin_series_variants(soup, page_url):
    variants = []
    seen = set()
    for i, anchor in enumerate(soup.select(".series-listing a[href]"), start=1):
        href = nurl(anchor.get("href"), page_url)
        if not href:
            continue
        key = href.lower()
        if key in seen:
            continue
        seen.add(key)
        raw_label = anchor.get("title") or anchor.get_text(" ", strip=True)
        label = infer_canonical_label(raw_label) or normalize_link_label(raw_label, fallback_index=i)
        variants.append({"url": href, "label": label or str(i)})
    return variants


def footballorgin_player_links(html, page_url, fallback_label=""):
    out = []
    primary_label = infer_canonical_label(fallback_label) or normalize_link_label(fallback_label)
    primary_containers = []
    soup = BeautifulSoup(html, "html.parser")
    for selector in (".single-player-video-wrapper", ".video-player-wrap", ".video-player-content"):
        primary_containers.extend(soup.select(selector))

    for raw_url in inline_single_video_urls(html):
        lk = mk_link(primary_label or "Replay", raw_url, page_url)
        if not lk:
            continue
        lk["label"] = primary_label
        out.append(lk)

    for container in primary_containers or [soup]:
        for selector in FOOTBALLORGIN_PLAYER_IFRAME_SELECTORS:
            for frame in container.select(selector):
                raw_url = nurl(frame.get("src"), page_url)
                if not raw_url or is_bad(raw_url) or not is_video(raw_url):
                    continue
                lk = mk_link(primary_label or "Replay", raw_url, page_url)
                if not lk:
                    continue
                if label_needs_replacement(lk.get("label")) or not primary_label:
                    lk["label"] = primary_label
                out.append(lk)
        for selector in FOOTBALLORGIN_PLAYER_LINK_SELECTORS:
            for anchor in container.select(selector):
                raw_url = nurl(anchor.get("href"), page_url)
                if not raw_url or is_bad(raw_url) or is_same_site_page(raw_url, page_url) or not is_video(raw_url):
                    continue
                lk = mk_link(primary_label or "Replay", raw_url, page_url)
                if not lk:
                    continue
                if label_needs_replacement(lk.get("label")) or not primary_label:
                    lk["label"] = primary_label
                out.append(lk)

    return uniq_links(out)


def soccerfull_sid_variants(soup, page_url):
    info = soup.select_one("article.infobv") or soup
    variants = []
    seen = set()
    for i, anchor in enumerate(info.select("a.video-server[href]"), start=1):
        href = nurl(anchor.get("href"), page_url)
        if not href:
            continue
        key = href.lower()
        if key in seen:
            continue
        seen.add(key)
        raw_label = anchor.get("title") or anchor.get_text(" ", strip=True)
        label = infer_canonical_label(raw_label) or normalize_link_label(raw_label, fallback_index=i)
        variants.append({"url": href, "label": label or str(i)})
    return variants


def soccerfull_play_targets(raw_url, base_url):
    play = nurl(raw_url, base_url)
    if not play:
        return "", ""
    m = re.search(r"/play/(\d+)", play)
    stream = f"https://soccerfull.net/hls/{m.group(1)}.m3u8" if m else ""
    return play, stream


def footreplays_row_label(row, fallback_index=None):
    row = row if getattr(row, "name", None) == "tr" else None
    part = ""
    if row:
        cells = row.select("td")
        if cells:
            part = clean(cells[0].get_text(" ", strip=True))
        if not part:
            anchor = row.select_one("a.play-button[aria-label]")
            if anchor:
                part = re.sub(r"^\s*Watch\s+", "", clean(anchor.get("aria-label")), flags=re.I)
    label = infer_canonical_label(part) or normalize_link_label(part)
    if label:
        return label
    if row:
        table = row.find_parent("table")
        heading_node = table.select_one("thead tr th[colspan]") if table else None
        heading = clean(heading_node.get_text(" ", strip=True)) if heading_node else ""
        label = infer_canonical_label(heading) or normalize_link_label(heading)
        if label:
            return label
    return str(fallback_index) if fallback_index is not None else ""


def footreplays_table_links(soup, base_url):
    out = []
    fallback_index = 1
    for anchor in soup.select('table.video-table a[onclick*="loadVideo"]'):
        onclick = anchor.get("onclick", "")
        match = re.search(r"loadVideo\((['\"])(.+?)\1\)", onclick)
        if not match:
            continue
        raw_url = match.group(2)
        row = anchor.find_parent("tr")
        label = footreplays_row_label(row, fallback_index=fallback_index)
        link = mk_link(label, raw_url, base_url)
        if link:
            out.append(link)
            fallback_index += 1
    return uniq_links(out)


TIMESOCCERTV_LABEL_SELECTORS = ("h1", "h2", "h3", "h4", "h5", "h6", "span", "strong")
TIMESOCCERTV_LABEL_QUERY = ", ".join(TIMESOCCERTV_LABEL_SELECTORS)


def timesoccertv_label_nodes(node):
    sibling = node.previous_sibling
    yielded = 0
    while sibling is not None and yielded < 12:
        if getattr(sibling, "name", None):
            if sibling.name == "iframe" and clean(sibling.get("src")):
                return
            candidates = []
            if sibling.name in TIMESOCCERTV_LABEL_SELECTORS:
                candidates.append(sibling)
            candidates.extend(reversed(sibling.select(TIMESOCCERTV_LABEL_QUERY)))
            for candidate in candidates:
                yield candidate
                yielded += 1
                if yielded >= 12:
                    return
        sibling = sibling.previous_sibling


def timesoccertv_variant_label(node, page_title=""):
    page_title = clean(page_title)
    page_lower = page_title.lower()
    for candidate_node in timesoccertv_label_nodes(node):
        raw = clean(candidate_node.get_text(" ", strip=True))
        if not raw:
            continue
        canonical = infer_canonical_label(raw)
        if canonical:
            return canonical
        raw_lower = raw.lower()
        if re.fullmatch(r"(?:part|server|link)\s*\d+", raw_lower):
            label = normalize_link_label(raw)
            if label:
                return label
        if not page_lower:
            continue
        if raw_lower == page_lower or raw_lower in page_lower or page_lower in raw_lower:
            continue
    return ""


def timesoccertv_links(soup, base_url, page_title=""):
    out = []
    container = soup.select_one(".td-post-content, .entry-content, article") or soup
    fallback_index = 1

    for iframe in container.select("iframe[src]"):
        url = nurl(iframe.get("src"), base_url)
        if not url or is_bad(url) or not is_video(url):
            continue
        label = timesoccertv_variant_label(iframe, page_title=page_title) or str(fallback_index)
        link = mk_link(label, url, base_url)
        if link:
            out.append(link)
            fallback_index += 1

    for anchor in container.select("a[href]"):
        url = nurl(anchor.get("href"), base_url)
        if is_same_site_page(url, base_url):
            continue
        if not url or is_bad(url) or not is_video(url):
            continue
        raw_label = clean(anchor.get_text(" ", strip=True))
        label = normalize_link_label(raw_label)
        if label_needs_replacement(label):
            label = timesoccertv_variant_label(anchor, page_title=page_title)
        label = label or str(fallback_index)
        link = mk_link(label, url, base_url)
        if link:
            out.append(link)
            fallback_index += 1

    return uniq_links(out)


def mk_link(label, url, base="", kind="replay", reject_direct=True):
    raw_input = clean(unescape(url))
    if not raw_input:
        return None
    raw_player = nurl(extract_iframe_src(raw_input, base) or raw_input, base)
    if not raw_player:
        return None
    embed_url = prefer_embed_url(raw_input, base)
    if not embed_url or is_bad(embed_url):
        return None
    chosen_url = embed_url if not is_direct_media(embed_url) else raw_player
    if reject_direct and is_direct_media(chosen_url):
        return None
    if is_page_style_replay_url(chosen_url):
        return None

    out = {
        "label": normalize_link_label(label) or "Replay",
        "url": chosen_url,
        "host": host(chosen_url),
        "kind": kind,
    }

    if raw_player and raw_player != chosen_url:
        out["player_url"] = raw_player
    if embed_url and not is_direct_media(embed_url):
        out["embed_url"] = embed_url

    embed_html = normalize_embed_html(raw_input, base)
    if not embed_html and can_inline_embed_url(embed_url):
        embed_html = build_embed_html(embed_url)
    if embed_html:
        out["embed_html"] = embed_html

    return out


def uniq_links(links):
    out, seen = [], set()
    for x in links:
        u = clean(x.get("url"))
        if not u:
            continue
        k = u.lower()
        if k in seen:
            continue
        seen.add(k)
        out.append(x)
    return out


def normalize_link_entry(link, page_url=""):
    if not isinstance(link, dict):
        return None
    raw_url = clean(link.get("url") or link.get("embed_url") or link.get("player_url"))
    if not raw_url:
        return None

    final_url = prefer_embed_url(raw_url, page_url)
    if not final_url:
        return None

    if is_direct_media(final_url):
        for alt_key in ("player_url", "embed_url", "embed", "src", "href"):
            alt_raw = clean(link.get(alt_key))
            if not alt_raw:
                continue
            alt_url = prefer_embed_url(alt_raw, page_url)
            if alt_url and not is_direct_media(alt_url):
                final_url = alt_url
                break

    if is_bad(final_url) or is_direct_media(final_url):
        return None
    if is_same_site_page(final_url, page_url) or is_page_style_replay_url(final_url):
        return None

    out = dict(link)
    out["label"] = normalize_link_label(out.get("label"))
    out["url"] = final_url
    out["host"] = host(clean(out.get("embed_url")) or final_url)
    out["kind"] = clean(out.get("kind")) or "replay"

    for k in ("player_url", "embed_url", "stream_url"):
        if k in out:
            v = nurl(out.get(k), page_url)
            if v:
                out[k] = prefer_embed_url(v, page_url) if k == "embed_url" else v
            else:
                out.pop(k, None)
    embed_html = normalize_embed_html(out.get("embed_html"), page_url)
    if not embed_html:
        embed_url = clean(out.get("embed_url")) or final_url
        if can_inline_embed_url(embed_url):
            embed_html = build_embed_html(embed_url)
            out.setdefault("embed_url", embed_url)
    if embed_html:
        out["embed_html"] = embed_html
    else:
        out.pop("embed_html", None)
    return out


def normalize_match_links(match):
    if not isinstance(match, dict):
        return match
    metadata = match.get("metadata") if isinstance(match.get("metadata"), dict) else {}
    page_url = clean(match.get("url") or match.get("page_url") or metadata.get("url") or metadata.get("page_url"))
    links = match.get("links") if isinstance(match.get("links"), list) else []
    norm = []
    for lk in links:
        x = normalize_link_entry(lk, page_url)
        if x:
            norm.append(x)
    norm = uniq_links(norm)
    match["links"] = norm
    return match


def public_link(link):
    if not isinstance(link, dict):
        return None
    url = clean(link.get("url"))
    if not url:
        return None
    if is_page_style_replay_url(url):
        return None
    label = normalize_link_label(link.get("label"))
    out = {"url": url}
    if label:
        out["label"] = label
    for key in ("embed_url", "player_url", "stream_url", "host", "kind"):
        value = clean(link.get(key))
        if value:
            out[key] = value
    embed_html = normalize_embed_html(link.get("embed_html") or "", url)
    if not embed_html:
        embed_url = clean(out.get("embed_url")) or url
        if can_inline_embed_url(embed_url):
            embed_html = build_embed_html(embed_url)
            out.setdefault("embed_url", embed_url)
    if embed_html:
        out["embed_html"] = embed_html
    return out


def public_match(match, log_data=None):
    if not isinstance(match, dict):
        return None
    log_data = log_data if isinstance(log_data, dict) else {}

    metadata = match.get("metadata") if isinstance(match.get("metadata"), dict) else {}
    match_id = clean(match.get("match_id") or metadata.get("match_id"))
    log_row = log_data.get(match_id) if match_id and isinstance(log_data.get(match_id), dict) else {}

    title = clean(match.get("match"))
    if not title:
        title = clean(match.get("title"))
    if not title:
        return None

    categories = match.get("categories")
    if not isinstance(categories, list):
        categories = metadata.get("categories") if isinstance(metadata.get("categories"), list) else []
    categories = dedupe_text(categories)
    competition = clean(match.get("competition"))
    if not competition and categories:
        competition = ", ".join(categories)
    if not competition:
        competition = "Football"

    published_at = first_normalized_dt(
        [
            match.get("published_at"),
            match.get("published_raw"),
            metadata.get("published_at"),
            metadata.get("published_raw"),
            metadata.get("datePublished"),
            match.get("date"),
            metadata.get("date"),
            metadata.get("listing_date_at"),
            metadata.get("listing_date_raw"),
        ]
    )
    date_value = first_normalized_dt(
        [
            match.get("date"),
            metadata.get("date"),
            metadata.get("listing_date_at"),
            metadata.get("listing_date_raw"),
            metadata.get("datePublished"),
            metadata.get("published_at"),
            metadata.get("published_raw"),
        ]
    )
    if not date_value:
        date_value = published_at

    description = clean(
        match.get("description")
        or metadata.get("description_text")
        or metadata.get("description")
        or metadata.get("summary")
    )

    links_src = match.get("links") if isinstance(match.get("links"), list) else []
    links = []
    for lk in links_src:
        pl = public_link(lk)
        if pl:
            links.append(pl)
    links = uniq_links(links)
    title_label = infer_canonical_label(title)
    for i, lk in enumerate(links):
        if clean(lk.get("label")):
            continue
        if len(links) == 1 and title_label:
            lk["label"] = title_label
        else:
            lk["label"] = str(i + 1)
    if not links:
        return None

    sid = parse_source_id(match.get("source_id")) or parse_source_id(metadata.get("source_id")) or parse_source_id(log_row.get("source_id"))
    source_tag = clean(match.get("source_tag") or metadata.get("source_tag"))
    source_name = clean(match.get("source_name") or metadata.get("source_name") or log_row.get("source_name"))
    if not sid:
        sid = parse_source_id_from_tag(source_tag)
    if not sid:
        sid = infer_source_from_url(clean(match.get("url") or metadata.get("url") or log_row.get("url")))
    if not sid:
        sid = infer_source_from_url(clean(match.get("preview_image") or metadata.get("preview_image")))
    if not sid:
        sid = infer_source_from_links(match.get("links") if isinstance(match.get("links"), list) else [])
    if sid:
        info = SOURCE_INFO.get(sid, {})
        if not source_tag:
            source_tag = f"source_{sid}"
        if not source_name:
            source_name = info.get("name", "")
    if not source_tag:
        source_tag = "source_1"

    preview_image = clean(
        match.get("preview_image")
        or metadata.get("preview_image")
        or metadata.get("thumbnail")
        or metadata.get("image")
    )
    page_url = clean(match.get("url") or match.get("page_url") or metadata.get("url") or metadata.get("page_url"))
    source_url = clean(match.get("source_url") or metadata.get("source_url"))
    updated_at = first_normalized_dt(
        [match.get("updated_at"), metadata.get("updated_at")]
    )
    scraped_at = clean(match.get("scraped_at") or metadata.get("scraped_at"))
    listing_date_raw = clean(metadata.get("listing_date_raw"))
    listing_date_at = first_normalized_dt(
        [metadata.get("listing_date_at"), metadata.get("listing_date_raw")]
    )

    out = {
        "match_id": match_id or gen_id(title + "|" + links[0]["url"]),
        "match": title,
        "competition": competition,
        "date": date_value,
        "published_at": published_at,
        "source_tag": source_tag,
        "source_name": source_name or None,
        "source_url": source_url or None,
        "preview_image": preview_image or None,
        "page_url": page_url or None,
        "updated_at": updated_at or None,
        "scraped_at": scraped_at or None,
        "listing_date_raw": listing_date_raw or None,
        "listing_date_at": listing_date_at or None,
        "categories": categories,
        "links": links,
    }
    if description:
        out["description"] = description
    return out


def to_public_rows(rows, log_data=None):
    out = []
    for row in rows:
        p = public_match(row, log_data=log_data)
        if p:
            out.append(p)
    return out


def public_row_signature(row):
    title_key = text_key(row.get("match"))
    competition_key = text_key(row.get("competition"))
    date_key = ""
    if row.get("date"):
        date_key = clean(row["date"])[:10]
    elif row.get("published_at"):
        date_key = clean(row["published_at"])[:10]

    if date_key:
        return f"{title_key}|{competition_key}|{date_key}"

    first_url = ""
    links = row.get("links") if isinstance(row.get("links"), list) else []
    if links and isinstance(links[0], dict):
        first_url = clean(links[0].get("url"))
    return f"{title_key}|{competition_key}|{host(first_url)}|{first_url.lower()}"


def public_row_score(row):
    links = row.get("links") if isinstance(row.get("links"), list) else []
    labeled_links = sum(
        1 for lk in links
        if normalize_link_label((lk or {}).get("label"))
    )
    return (
        1 if clean(row.get("published_at")) else 0,
        1 if clean(row.get("date")) else 0,
        len(links),
        labeled_links,
        1 if clean(row.get("preview_image")) else 0,
        1 if clean(row.get("description")) else 0,
        1 if clean(row.get("source_tag")) else 0,
        dt_unix(row.get("published_at") or row.get("date")),
    )


def choose_better_public_row(current, candidate):
    return candidate if public_row_score(candidate) > public_row_score(current) else current


def dedupe_public_rows(rows):
    deduped = []
    by_id = {}
    by_sig = {}

    for row in rows:
        if not isinstance(row, dict):
            continue
        links = row.get("links") if isinstance(row.get("links"), list) else []
        links = uniq_links([lk for lk in links if public_link(lk)])
        if not links:
            continue
        row["links"] = links

        match_id = clean(row.get("match_id")) or gen_id(clean(row.get("match")) + "|" + links[0]["url"])
        row["match_id"] = match_id
        signature = public_row_signature(row)

        idx = None
        if match_id in by_id:
            idx = by_id[match_id]
        if signature in by_sig:
            idx = by_sig[signature] if idx is None else idx

        if idx is None:
            idx = len(deduped)
            deduped.append(row)
        else:
            deduped[idx] = choose_better_public_row(deduped[idx], row)

        best = deduped[idx]
        best_id = clean(best.get("match_id"))
        best_sig = public_row_signature(best)
        if best_id:
            by_id[best_id] = idx
        if best_sig:
            by_sig[best_sig] = idx
        by_id[match_id] = idx
        by_sig[signature] = idx

    deduped.sort(key=lambda x: dt_unix(x.get("published_at") or x.get("date")), reverse=True)
    return deduped


def ctx_label(node):
    selectors = ["h2", "h3", "h4", "strong", "button", "a", "p", "li"]
    for p in node.find_all_previous(selectors, limit=12):
        candidate = normalize_link_label(p.get_text(" ", strip=True))
        if candidate:
            return candidate
    return ""


def build_match(source_id, source_name, source_url, url, title, preview, categories, links, scraped_at,
                published_raw="", published_iso="", updated_iso="", duration="", extra=None):
    categories = dedupe_text(categories)
    meta = {
        "source_id": source_id,
        "source_tag": f"source_{source_id}",
        "source_name": source_name,
        "source_url": source_url,
        "scraped_at": scraped_at,
        "published_raw": clean(published_raw),
        "published_at": published_iso,
        "updated_at": updated_iso,
        "categories": categories,
    }
    if extra:
        meta.update(extra)
    return {
        "match_id": gen_id(url),
        "source_id": source_id,
        "source_tag": f"source_{source_id}",
        "source_name": source_name,
        "source_url": source_url,
        "url": url,
        "match": title,
        "date": clean(published_raw),
        "competition": ", ".join(categories),
        "preview_image": preview,
        "duration": duration or "",
        "links": uniq_links(links),
        "categories": categories,
        "published_raw": clean(published_raw),
        "published_at": published_iso,
        "updated_at": updated_iso,
        "scraped_at": scraped_at,
        "metadata": meta,
    }


class BaseScraper:
    def __init__(self, log_data, source_id, source_name, base_url):
        self.log_data = log_data
        self.source_id = source_id
        self.source_name = source_name
        self.base_url = base_url
        self.session = requests.Session()
        self.session.headers.update(UA)

    def get(self, url):
        try:
            r = self.session.get(url, timeout=REQUEST_TIMEOUT)
            if r.status_code == 200:
                return r
        except Exception:
            return None
        return None

    def get_soup(self, url):
        r = self.get(url)
        if not r:
            return None, ""
        return BeautifulSoup(r.text, "html.parser"), r.text

    def _parse_log_datetime(self, value):
        text = clean(value)
        if not text:
            return None
        try:
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        except Exception:
            return None

    def listing_stamp(self, title="", listing_date_raw="", preview="", cats=None):
        stable_date = "" if is_relative_date_text(listing_date_raw) else clean(listing_date_raw)
        payload = "|".join(
            [
                clean(title),
                stable_date,
                clean(preview),
                ",".join(dedupe_text(cats or [])),
            ]
        )
        return hashlib.md5(payload.encode("utf-8")).hexdigest()

    def should_scrape(self, match_id, listing_stamp="", listing_date_raw=""):
        if not STRICT_SKIP_ALREADY_SCRAPED:
            return True, "strict_skip_disabled"

        row = self.log_data.get(match_id)
        if not isinstance(row, dict):
            return True, "new_match"

        prev_stamp = clean(row.get("listing_stamp"))
        prev_date = clean(row.get("listing_date_raw"))
        cur_date = clean(listing_date_raw)

        if not prev_stamp and not prev_date:
            return True, "missing_listing_fingerprint"

        if cur_date and prev_date and not is_relative_date_text(cur_date) and cur_date != prev_date:
            return True, "listing_date_changed"
        if listing_stamp and prev_stamp and listing_stamp != prev_stamp:
            return True, "listing_stamp_changed"

        if RECHECK_AFTER_HOURS > 0:
            last_updated = self._parse_log_datetime(row.get("last_updated"))
            if not last_updated:
                return True, "stale_no_last_updated"
            age = now_utc() - last_updated.astimezone(timezone.utc)
            if age.total_seconds() >= RECHECK_AFTER_HOURS * 3600:
                return True, "stale_recheck_window"

        return False, "already_scraped"

    def update_log(self, match, listing_date_raw="", listing_stamp=""):
        self.log_data[match["match_id"]] = {
            "match_title": match["match"],
            "url": match.get("url", ""),
            "source_id": self.source_id,
            "source_name": self.source_name,
            "listing_date_raw": clean(listing_date_raw),
            "listing_stamp": clean(listing_stamp),
            "link_count": len(match.get("links", [])),
            "last_updated": now_utc().isoformat(),
        }


class SoccerFull(BaseScraper):
    def __init__(self, log_data):
        super().__init__(log_data, 1, "soccerfull.net", "https://soccerfull.net")

    def resolve_sid(self, sid_url):
        soup, _ = self.get_soup(nurl(sid_url, self.base_url))
        if not soup:
            return "", ""
        f = soup.select_one("iframe[src]")
        if not f:
            return "", ""
        return soccerfull_play_targets(f.get("src"), self.base_url)

    def run(self):
        print("--- Source 1: soccerfull.net ---")
        soup, _ = self.get_soup(self.base_url)
        if not soup:
            return []
        results, seen = [], set()
        skipped = 0
        inspected = 0
        scraped_at = now_utc().isoformat()
        for item in soup.select("li.item-movie"):
            ttag = item.select_one(".title-movie h3")
            atag = item.select_one("a[href]")
            if not ttag or not atag:
                continue
            url = nurl(atag.get("href"), self.base_url)
            if not url or url in seen:
                continue
            seen.add(url)
            inspected += 1
            if inspected > MAX_POSTS_PER_SOURCE:
                break
            title = clean(ttag.get_text(" ", strip=True))
            preview = img_url(item.select_one("img.movie-thumbnail"))
            match_id = gen_id(url)
            listing_stamp = self.listing_stamp(title=title, listing_date_raw="", preview=preview, cats=[])
            go, _reason = self.should_scrape(match_id, listing_stamp=listing_stamp, listing_date_raw="")
            if not go:
                skipped += 1
                continue

            ds, _ = self.get_soup(url)
            if not ds:
                continue
            info = ds.select_one("article.infobv")
            info_text = clean(info.get_text(" ", strip=True)) if info else ""
            cats = [a.get_text(" ", strip=True) for a in ds.select("#extras a")]

            km = re.search(
                r"KICK-OFF at\s+(\d{1,2}:\d{2})\s*\(UTC\)\s*on\s*([0-9]{1,2}[a-z]{0,2}\s+[A-Za-z]+\s+\d{4})",
                info_text,
                re.I,
            )
            pub_raw, pub_iso = "", ""
            if km:
                pub_raw = f"{km.group(2)} {km.group(1)} UTC"
                kickoff_day = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", km.group(2), flags=re.I)
                d = parse_abs(f"{kickoff_day} {km.group(1)}")
                pub_iso = iso(d.replace(tzinfo=timezone.utc) if d else None)

            links = []
            for variant in soccerfull_sid_variants(ds, url):
                play, stream = self.resolve_sid(variant.get("url"))
                target = play or stream
                if not target:
                    continue
                lk = mk_link(variant.get("label"), target, self.base_url)
                if lk:
                    if stream and stream != lk["url"]:
                        lk["stream_url"] = stream
                    links.append(lk)
                time.sleep(0.2)

            if not links:
                active_label = ""
                active_button = ds.select_one("a.video-server.bt_active, a.video-server.active, a.video-server[href]")
                if active_button:
                    raw_label = active_button.get("title") or active_button.get_text(" ", strip=True)
                    active_label = infer_canonical_label(raw_label) or normalize_link_label(raw_label)
                f = ds.select_one("article.infobv iframe[src], iframe[src]")
                if f:
                    play, stream = soccerfull_play_targets(f.get("src"), self.base_url)
                    fb = play or stream
                    lk = mk_link(active_label or "1", fb, self.base_url)
                    if lk:
                        if stream and stream != lk["url"]:
                            lk["stream_url"] = stream
                        links.append(lk)

            links = uniq_links(links)
            if not links:
                lk = mk_link("Match Page", url, self.base_url, kind="page")
                if lk:
                    links = [lk]
            if not links:
                continue
            match = build_match(1, self.source_name, self.base_url, url, title, preview, cats, links, scraped_at,
                                published_raw=pub_raw, published_iso=pub_iso, extra={"description_text": info_text})
            results.append(match)
            self.update_log(match, listing_date_raw="", listing_stamp=listing_stamp)
        print(f"[SOURCE_1] Collected: {len(results)} (skipped: {skipped})")
        return results


class FootReplays(BaseScraper):
    def __init__(self, log_data):
        super().__init__(log_data, 2, "footreplays.com", "https://www.footreplays.com")

    def links(self, soup):
        out = footreplays_table_links(soup, self.base_url)
        c = soup.select_one(".entry-content, article, .post-content") or soup
        for f in c.select("iframe[src]"):
            u = nurl(f.get("src"), self.base_url)
            if not u or is_bad(u) or not is_video(u):
                continue
            lk = mk_link(ctx_label(f) or "Replay", u, self.base_url)
            if lk:
                out.append(lk)
        for a in c.select("a[href]"):
            u = nurl(a.get("href"), self.base_url)
            if is_same_site_page(u, self.base_url):
                continue
            if not u or not is_video(u):
                continue
            lk = mk_link(clean(a.get_text(" ", strip=True)) or ctx_label(a) or "Replay", u, self.base_url)
            if lk:
                out.append(lk)
        return uniq_links(out)

    def run(self):
        print("--- Source 2: footreplays.com ---")
        soup, _ = self.get_soup(self.base_url)
        if not soup:
            return []
        results, seen = [], set()
        skipped = 0
        inspected = 0
        scraped_at = now_utc().isoformat()
        for card in soup.select("div.p-wrap"):
            a = card.select_one("h3.entry-title a[href], h2.entry-title a[href]")
            if not a:
                continue
            url = nurl(a.get("href"), self.base_url)
            if not url or url in seen:
                continue
            seen.add(url)
            inspected += 1
            if inspected > MAX_POSTS_PER_SOURCE:
                break
            title = clean(a.get_text(" ", strip=True))
            preview = img_url(card.select_one("img"))
            listing_cats = [x.get_text(" ", strip=True) for x in card.select(".p-categories a")]
            dnode = card.select_one("time.date, .meta-date")
            listing_raw = clean(dnode.get_text(" ", strip=True)) if dnode else ""
            match_id = gen_id(url)
            listing_stamp = self.listing_stamp(
                title=title,
                listing_date_raw=listing_raw,
                preview=preview,
                cats=listing_cats,
            )
            go, _reason = self.should_scrape(match_id, listing_stamp=listing_stamp, listing_date_raw=listing_raw)
            if not go:
                skipped += 1
                continue

            ds, _ = self.get_soup(url)
            if not ds:
                continue
            links = self.links(ds)
            if not links:
                continue
            objs = jsonld_objs(ds)
            art = jsonld_first(objs, {"Article", "NewsArticle", "BlogPosting"})
            sec = art.get("articleSection", [])
            if isinstance(sec, str):
                sec = [x.strip() for x in sec.split(",") if x.strip()]
            kw = art.get("keywords", [])
            if isinstance(kw, str):
                kw = [x.strip() for x in kw.split(",") if x.strip()]
            cats = dedupe_text(listing_cats + sec + kw)
            preview2 = preview or jsonld_img(art, objs) or meta_img(ds)
            pub_raw = clean(art.get("datePublished") or listing_raw)
            upd_raw = clean(art.get("dateModified"))
            pub_dt, pub_est = parse_dt(pub_raw, now_utc())
            upd_dt, _ = parse_dt(upd_raw, now_utc())
            match = build_match(2, self.source_name, self.base_url, url, title, preview2, cats, links, scraped_at,
                                published_raw=pub_raw, published_iso=iso(pub_dt), updated_iso=iso(upd_dt),
                                extra={"listing_date_raw": listing_raw, "published_is_estimated": pub_est})
            results.append(match)
            self.update_log(match, listing_date_raw=listing_raw, listing_stamp=listing_stamp)
        print(f"[SOURCE_2] Collected: {len(results)} (skipped: {skipped})")
        return results


class TimeSoccerTV(BaseScraper):
    def __init__(self, log_data):
        super().__init__(log_data, 3, "timesoccertv.com", "https://timesoccertv.com")

    def links(self, soup, page_title=""):
        return timesoccertv_links(soup, self.base_url, page_title=page_title)

    def run(self):
        print("--- Source 3: timesoccertv.com ---")
        soup, _ = self.get_soup(self.base_url)
        if not soup:
            return []
        results, seen = [], set()
        skipped = 0
        inspected = 0
        scraped_at = now_utc().isoformat()
        for card in soup.select(".td_module_wrap"):
            a = card.select_one("h3.entry-title a[href], h2.entry-title a[href], .entry-title a[href]")
            if not a:
                continue
            url = nurl(a.get("href"), self.base_url)
            if not url or url in seen:
                continue
            seen.add(url)
            inspected += 1
            if inspected > MAX_POSTS_PER_SOURCE:
                break
            title = clean(a.get_text(" ", strip=True))
            preview = img_url(card.select_one("img.entry-thumb, img"))
            dnode = card.select_one("time.entry-date, .entry-date")
            listing_raw = clean(dnode.get_text(" ", strip=True)) if dnode else ""
            listing_cats = [x.get_text(" ", strip=True) for x in card.select(".td-post-category")]
            match_id = gen_id(url)
            listing_stamp = self.listing_stamp(
                title=title,
                listing_date_raw=listing_raw,
                preview=preview,
                cats=listing_cats,
            )
            go, _reason = self.should_scrape(match_id, listing_stamp=listing_stamp, listing_date_raw=listing_raw)
            if not go:
                skipped += 1
                continue

            ds, _ = self.get_soup(url)
            if not ds:
                continue
            links = self.links(ds, page_title=title)
            if not links:
                continue

            objs = jsonld_objs(ds)
            art = jsonld_first(objs, {"Article", "NewsArticle", "BlogPosting"})
            sec = art.get("articleSection", [])
            if isinstance(sec, str):
                sec = [x.strip() for x in sec.split(",") if x.strip()]
            kw = art.get("keywords", [])
            if isinstance(kw, str):
                kw = [x.strip() for x in kw.split(",") if x.strip()]
            bc = [a.get_text(" ", strip=True) for a in ds.select(".tdb-breadcrumbs a, .entry-crumbs a") if clean(a.get_text(" ", strip=True)).lower() != "home"]
            cats = dedupe_text(listing_cats + sec + kw + bc)
            preview2 = preview or jsonld_img(art, objs) or meta_img(ds)
            hd = ds.select_one(".td-post-header time.entry-date, .td-post-header .entry-date")
            hd_raw = clean(hd.get_text(" ", strip=True)) if hd else ""
            pub_raw = clean(art.get("datePublished") or hd_raw or listing_raw)
            upd_raw = clean(art.get("dateModified"))
            pub_dt, pub_est = parse_dt(pub_raw, now_utc())
            list_dt, list_est = parse_dt(listing_raw, now_utc())
            upd_dt, _ = parse_dt(upd_raw, now_utc())
            match = build_match(3, self.source_name, self.base_url, url, title, preview2, cats, links, scraped_at,
                                published_raw=pub_raw, published_iso=iso(pub_dt or list_dt), updated_iso=iso(upd_dt),
                                extra={"listing_date_raw": listing_raw, "listing_date_at": iso(list_dt), "listing_date_is_estimated": list_est, "published_is_estimated": pub_est})
            results.append(match)
            self.update_log(match, listing_date_raw=listing_raw, listing_stamp=listing_stamp)
        print(f"[SOURCE_3] Collected: {len(results)} (skipped: {skipped})")
        return results


class FootballOrgin(BaseScraper):
    def __init__(self, log_data):
        super().__init__(log_data, 4, "footballorgin.com", "https://www.footballorgin.com")
        self.scraper = cloudscraper.create_scraper(browser={"browser": "chrome", "platform": "windows", "mobile": False})

    def get(self, url):
        for _ in range(3):
            try:
                r = self.scraper.get(url, timeout=REQUEST_TIMEOUT, headers=UA)
                if r.status_code == 200:
                    return r
            except Exception:
                pass
            time.sleep(1.2)
        return None

    def extract_links_html(self, html, page_url, fallback_label=""):
        return footballorgin_player_links(html, page_url, fallback_label=fallback_label)

    def detail(self, url):
        r = self.get(url)
        if not r:
            return [], [], "", "", "", "", ""
        html = r.text
        s = BeautifulSoup(html, "html.parser")
        objs = jsonld_objs(s)
        vobj = jsonld_first(objs, {"VideoObject"})
        cats = [a.get_text(" ", strip=True) for a in s.select(".categories-wrap a")]
        dnode = s.select_one("time.entry-date, time")
        date_meta = node_time_value(dnode, ref=now_utc())
        date_raw = clean(date_meta["raw"])
        upload_raw = clean(vobj.get("uploadDate"))
        preview = meta_img(s)
        variants = footballorgin_series_variants(s, url)[:6]

        links = []
        if variants:
            current_url = nurl(url, self.base_url)
            for variant in variants:
                variant_url = nurl(variant.get("url"), url)
                label = normalize_link_label(variant.get("label")) or ""
                if not variant_url:
                    continue
                if variant_url == current_url:
                    variant_html = html
                else:
                    sr = self.get(variant_url)
                    if not sr:
                        continue
                    variant_html = sr.text
                    time.sleep(0.3)
                links.extend(self.extract_links_html(variant_html, variant_url, fallback_label=label))
        else:
            links = self.extract_links_html(html, url)

        published_raw = clean(upload_raw or date_raw)
        return uniq_links(links), cats, date_raw, published_raw, "", preview, html, date_meta

    def run(self):
        print("--- Source 4: footballorgin.com ---")
        listings = [
            self.base_url + "/",
            f"{self.base_url}/full-match-replay/",
            f"{self.base_url}/tv-show/",
            f"{self.base_url}/news-and-interviews/",
            f"{self.base_url}/review-show/",
        ]
        cand, seen = [], set()
        for lu in listings:
            s, _ = self.get_soup(lu)
            if not s:
                continue
            for card in s.select("article.post-item"):
                a = card.select_one("h3.post-title a[href]")
                if not a:
                    continue
                url = nurl(a.get("href"), self.base_url)
                if not url or url in seen:
                    continue
                seen.add(url)
                dnode = card.select_one("time.entry-date, time")
                cand.append({
                    "title": clean(a.get_text(" ", strip=True)),
                    "url": url,
                    "preview": img_url(card.select_one("img")),
                    "listing_date_meta": node_time_value(dnode, ref=now_utc()),
                    "listing_categories": [x.get_text(" ", strip=True) for x in card.select(".categories-wrap a")],
                })
            if len(cand) >= MAX_POSTS_PER_SOURCE:
                break

        results, scraped_at = [], now_utc().isoformat()
        skipped = 0
        for item in cand[:MAX_POSTS_PER_SOURCE]:
            match_id = gen_id(item["url"])
            listing_stamp = self.listing_stamp(
                title=item["title"],
                listing_date_raw=clean(item["listing_date_meta"].get("raw")),
                preview=item["preview"],
                cats=item["listing_categories"],
            )
            go, _reason = self.should_scrape(
                match_id,
                listing_stamp=listing_stamp,
                listing_date_raw=clean(item["listing_date_meta"].get("raw")),
            )
            if not go:
                skipped += 1
                continue
            links, dcats, date_raw, pub_raw, upd_raw, dpreview, html, detail_date_meta = self.detail(item["url"])
            if not links:
                lk = mk_link("Match Page", item["url"], self.base_url, kind="page")
                if lk:
                    links = [lk]
            if not links:
                continue
            preview = item["preview"] or dpreview
            cats = dedupe_text(item["listing_categories"] + dcats)
            listing_raw = clean(item["listing_date_meta"].get("raw"))
            listing_display = clean(item["listing_date_meta"].get("display"))
            pub_raw = clean(pub_raw or listing_raw or date_raw)
            pub_dt = first_normalized_dt([pub_raw, detail_date_meta.get("iso"), item["listing_date_meta"].get("iso")])
            if pub_dt:
                pub_est = False
            else:
                pub_parsed_dt, pub_est = parse_dt(pub_raw, now_utc())
                pub_dt = iso(pub_parsed_dt)
            list_dt = clean(item["listing_date_meta"].get("iso"))
            list_est = bool(item["listing_date_meta"].get("estimated"))
            upd_dt, _ = parse_dt(upd_raw, now_utc())
            match = build_match(4, self.source_name, self.base_url, item["url"], item["title"], preview, cats, links, scraped_at,
                                published_raw=pub_raw or listing_raw, published_iso=pub_dt or list_dt, updated_iso=iso(upd_dt),
                                extra={
                                    "listing_date_raw": listing_raw,
                                    "listing_date_display": listing_display or listing_raw,
                                    "listing_date_at": list_dt,
                                    "listing_date_is_estimated": list_est,
                                    "published_display": clean(detail_date_meta.get("display")) or clean(pub_raw),
                                    "published_is_estimated": pub_est,
                                    "html_has_single_video_url": '"single_video_url"' in html,
                                })
            results.append(match)
            self.update_log(match, listing_date_raw=listing_raw, listing_stamp=listing_stamp)
        print(f"[SOURCE_4] Collected: {len(results)} (skipped: {skipped})")
        return results


def merge(existing, new):
    mp = {}
    for x in existing:
        if isinstance(x, dict) and x.get("match_id"):
            mp[x["match_id"]] = normalize_match_links(ensure_source_fields(x))
    for m in new:
        mp[m["match_id"]] = normalize_match_links(ensure_source_fields(m))
    merged = [normalize_match_links(ensure_source_fields(m)) for m in mp.values()]
    merged = [m for m in merged if m.get("match_id") and isinstance(m.get("links"), list) and m.get("links")]
    new_ids = {m["match_id"] for m in new}
    return [m for m in merged if m["match_id"] in new_ids] + [m for m in merged if m["match_id"] not in new_ids]


def main():
    print("========================================")
    print("   UNIVERSAL SPORTS SCRAPER (v2.0)      ")
    print("========================================")
    print(f"Per-source scrape limit: {MAX_POSTS_PER_SOURCE}")

    log_data = load_json_file(LOG_FILE)
    existing = load_json_file(OUTPUT_FILE)

    scrapers = [SoccerFull(log_data), FootReplays(log_data), TimeSoccerTV(log_data), FootballOrgin(log_data)]
    fresh = []
    for s in scrapers:
        try:
            fresh.extend(s.run())
        except Exception as e:
            print(f"[ERROR] {s.source_name} failed: {e}")

    fresh = [m for m in fresh if m.get("links")]
    merged = merge(existing, fresh)
    public_rows = dedupe_public_rows(to_public_rows(merged, log_data=log_data))
    save_json_file(OUTPUT_FILE, public_rows)
    save_json_file(LOG_FILE, log_data)

    by_src = {}
    for m in fresh:
        by_src[m.get("source_tag", "source_1")] = by_src.get(m.get("source_tag", "source_1"), 0) + 1

    print("\n----------------------------------------")
    print("SUCCESS!" if fresh else "No new matches found from any source.")
    if fresh:
        print("New records this run:")
        for tag, cnt in sorted(by_src.items()):
            print(f"  - {tag}: {cnt}")
    else:
        print("Existing records were normalized + deduplicated.")
    print(f"Total records in {OUTPUT_FILE}: {len(public_rows)}")
    print(f"Log updated: {LOG_FILE}")
    print("----------------------------------------")


if __name__ == "__main__":
    main()
