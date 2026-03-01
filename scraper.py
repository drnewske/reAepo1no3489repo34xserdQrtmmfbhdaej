import hashlib
import json
import os
import re
import time
from datetime import datetime, timedelta, timezone
from html import unescape
from urllib.parse import urljoin, urlparse

import cloudscraper
import requests
from bs4 import BeautifulSoup

LOG_FILE = "scraper_log.json"
OUTPUT_FILE = "matches.json"
MAX_POSTS_PER_SOURCE = int(os.getenv("MAX_POSTS_PER_SOURCE", "12"))
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "30"))
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
SOURCE_INFO = {
    1: {"name": "soccerfull.net", "url": "https://soccerfull.net"},
    2: {"name": "footreplays.com", "url": "https://www.footreplays.com"},
    3: {"name": "timesoccertv.com", "url": "https://timesoccertv.com"},
    4: {"name": "footballorgin.com", "url": "https://www.footballorgin.com"},
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


def nurl(url, base=""):
    url = clean(unescape(url))
    if not url:
        return ""
    if url.startswith("//"):
        return "https:" + url
    if base and (url.startswith("/") or url.startswith("?")):
        return urljoin(base, url)
    return url


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


def is_video(url):
    l = (url or "").lower()
    return any(h in l for h in VIDEO_HINTS)


def is_bad(url):
    l = (url or "").lower()
    return any(h in l for h in BAD_HINTS)


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
    m = re.search(r'"single_video_url"\s*:\s*"((?:\\.|[^"])*)"', html)
    if not m:
        return []
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


def mk_link(label, url, base="", kind="replay"):
    u = nurl(url, base)
    if not u:
        return None
    return {"label": clean(label) or "Replay", "url": u, "host": host(u), "kind": kind}


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


def ctx_label(node):
    for p in node.find_all_previous(["h2", "h3", "h4", "strong", "p", "li"], limit=8):
        t = clean(p.get_text(" ", strip=True))
        if not t:
            continue
        l = t.lower()
        if any(k in l for k in ("full match", "highlights", "1st", "2nd", "half", "server", "link")):
            return t[:80]
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

    def update_log(self, match):
        self.log_data[match["match_id"]] = {
            "match_title": match["match"],
            "source_id": self.source_id,
            "source_name": self.source_name,
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
        play = nurl(f.get("src"), self.base_url)
        if not play:
            return "", ""
        m = re.search(r"/play/(\d+)", play)
        stream = f"{self.base_url}/hls/{m.group(1)}.m3u8" if m else play
        return stream, play

    def run(self):
        print("--- Source 1: soccerfull.net ---")
        soup, _ = self.get_soup(self.base_url)
        if not soup:
            return []
        results, seen = [], set()
        scraped_at = now_utc().isoformat()
        for item in soup.select("li.item-movie"):
            if len(results) >= MAX_POSTS_PER_SOURCE:
                break
            ttag = item.select_one(".title-movie h3")
            atag = item.select_one("a[href]")
            if not ttag or not atag:
                continue
            url = nurl(atag.get("href"), self.base_url)
            if not url or url in seen:
                continue
            seen.add(url)
            title = clean(ttag.get_text(" ", strip=True))
            preview = img_url(item.select_one("img.movie-thumbnail"))

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
                d = parse_abs(f"{re.sub(r'(\d+)(st|nd|rd|th)', r'\1', km.group(2), flags=re.I)} {km.group(1)}")
                pub_iso = iso(d.replace(tzinfo=timezone.utc) if d else None)

            links = []
            for a in ds.select("a.video-server[href]"):
                lbl = clean(a.get_text(" ", strip=True)) or "Replay"
                stream, play = self.resolve_sid(nurl(a.get("href"), url))
                if not stream:
                    continue
                lk = mk_link(lbl, stream, self.base_url)
                if lk:
                    if play and play != stream:
                        lk["player_url"] = play
                    links.append(lk)
                time.sleep(0.2)

            if not links:
                f = ds.select_one("iframe[src]")
                if f:
                    stream, play = self.resolve_sid(nurl(f.get("src"), self.base_url))
                    fb = stream or play
                    lk = mk_link("Replay", fb, self.base_url)
                    if lk:
                        if play and play != fb:
                            lk["player_url"] = play
                        links.append(lk)

            links = uniq_links(links)
            if not links:
                continue
            match = build_match(1, self.source_name, self.base_url, url, title, preview, cats, links, scraped_at,
                                published_raw=pub_raw, published_iso=pub_iso, extra={"description_text": info_text})
            results.append(match)
            self.update_log(match)
        print(f"[SOURCE_1] Collected: {len(results)}")
        return results


class FootReplays(BaseScraper):
    def __init__(self, log_data):
        super().__init__(log_data, 2, "footreplays.com", "https://www.footreplays.com")

    def links(self, soup):
        out = []
        for a in soup.select('a[onclick*="loadVideo"]'):
            oc = a.get("onclick", "")
            m = re.search(r"loadVideo\((['\"])(.+?)\1\)", oc)
            if not m:
                continue
            raw = m.group(2)
            row = a.find_parent("tr")
            parts = []
            if row:
                for c in [clean(td.get_text(" ", strip=True)) for td in row.find_all("td")][:3]:
                    if c and c != "▶️":
                        parts.append(c)
            lbl = " | ".join(parts) if parts else "Replay"
            lk = mk_link(lbl, raw, self.base_url)
            if lk:
                out.append(lk)
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
        scraped_at = now_utc().isoformat()
        for card in soup.select("div.p-wrap"):
            if len(results) >= MAX_POSTS_PER_SOURCE:
                break
            a = card.select_one("h3.entry-title a[href], h2.entry-title a[href]")
            if not a:
                continue
            url = nurl(a.get("href"), self.base_url)
            if not url or url in seen:
                continue
            seen.add(url)
            title = clean(a.get_text(" ", strip=True))
            preview = img_url(card.select_one("img"))
            listing_cats = [x.get_text(" ", strip=True) for x in card.select(".p-categories a")]
            dnode = card.select_one("time.date, .meta-date")
            listing_raw = clean(dnode.get_text(" ", strip=True)) if dnode else ""

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
            self.update_log(match)
        print(f"[SOURCE_2] Collected: {len(results)}")
        return results


class TimeSoccerTV(BaseScraper):
    def __init__(self, log_data):
        super().__init__(log_data, 3, "timesoccertv.com", "https://timesoccertv.com")

    def links(self, soup):
        out = []
        c = soup.select_one(".td-post-content, .entry-content, article") or soup
        idx = 1
        for f in c.select("iframe[src]"):
            u = nurl(f.get("src"), self.base_url)
            if not u or is_bad(u) or not is_video(u):
                continue
            lk = mk_link(ctx_label(f) or f"Replay {idx}", u, self.base_url)
            if lk:
                out.append(lk)
                idx += 1
        for a in c.select("a[href]"):
            u = nurl(a.get("href"), self.base_url)
            if not u or is_bad(u) or not is_video(u):
                continue
            lk = mk_link(clean(a.get_text(" ", strip=True)) or ctx_label(a) or "Replay", u, self.base_url)
            if lk:
                out.append(lk)
        return uniq_links(out)

    def run(self):
        print("--- Source 3: timesoccertv.com ---")
        soup, _ = self.get_soup(self.base_url)
        if not soup:
            return []
        results, seen = [], set()
        scraped_at = now_utc().isoformat()
        for card in soup.select(".td_module_wrap"):
            if len(results) >= MAX_POSTS_PER_SOURCE:
                break
            a = card.select_one("h3.entry-title a[href], h2.entry-title a[href], .entry-title a[href]")
            if not a:
                continue
            url = nurl(a.get("href"), self.base_url)
            if not url or url in seen:
                continue
            seen.add(url)
            title = clean(a.get_text(" ", strip=True))
            preview = img_url(card.select_one("img.entry-thumb, img"))
            dnode = card.select_one("time.entry-date, .entry-date")
            listing_raw = clean(dnode.get_text(" ", strip=True)) if dnode else ""
            listing_cats = [x.get_text(" ", strip=True) for x in card.select(".td-post-category")]

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
            self.update_log(match)
        print(f"[SOURCE_3] Collected: {len(results)}")
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

    def extract_links_html(self, html, page_url):
        out = []
        for u in inline_single_video_urls(html):
            lk = mk_link("Replay", u, page_url)
            if lk and not is_bad(lk["url"]):
                out.append(lk)
        s = BeautifulSoup(html, "html.parser")
        c = s.select_one(".entry-content, article, .single-content-inner") or s
        for f in c.select("iframe[src]"):
            u = nurl(f.get("src"), page_url)
            if not u or is_bad(u) or not is_video(u):
                continue
            lk = mk_link(ctx_label(f) or "Replay", u, page_url)
            if lk:
                out.append(lk)
        for a in c.select("a[href]"):
            u = nurl(a.get("href"), page_url)
            if not u or is_bad(u) or not is_video(u):
                continue
            lk = mk_link(clean(a.get_text(" ", strip=True)) or ctx_label(a) or "Replay", u, page_url)
            if lk:
                out.append(lk)
        return uniq_links(out)

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
        date_raw = clean(dnode.get_text(" ", strip=True)) if dnode else ""
        upload_raw = clean(vobj.get("uploadDate"))
        preview = meta_img(s)

        links = self.extract_links_html(html, url)
        for a in s.select(".series-listing a[href]")[:6]:
            lbl = clean(a.get_text(" ", strip=True)) or "Replay"
            su = nurl(a.get("href"), url)
            if not su:
                continue
            sr = self.get(su)
            if not sr:
                continue
            for lk in self.extract_links_html(sr.text, su):
                if not lk.get("label") or lk["label"] == "Replay":
                    lk["label"] = lbl
                links.append(lk)
            time.sleep(0.3)

        return uniq_links(links), cats, date_raw, (upload_raw or date_raw), "", preview, html

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
                    "listing_date": clean(dnode.get_text(" ", strip=True)) if dnode else "",
                    "listing_categories": [x.get_text(" ", strip=True) for x in card.select(".categories-wrap a")],
                })
            if len(cand) >= MAX_POSTS_PER_SOURCE:
                break

        results, scraped_at = [], now_utc().isoformat()
        for item in cand[:MAX_POSTS_PER_SOURCE]:
            links, dcats, date_raw, pub_raw, upd_raw, dpreview, html = self.detail(item["url"])
            if not links:
                continue
            preview = item["preview"] or dpreview
            cats = dedupe_text(item["listing_categories"] + dcats)
            pub_raw = clean(pub_raw or item["listing_date"] or date_raw)
            pub_dt, pub_est = parse_dt(pub_raw, now_utc())
            list_dt, list_est = parse_dt(item["listing_date"], now_utc())
            upd_dt, _ = parse_dt(upd_raw, now_utc())
            match = build_match(4, self.source_name, self.base_url, item["url"], item["title"], preview, cats, links, scraped_at,
                                published_raw=pub_raw or item["listing_date"], published_iso=iso(pub_dt or list_dt), updated_iso=iso(upd_dt),
                                extra={"listing_date_raw": item["listing_date"], "listing_date_at": iso(list_dt), "listing_date_is_estimated": list_est, "published_is_estimated": pub_est, "html_has_single_video_url": '"single_video_url"' in html})
            results.append(match)
            self.update_log(match)
        print(f"[SOURCE_4] Collected: {len(results)}")
        return results


def merge(existing, new):
    mp = {}
    for x in existing:
        if isinstance(x, dict) and x.get("match_id"):
            mp[x["match_id"]] = ensure_source_fields(x)
    for m in new:
        mp[m["match_id"]] = ensure_source_fields(m)
    merged = [ensure_source_fields(m) for m in mp.values()]
    merged = [m for m in merged if m.get("source_id") in SOURCE_INFO and m.get("url")]
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
    if not fresh:
        print("\nNo new matches found from any source.")
        return

    merged = merge(existing, fresh)
    save_json_file(OUTPUT_FILE, merged)
    save_json_file(LOG_FILE, log_data)

    by_src = {}
    for m in fresh:
        by_src[m["source_tag"]] = by_src.get(m["source_tag"], 0) + 1

    print("\n----------------------------------------")
    print("SUCCESS!")
    print("New records this run:")
    for tag, cnt in sorted(by_src.items()):
        print(f"  - {tag}: {cnt}")
    print(f"Total records in {OUTPUT_FILE}: {len(merged)}")
    print(f"Log updated: {LOG_FILE}")
    print("----------------------------------------")


if __name__ == "__main__":
    main()
