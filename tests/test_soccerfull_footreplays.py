import unittest
from pathlib import Path
import sys

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scraper import (
    footreplays_table_links,
    prefer_embed_url,
    soccerfull_play_targets,
    soccerfull_sid_variants,
)


class SoccerFullExtractionTests(unittest.TestCase):
    def test_soccerfull_sid_variants_extract_button_labels(self):
        html = """
        <article class="infobv">
          <a href="?sid=12726" class="video-server bt_active">Highlights</a>
          <a href="?sid=12727" class="video-server">Full Match</a>
          <a href="?sid=12728" class="video-server">Highlights</a>
          <h1>Brighton &amp; Hove Albion vs Arsenal</h1>
        </article>
        """
        soup = BeautifulSoup(html, "html.parser")
        variants = soccerfull_sid_variants(
            soup,
            "https://soccerfull.net/brighton-038-hove-albion-vs-arsenal-3134.html",
        )
        self.assertEqual(
            [
                {
                    "url": "https://soccerfull.net/brighton-038-hove-albion-vs-arsenal-3134.html?sid=12726",
                    "label": "Highlights",
                },
                {
                    "url": "https://soccerfull.net/brighton-038-hove-albion-vs-arsenal-3134.html?sid=12727",
                    "label": "Full Match",
                },
                {
                    "url": "https://soccerfull.net/brighton-038-hove-albion-vs-arsenal-3134.html?sid=12728",
                    "label": "Highlights",
                },
            ],
            variants,
        )

    def test_soccerfull_play_targets_extracts_play_and_stream_urls(self):
        play, stream = soccerfull_play_targets("/play/12726", "https://soccerfull.net")
        self.assertEqual("https://soccerfull.net/play/12726", play)
        self.assertEqual("https://soccerfull.net/hls/12726.m3u8", stream)


class FootReplaysExtractionTests(unittest.TestCase):
    def test_prefer_embed_url_converts_hgcloud_raw_watch_url(self):
        self.assertEqual("https://hgcloud.to/e/9anzw7qnydvw", prefer_embed_url("https://hgcloud.to/9anzw7qnydvw"))

    def test_footreplays_table_links_use_part_column_labels(self):
        html = """
        <div class="tv-table-container">
          <table class="video-table">
            <thead>
              <tr><th colspan="5">NOW Sports</th></tr>
            </thead>
            <tbody>
              <tr>
                <td>1st Half</td><td>English</td><td>HGlink</td>
                <td><a href="#" onclick="loadVideo('https://hgcloud.to/9anzw7qnydvw'); return false;" class="play-button" aria-label="Watch 1st Half">▶️</a></td>
              </tr>
              <tr>
                <td>2nd Half</td><td>English</td><td>HGlink</td>
                <td><a href="#" onclick="loadVideo('https://hgcloud.to/83naj3ln2iae'); return false;" class="play-button" aria-label="Watch 2nd Half">▶️</a></td>
              </tr>
            </tbody>
          </table>
          <table class="video-table">
            <thead>
              <tr><th colspan="5">Setanta Sports</th></tr>
            </thead>
            <tbody>
              <tr>
                <td>Full Match</td><td>Russian</td><td>HGlink</td>
                <td><a href="#" onclick="loadVideo('https://hgcloud.to/kwnzhgxyuxu8'); return false;" class="play-button" aria-label="Watch Full Match">▶️</a></td>
              </tr>
            </tbody>
          </table>
          <table class="video-table">
            <thead>
              <tr><th colspan="5">Highlights</th></tr>
            </thead>
            <tbody>
              <tr>
                <td>Highlight</td><td>English</td><td>HGlink</td>
                <td><a href="#" onclick="loadVideo('https://hgcloud.to/9ji5aabjmru8'); return false;" class="play-button" aria-label="Watch Highlight">▶️</a></td>
              </tr>
            </tbody>
          </table>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        links = footreplays_table_links(soup, "https://www.footreplays.com")
        self.assertEqual(
            ["First Half", "Second Half", "Full Match", "Highlights"],
            [link["label"] for link in links],
        )
        self.assertEqual(
            [
                "https://hgcloud.to/e/9anzw7qnydvw",
                "https://hgcloud.to/e/83naj3ln2iae",
                "https://hgcloud.to/e/kwnzhgxyuxu8",
                "https://hgcloud.to/e/9ji5aabjmru8",
            ],
            [link["url"] for link in links],
        )


if __name__ == "__main__":
    unittest.main()
