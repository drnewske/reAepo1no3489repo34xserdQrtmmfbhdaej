import unittest
from datetime import datetime, timezone
from pathlib import Path
import sys

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scraper import (
    footballorgin_player_links,
    footballorgin_series_variants,
    node_time_value,
    normalize_link_label,
)


class FootballOrginExtractionTests(unittest.TestCase):
    def test_node_time_value_prefers_datetime_attribute(self):
        soup = BeautifulSoup(
            '<time class="entry-date published updated" datetime="2026-03-06T09:35:39+00:00">28 minutes ago</time>',
            "html.parser",
        )
        meta = node_time_value(soup.select_one("time"), ref=datetime(2026, 3, 6, 10, 3, 0, tzinfo=timezone.utc))
        self.assertEqual("28 minutes ago", meta["display"])
        self.assertEqual("2026-03-06T09:35:39+00:00", meta["raw"])
        self.assertEqual("2026-03-06T09:35:39+00:00", meta["iso"])
        self.assertFalse(meta["estimated"])

    def test_node_time_value_falls_back_to_relative_text(self):
        soup = BeautifulSoup("<time>30 minutes ago</time>", "html.parser")
        meta = node_time_value(soup.select_one("time"), ref=datetime(2026, 3, 6, 17, 43, 0, tzinfo=timezone.utc))
        self.assertEqual("30 minutes ago", meta["raw"])
        self.assertEqual("2026-03-06T17:13:00+00:00", meta["iso"])
        self.assertTrue(meta["estimated"])

    def test_footballorgin_series_variants_extracts_button_labels(self):
        html = """
        <div class="series-wrapper">
          <div class="series-listing">
            <a href="https://www.footballorgin.com/test-post/" class="series-item active-item" title="Full match">
              <span>Full match</span>
            </a>
            <a href="https://www.footballorgin.com/test-post/?video_index=1" class="series-item" title="Highlights">
              <span>Highlights</span>
            </a>
          </div>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        variants = footballorgin_series_variants(soup, "https://www.footballorgin.com/test-post/")
        self.assertEqual(
            [
                {"url": "https://www.footballorgin.com/test-post/", "label": "Full Match"},
                {"url": "https://www.footballorgin.com/test-post/?video_index=1", "label": "Highlights"},
            ],
            variants,
        )

    def test_footballorgin_player_links_extracts_iframe_string_and_uses_button_label(self):
        html = """
        <script>
        var payload = {"single_video_url":"<iframe width=\\"853\\" height=\\"480\\" src=\\"https://soccerfull.net/play/12720\\" frameborder=\\"0\\" allow=\\"autoplay\\" allowfullscreen><\\/iframe>"};
        </script>
        <div class="single-player-video-wrapper">
          <div class="ads-above-single-player"><p><strong>Disclaimer:</strong> bad nearby text</p></div>
        </div>
        """
        links = footballorgin_player_links(
            html,
            "https://www.footballorgin.com/sporting-cp-vs-fc-porto-full-match-4-march-2026/",
            fallback_label="Full match",
        )
        self.assertEqual(1, len(links))
        self.assertEqual("Full Match", links[0]["label"])
        self.assertEqual("https://soccerfull.net/play/12720", links[0]["url"])
        self.assertEqual("https://soccerfull.net/play/12720", links[0]["embed_url"])

    def test_footballorgin_player_links_extracts_youtube_url_without_disclaimer_label(self):
        html = """
        <div class="single-player-video-wrapper">
          <div class="ads-above-single-player"><p><strong>Disclaimer:</strong> This video is hosted on an external server.</p></div>
          <div class="video-player-content"><div class="player-api"></div></div>
        </div>
        <script>
        var payload = {"single_video_url":"https:\\/\\/www.youtube.com\\/watch?v=EErQjE8YGxs"};
        </script>
        """
        links = footballorgin_player_links(
            html,
            "https://www.footballorgin.com/every-midweek-goal-matchweek-29-2025-26-premier-league-highlights/",
        )
        self.assertEqual(1, len(links))
        self.assertEqual("", links[0]["label"])
        self.assertEqual("https://www.youtube-nocookie.com/embed/EErQjE8YGxs", links[0]["url"])
        self.assertEqual("https://www.youtube-nocookie.com/embed/EErQjE8YGxs", links[0]["embed_url"])

    def test_footballorgin_player_links_extracts_player_anchor_link(self):
        html = """
        <div class="single-player-video-wrapper">
          <div class="player-api">
            <a href="https://mixdrop.top/e/dkdvndlltvx0xe" target="_blank" rel="noopener noreferrer">
              <img src="https://example.com/poster.jpg" />
            </a>
          </div>
        </div>
        """
        links = footballorgin_player_links(
            html,
            "https://www.footballorgin.com/olympique-de-marseille-vs-toulouse-full-match-4-march-2026/",
        )
        self.assertEqual(1, len(links))
        self.assertEqual("", links[0]["label"])
        self.assertEqual("https://mixdrop.top/e/dkdvndlltvx0xe", links[0]["url"])
        self.assertEqual("https://mixdrop.top/e/dkdvndlltvx0xe", links[0]["embed_url"])

    def test_normalize_link_label_rejects_ad_warning_text(self):
        self.assertEqual("", normalize_link_label("📢 Ad Warning:"))


if __name__ == "__main__":
    unittest.main()
