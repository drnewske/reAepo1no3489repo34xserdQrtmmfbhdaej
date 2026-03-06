import unittest
from pathlib import Path
import sys

from bs4 import BeautifulSoup

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scraper import timesoccertv_links, timesoccertv_variant_label


class TimeSoccerTVExtractionTests(unittest.TestCase):
    def test_timesoccertv_variant_label_prefers_nearest_heading(self):
        html = """
        <div class="td-post-content">
          <p>Watch Tottenham Hotspur vs Crystal Palace full match replay.</p>
          <h2 class="wp-block-heading block-title"><span>Tottenham Hotspur vs Crystal Palace Full Match</span></h2>
          <iframe src="//ok.ru/videoembed/12423151815405?nochat=1"></iframe>
          <h3 class="wp-block-heading"></h3>
          <p>Note: This video may display ads or pop-ups.</p>
          <h3 class="wp-block-heading">Highlights</h3>
          <iframe src="https://soccertims.vortexvisionworks.com/embed/u0gfxbo5ds3M7"></iframe>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        frames = soup.select("iframe[src]")
        self.assertEqual("Full Match", timesoccertv_variant_label(frames[0], page_title="Tottenham Hotspur vs Crystal Palace Full Match & Highlights"))
        self.assertEqual("Highlights", timesoccertv_variant_label(frames[1], page_title="Tottenham Hotspur vs Crystal Palace Full Match & Highlights"))

    def test_timesoccertv_links_extracts_half_and_highlight_labels(self):
        html = """
        <div class="td-post-content">
          <h2 class="wp-block-heading block-title">Newcastle United vs Manchester United Full Match</h2>
          <iframe src="//ok.ru/videoembed/12397743835885?nochat=1"></iframe>
          <h3 class="wp-block-heading">1st Half</h3>
          <iframe src="https://hgcloud.to/e/9anzw7qnydvw"></iframe>
          <p>Note: This video may display ads or pop-ups.</p>
          <h3 class="wp-block-heading">2nd Half</h3>
          <iframe src="https://hgcloud.to/e/83naj3ln2iae"></iframe>
          <h3 class="wp-block-heading">Highlights</h3>
          <iframe src="https://soccertims.vortexvisionworks.com/embed/fM6Fofo8MmFTX"></iframe>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        links = timesoccertv_links(
            soup,
            "https://timesoccertv.com",
            page_title="Newcastle United vs Manchester United Full Match and Highlights",
        )
        self.assertEqual(["Full Match", "First Half", "Second Half", "Highlights"], [link["label"] for link in links])

    def test_timesoccertv_links_falls_back_to_numeric_labels(self):
        html = """
        <div class="td-post-content">
          <iframe src="https://mega.nz/embed/DlAz1TIQ#5fldWcOKBMmIZoX1UjwdlvNmDEEexByj0RvGGXdvzYU"></iframe>
          <h2 class="wp-block-heading">BBC Match of the Day 28/02/2026</h2>
          <iframe src="https://mega.nz/embed/W1hWUSQK#F6sSg4QeuM8gQ2XjZ29-dCI-Jc3kKKniBVDwOLhS6z4"></iframe>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        links = timesoccertv_links(
            soup,
            "https://timesoccertv.com",
            page_title="BBC Match of the Day 28/02/2026",
        )
        self.assertEqual(["1", "2"], [link["label"] for link in links])

    def test_timesoccertv_links_do_not_reuse_full_match_after_previous_iframe(self):
        html = """
        <div class="td-post-content">
          <h2 class="wp-block-heading block-title">Brighton vs Arsenal Full Match</h2>
          <iframe src="//ok.ru/videoembed/12398012795629?nochat=1"></iframe>
          <h3 class="wp-block-heading"></h3>
          <iframe src="https://hgcloud.to/e/tv8oq8dv2n02"></iframe>
          <h3 class="wp-block-heading">Highlights</h3>
          <iframe src="https://soccertims.vortexvisionworks.com/embed/OfLu4oUheCMro"></iframe>
        </div>
        """
        soup = BeautifulSoup(html, "html.parser")
        links = timesoccertv_links(
            soup,
            "https://timesoccertv.com",
            page_title="Brighton vs Arsenal Full Match Replay and Highlights",
        )
        self.assertEqual(["Full Match", "2", "Highlights"], [link["label"] for link in links])


if __name__ == "__main__":
    unittest.main()
