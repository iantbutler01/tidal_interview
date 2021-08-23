import datetime
import unittest
from unittest.mock import patch, call

from .ingest import pull_items_for_timespan


class TestItemsArePulled(unittest.TestCase):
    def testAttemptsToPullComments(self):
        with patch("lib.api.RedditApiWrapper") as mock:
            instance = mock.return_value
            instance.pull_comments.return_value = []

            mock_calls = [call.pull_comments()]

            today = datetime.date.today()
            today = datetime.datetime.combine(today, datetime.datetime.min.time())
            date = today

            pull_items_for_timespan(int(date.timestamp()))

            assert mock.mock_calls == mock_calls

    def testAttemptsToPullSubmissions(self):
        with patch("lib.api.RedditApiWrapper") as mock:
            instance = mock.return_value
            instance.pull_submissions.return_value = []

            mock_calls = [call.pull_submissions()]

            today = datetime.date.today()
            today = datetime.datetime.combine(today, datetime.datetime.min.time())
            date = today

            pull_items_for_timespan(int(date.timestamp()), True)

            print(mock.mock_calls)

            assert mock.mock_calls == mock_calls


if __name__ == '__main__':
    unittest.main()
