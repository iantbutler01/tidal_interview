import datetime
import unittest
from unittest.mock import patch, call

from .ingest import pull_items_for_timespan


class TestItemsArePulled(unittest.TestCase):
    def testAttemptsToPullComments(self):
        with patch("lib.api.RedditApiWrapper") as mock:
            instance = mock.return_value
            instance.pull_comments.return_value = []

            today = datetime.date.today()
            today = datetime.datetime.combine(today, datetime.datetime.min.time())
            date = today

            timestamp = int(date.timestamp())
            timestamp_end = int((date - datetime.timedelta(hours=1)).timestamp())

            mock_calls = [call("TIdaL"), call().pull_comments(timestamp, timestamp_end)]
            pull_items_for_timespan(timestamp)

            assert mock.mock_calls == mock_calls

    def testAttemptsToPullSubmissions(self):
        with patch("lib.api.RedditApiWrapper") as mock:
            instance = mock.return_value
            instance.pull_submissions.return_value = []

            today = datetime.date.today()
            today = datetime.datetime.combine(today, datetime.datetime.min.time())
            date = today
            timestamp = int(date.timestamp())
            timestamp_end = int((date - datetime.timedelta(hours=1)).timestamp())
            pull_items_for_timespan(timestamp, True)

            mock_calls = [call("TIdaL"), call().pull_submissions(timestamp, timestamp_end)]

            print(mock.mock_calls, mock_calls)

            assert mock.mock_calls == mock_calls


if __name__ == '__main__':
    unittest.main()
