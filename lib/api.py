import json
import os
import time

import requests as r


class RedditApiWrapper():
    '''
    A simple wrapper for Pushshift.io's reddit api.

    Each 'public' method builds a request_string which is then
    fetched by the 'private' method _make_request

    Configuration is set by config file, this could be used to
    mock out the API in the event of testing.
    '''
    def __init__(self, subreddit):
        self.config = {
            "subreddit": subreddit
        }

        dirname = os.path.dirname(__file__)
        default_filename = os.path.join(dirname, 'config.txt')

        config_path = os.environ.get("REDDIT_API_CONFIG_FILE", default_filename)

        with open(config_path) as f:
            for line in f.readlines():
                [k, v] = line.split("=")
                self.config[k] = v.strip()

    def pull_comments(self, before, after):
        print("NOOOOOOOOOOOOOOOOOOOOOOOO")
        api_base_uri = self.config["api_base_uri"]
        api_comments_endpoint = self.config["api_comments_endpoint"]
        response_size = self.config["api_response_number_of_elements"]
        subreddit = self.config["subreddit"]

        request_string = f"{api_base_uri}{api_comments_endpoint}?before={before}&after={after}&limit={response_size}&subreddit={subreddit}"

        resp = self._make_request(request_string)

        return resp.get("data", [])

    def pull_submissions(self, before, after):
        api_base_uri = self.config["api_base_uri"]
        api_submissions_endpoint = self.config["api_submissions_endpoint"]
        response_size = self.config["api_response_number_of_elements"]
        subreddit = self.config["subreddit"]

        request_string = f"{api_base_uri}{api_submissions_endpoint}?before={before}&after={after}&limit={response_size}&subreddit={subreddit}"

        resp = self._make_request(request_string)

        return resp.get("data", [])

    def _make_request(self, request_string):
        resp = r.get(request_string)
        if resp.status_code == 200:
            return resp.json()
        elif resp.status_code == 429:
            print("Too many requests, backing off.")
            time.sleep(5)
            return self._make_request(request_string)
        else:
            resp.raise_for_status()
