import os
import json
from urllib.parse import urlencode
import logging

import requests

from dailyprophet.feeds.feed import Feed
from dailyprophet.configs import (
    LINKEDIN_CLIENT_ID,
    LINKEDIN_CLIENT_SECRET,
    LINKEDIN_REDIRECT_URI,
)

logger = logging.getLogger(__name__)


class LinkedinJobFeed(Feed):
    def __init__(self):
        super().__init__()
        self.client_id = LINKEDIN_CLIENT_ID
        self.client_secret = LINKEDIN_CLIENT_SECRET
        self.redirect_uri = LINKEDIN_REDIRECT_URI
        self.authorization_url = "https://www.linkedin.com/oauth/v2/authorization"
        self.token_url = "https://www.linkedin.com/oauth/v2/accessToken"
        self.scope = "r_liteprofile"
        self.state = "your_state_value"  # Replace with a unique state value
        self.access_token = None
        self.refresh_token = None
        self.token_file_path = "linkedin_tokens.json"

    def save_tokens_to_file(self):
        """Save access token and refresh token to a JSON file."""
        tokens = {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
        }
        with open(self.token_file_path, "w") as token_file:
            json.dump(tokens, token_file)

    def load_tokens_from_file(self):
        """Load access token and refresh token from a JSON file."""
        if os.path.exists(self.token_file_path):
            with open(self.token_file_path, "r") as token_file:
                tokens = json.load(token_file)
                self.access_token = tokens.get("access_token")
                self.refresh_token = tokens.get("refresh_token")

    def get_access_token(self, authorization_code):
        """
        Exchanges the authorization code for an access token.
        Returns the access token and refresh token.
        """
        data = {
            "grant_type": "authorization_code",
            "code": authorization_code,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "redirect_uri": self.redirect_uri,
        }

        response = requests.post(self.token_url, data=data)
        response.raise_for_status()

        tokens = response.json()
        self.access_token = tokens.get("access_token")
        self.refresh_token = tokens.get("refresh_token")
        self.save_tokens_to_file()
        return self.access_token

    def refresh_access_token(self):
        """
        Refreshes the access token using the refresh token.
        Returns the refreshed access token.
        """
        data = {
            "grant_type": "refresh_token",
            "refresh_token": self.refresh_token,
            "client_id": self.client_id,
            "client_secret": self.client_secret,
        }

        response = requests.post(self.token_url, data=data)
        response.raise_for_status()

        tokens = response.json()
        self.access_token = tokens.get("access_token")
        self.save_tokens_to_file()
        return self.access_token

    def initiate_authorization(self):
        """
        Initiates the OAuth 2.0 authorization process.
        Returns the authorization URL to redirect the user for approval.
        """
        params = {
            "response_type": "code",
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "state": self.state,
            "scope": self.scope,
        }

        authorization_url = f"{self.authorization_url}?{urlencode(params)}"
        return authorization_url

    def parse_job(self, job):
        # Parse the job data and return a standardized format
        return {
            "type": "linkedin_job",
            "title": job.get("title", ""),
            "company": job.get("company", ""),
            "location": job.get("location", ""),
            "description": job.get("description", ""),
            "url": job.get("url", ""),
            # Add other relevant job information
        }

    def fetch(self, n: int):
        if not self.access_token:
            self.load_tokens_from_file()

        # Check if access token is still valid, if not, refresh it
        if not self.access_token or self.token_expired():
            # Initiate authorization and redirect the user for approval
            authorization_url = self.initiate_authorization()
            logger.info(
                f"Please visit the following URL to authorize the application:\n{authorization_url}"
            )

            # After user approval, get the authorization code (manually or through a callback)
            authorization_code = input("Enter the authorization code: ")

            # Exchange the authorization code for an access token
            self.get_access_token(authorization_code)

        # Placeholder: Implement the logic to fetch job information from the LinkedIn API
        # Use the obtained access token for authentication
        # Replace the following URL with the actual LinkedIn API endpoint for jobs
        endpoint_url = "https://api.linkedin.com/v2/jobs"

        # Placeholder: Customize the headers and parameters based on LinkedIn API requirements
        headers = {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

        params = {
            "count": n,
            # Add other required parameters
        }

        try:
            response = requests.get(endpoint_url, headers=headers, params=params)
            response.raise_for_status()
            jobs_data = response.json().get("elements", [])

            # Parse the job data using the parse_job method
            result = [self.parse_job(job) for job in jobs_data]
            return result

        except requests.exceptions.RequestException as e:
            raise Exception(f"Error fetching LinkedIn job feed: {str(e)}")

    def token_expired(self):
        # Check if the access token has expired
        # You may need to adjust this based on the actual expiration time provided by LinkedIn
        return False  # Placeholder: Implement the actual check


if __name__ == "__main__":
    linkedin = LinkedinJobFeed()
    out = linkedin.fetch(1)
    logger.info(out)
