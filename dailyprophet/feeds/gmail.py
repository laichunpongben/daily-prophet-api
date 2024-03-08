import os.path
import logging

from google_auth_oauthlib.flow import InstalledAppFlow

# from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

from ..configs import (
    GMAIL_CREDENTIAL_FILE_NAME,
    GMAIL_ACCOUNT,
)

logger = logging.getLogger(__name__)


class GmailFeed:
    SCOPES = ["https://www.googleapis.com/auth/gmail.readonly"]

    def __init__(self, keyword: str):
        self.keyword = keyword
        self.credential_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"../secrets/{GMAIL_CREDENTIAL_FILE_NAME}",
        )
        self.service = self._create_gmail_service()

    def _create_gmail_service(self):
        """
        Requires manual auth at browser
        """
        flow = InstalledAppFlow.from_client_secrets_file(
            self.credential_file_path, self.SCOPES
        )
        credentials = flow.run_local_server(port=0)
        service = build("gmail", "v1", credentials=credentials)
        return service

    # not working
    # def _create_gmail_service(self):
    #     credentials = service_account.Credentials.from_service_account_file(
    #         self.credential_file_path, scopes=self.SCOPES
    #     )
    #     credentials_delegated = credentials.with_subject(GMAIL_ACCOUNT)
    #     service = build("gmail", "v1", credentials=credentials_delegated)
    #     return service

    def parse(self, message):
        """
        {'id': '18de3a7d6e231ec4', 'threadId': '18de3a7d6e231ec4', 'labelIds': ['UNREAD', 'CATEGORY_UPDATES', 'INBOX'], 'snippet': 'Pathsight', 'payload': {'partId': '', 'mimeType': 'multipart/mixed', 'filename': '', 'headers': [{'name': 'Delivered-To', 'value': '@gmail.com'}, {'name': 'Received', 'value': 'by 2002:a17:906:cb08:b0:a3e:5993:ba73 with SMTP id lk8csp1888899ejb;        Sun, 25 Feb 2024 20:24:04 -0800 (PST)'}, {'name': 'X-Google-Smtp-Source', 'value': 'AGHT+IHJFI9ESENGVbtpLd7oG8RFwmC7YnJ0T2DLhRY'}, {'name': 'X-Received', 'value': 'by 2002:a05:6214:27ca:b0:68f:d594:2896 with SMTP id ge10-2002r7639917qvb.3.1708921444105;        Sun, 25 Feb 2024 20:24:04 -0800 (PST)'}, {'name': 'ARC-Seal', 'value': 'i=1; a=rsa-sha256; t=1708921444; cv=none;        d=google.com; s=arc-20160816;        b=onxqpTFkUltu0dwt3Goi+TBZJJCthcvlqR/wm         OFz23oLnjlgi6mng+c8amX6mqj0w1CzapAcaMjeK/NsYMj5UYWlQhwgT         A3aT0KeaP5v0h5fx7mNB3NzEGjUsIlqktd7+rYtAbFJ81Ln5Go6KjQXARXm4EcnVI0j2         oYleUURq/LKYD/E3pUme1ZjUz218JwT58         SXsdeASXa2jdNAlnqeFeeIX7cKEY+kt         6XGw=='}, {'name': 'ARC-Message-Signature', 'value': 'i=1; a=rsa-sha256; c=relaxed/relaxed; d=google.com; s=arc-20160816;        h=to:subject:mime-version:message-id:from:feedback-id:date         :dkim-signature:dkim-signature;        bh=Dd8SaYUBcnBXCphxZ37ZwXXz+UuOfT9sv6oOL79WltA=;        fh=bWqR3WAN2Ro8A/rPIABVeGcxgSjeimB3OFTjjCA++MU=;        b=MXGOpt4XfzTjJBT7NI3BeWpEPdPNyL         F7pkMTdsXUzT+em3dRtYiW/mGr6IGHk         eodQ==;        dara=google.com'}, {'name': 'ARC-Authentication-Results', 'value': 'i=1; mx.google.com;       dkim=pass header.i=@search.jobsdb.com header.s=t header.b="fUkHAT/H";       dkim=pass header.i=@sendgrid.info header.s=smtpapi header.b=h8gynjJt;       spf=pass (google.com: domain of bounces+23398903-6bc9-=gmail.com@noti.search.jobsdb.com designates 149.72.61.213 as permitted sender) smtp.mailfrom="bounces+23398903-6bc9-=gmail.com@noti.search.jobsdb.com";       dmarc=pass (p=NONE sp=NONE dis=NONE) header.from=jobsdb.com'}, {'name': 'Return-Path', 'value': '<bounces+23398903-6bc9-=gmail.com@noti.search.jobsdb.com>'}, {'name': 'Received', 'value': 'from o14.sgrid.jobsdb.com (o14.sgrid.jobsdb.com. [149.72.61.213])        by mx.google.com with ESMTPS id ed9-20020ad44ea9000000b0068fab5b582esi4553471qvb.132.2024.02.25.20.24.03        for <@gmail.com>        (version=TLS1_3 cipher=TLS_AES_128_GCM_SHA256 bits=128/128);        Sun, 25 Feb 2024 20:24:04 -0800 (PST)'}, {'name': 'Received-SPF', 'value': 'pass (google.com: domain of bounces+23398903-6bc9-=gmail.com@noti.search.jobsdb.com designates 149.72.61.213 as permitted sender) client-ip=149.72.61.213;'}, {'name': 'Authentication-Results', 'value': 'mx.google.com;       dkim=pass header.i=@search.jobsdb.com header.s=t header.b="fUkHAT/H";       dkim=pass header.i=@sendgrid.info header.s=smtpapi header.b=h8gynjJt;       spf=pass (google.com: domain of bounces+23398903-6bc9-=gmail.com@noti.search.jobsdb.com designates 149.72.61.213 as permitted sender) smtp.mailfrom="bounces+23398903-6bc9-=gmail.com@noti.search.jobsdb.com";       dmarc=pass (p=NONE sp=NONE dis=NONE) header.from=jobsdb.com'}, {'name': 'DKIM-Signature', 'value': 'v=1; a=rsa-sha256; c=relaxed/relaxed; d=search.jobsdb.com; h=content-type:from:mime-version:subject:x-feedback-id:to:cc: content-type:from:subject:to; s=t; bh=Dd8SaYUBcnBXCphxZ37ZwXXz+UuOfT9sv6oOL79WltA=; b=fUkHAT/H5jBgAUeIip4Llpsa108Y78xI6W64ljHWvcmveSi9+RN1kzCmfgFe4TniHaRN YZugoaiellP9VJ5mnl03C4hCwPbjtik6tChPv6eM/lZ40GN405cekoRKD+Ad8i59Q=='}, {'name': 'DKIM-Signature', 'value': 'v=1; a=rsa-sha256; c=relaxed/relaxed; d=sendgrid.info; h=content-type:from:mime-version:subject:x-feedback-id:to:cc: content-type:from:subject:to; s=smtpapi; bh=Dd8SaYUBcnBXCphxZ37ZwXXz+UuOfT9sv6oOL79WltA=; b=h8gynjJtcFloGfVPUB0ZJOcKHE 38cS16oyKNNwKFRi7QAjL/mLPJIkyb3YL3+xBgJFf2rP8g8OPKEewCu1st+7SH2UxtZ3/m ZvCP56nkY5sjP7lMqiQHIt6pIZn4jxRMw='}, {'name': 'Received', 'value': 'by filterdrecv-54fb999b49-kwzv8 with SMTP id filterdrecv-54fb999b49-kwzv8-1-65DB551E-C        2024-02-25 14:56:30.543806319 +0000 UTC m=+11304866.707582594'}, {'name': 'Received', 'value': 'from localhost (unknown) by geopod-ismtpd-10 (SG) with ESMTP id wsVqkpfoRB-Xsq_Pdft5ow for <@gmail.com>; Sun, 25 Feb 2024 14:56:29.962 +0000 (UTC)'}, {'name': 'Content-Type', 'value': 'multipart/mixed; boundary="--_NmP-aa0776f6036187e3-Part_1"'}, {'name': 'Date', 'value': 'Mon, 26 Feb 2024 04:24:03 +0000 (UTC)'}, {'name': 'Feedback-Id', 'value': 'saved-search:noti-x:seekasia'}, {'name': 'From', 'value': 'JobsDB Job Alerts <no-reply@search.jobsdb.com>'}, {'name': 'Message-Id', 'value': '<2525427b-0f7c-4521-8714-30bcb3e0b8a3-20240226-hk-asia-1.jobalert@noti.outfra.xyz>'}, {'name': 'Mime-Version', 'value': '1.0'}, {'name': 'Subject', 'value': '20 new jobs for python'}, {'name': 'X-Feedback-ID', 'value': '23398903:SG'}, {'name': 'X-SG-EID', 'value': 'JgdSO'}}]}]}, 'sizeEstimate': 167943, 'historyId': '49850605', 'internalDate': '1708921443000'}
        """
        try:
            payload = message.get("payload", {})
            headers = payload.get("headers", [])
            sender = next(
                (header["value"] for header in headers if header["name"] == "From"),
                "No Sender",
            )
            subject = next(
                (header["value"] for header in headers if header["name"] == "Subject"),
                "No Subject",
            )
            date = next(
                (header["value"] for header in headers if header["name"] == "Date"),
                "No Date",
            )

            return {
                "from": sender,
                "subject": subject,
                "date": date,
            }

        except Exception as e:
            logging.error(f"Error parsing subject: {e}")
            return None

    def fetch(self, n: int):
        try:
            results = (
                self.service.users()
                .messages()
                .list(userId="me", q=f"{self.keyword}")
                .execute()
            )
            messages = results.get("messages", [])

            if not messages:
                logger.warning("No messages found.")
                return []

            parsed_messages = []
            for message in messages[:n]:
                try:
                    msg = (
                        self.service.users()
                        .messages()
                        .get(userId="me", id=message["id"])
                        .execute()
                    )
                    parsed_msg = self.parse(msg)
                    parsed_messages.append(parsed_msg)
                except HttpError as msg_error:
                    logging.error(
                        f"Error fetching message with ID {message['id']}: {msg_error}"
                    )

            return parsed_messages

        except HttpError as error:
            logging.error(f"An error occurred: {error}")
            return []


if __name__ == "__main__":
    gmail = GmailFeed("machine learning")
    result = gmail.fetch(1)
    logger.info(result)
