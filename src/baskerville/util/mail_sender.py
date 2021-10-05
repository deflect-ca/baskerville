# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.
import smtplib

class MailSender(object):

    def __init__(self, from_email, server, port, user, password):
        super().__init__()
        self.from_email = from_email
        self.server = server
        self.port = port
        self.user = user
        self.password = password

    def send(self, emails, subject, body):

        # Import the email modules we'll need
        from email.mime.text import MIMEText

        # Open a plain text file for reading.  For this example, assume that
        # the text file contains only ASCII characters.
        msg = MIMEText(body)
        msg['Subject'] = subject
        msg['From'] = self.from_email

        try:
            # Send the message via our own SMTP server, but don't include the
            # envelope header.
            smtp = smtplib.SMTP()
            smtp.connect(self.server, self.port)
            smtp.ehlo()
            smtp.starttls()
            smtp.ehlo()
            smtp.login(self.user, self.password)
            try:
                smtp.sendmail(self.from_email, emails, msg.as_string())
            finally:
                smtp.quit()
        except Exception as ex:
            print(str(ex))
