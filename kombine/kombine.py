#!/usr/bin/env python

import datetime
import gzip
import json
import os
import re
import sys
import syslog
import time
from logging.handlers import RotatingFileHandler

from pygtail import Pygtail


class LogEntry(object):

    def __init__(self):
        self.queue_id = ""
        self.from_email = ""
        self.email= ""
        self.campaign = ""
        self.status = ""
        self.initiated_at = ""
        self.sent_at = ""
        self.campdt = ""


class Kombiner(object):
    """
    A simple module to kombine the postfix logs to be fed
    to fluentd or similar log tools.
    """

    def __init__(self, input_file, output_file=None, interval=5,
            max_size=1024 * 1024 * 2000, backup_count=7, file_handler=None):

        self.input_file = input_file
        self.output_file = output_file
        self.interval = interval
        self.identifier = r'\w{3}\s+\d+\s+\d+:\d{2}:\d{2}\s[\w\d\/\-\.]+\s[\w\d\/\-\.]+\[\d+\]:\s\w+'
        self.queue_regex = r'^\w{3}\s+\d+\s+\d+:\d{2}:\d{2}\s[\w\d\/\-\.]+\s[\w\d\/\-\.]+\[\d+\]:\s(\w+)'
        self.from_regex = r'from=<([\w\d@.-]+)'
        self.to_regex = r'to=<([\w\d@.-]+)'
        self.mailer_tag_regex = r'X-MailerTag:\s([\w\d]+)'
        self.status_regex = r'status=([\w\d]+)'
        self.date_regex = r'^(\w{3}\s+\d+\s+\d+:\d{2}:\d{2})'
        self.max_size = max_size
        self.backup_count = backup_count
        self.entries = {}
        self.file_handler = file_handler

        ## Set default output file
        if self.output_file is None:
            self.output_file = "/var/log/kombine/kombine.log"

        ## prepare for listening
        self._prepare()

    def _prepare(self):
        """
        Does the preparation for reading a log file.
        """
        try:
            if not os.path.exists(os.path.dirname(self.output_file)):
                os.makedirs(os.path.dirname(self.output_file))
            writer = open(self.output_file, "w")
            writer.close()

        except Exception as err:
            syslog.syslog(syslog.LOG_ERR, "KOMBINE: could not create file "\
                    "make sure the output folder has the correct owner and "\
                    "permissions.")
        if not self.file_handler:
            self.file_handler = RotatingFileHandler(self.output_file,
                    maxBytes=self.max_size, backupCount=self.backup_count)

    def kombine(self):
        """
        Starts tailing and reading the given input file.
        """

        try:
            for line in Pygtail(self.input_file):
                self._process_line(line)

            ## clear deffered mails
            self.entries = {}

            ## reloop
            time.sleep(self.interval)
            self.kombine()
        except KeyboardInterrupt:
            sys.stdout.write("\nBye!")
        except OSError:
            time.sleep(self.interval)
            self.kombine()


    def _process_line(self, line):
        """
        Processes the given line.
        """
        matches = re.findall(self.identifier, line)
        if matches:
            try:
                qid = re.findall(self.queue_regex, line)[0]

                ## Get object from current entries or create
                ## a new one and add it to entries.
                if qid in self.entries:
                    obj = self.entries[qid]
                else:
                    obj = LogEntry()
                    obj.queue_id = qid
                    self.entries[qid] = obj

                self.parse_line(obj, line)
            except Exception as err:
                syslog.syslog(syslog.LOG_ERR, "KOMBINE: %s : %s" % (str(err), line))
        else:
            syslog.syslog(syslog.LOG_ERR, "KOMBINE Unmatched Postfix: %s" % line)

    def get_mailer_tag(self, line):
        """
        Gets and returns X-MailerTag, From and To values from
        the given line.
        """

        tag = re.findall(self.mailer_tag_regex, line)[0]
        frm = re.findall(self.from_regex, line)[0]
        to = re.findall(self.to_regex, line)[0]

        return tag, frm, to

    def log_entry(self, obj):
        """
        Logs to the output file and removes the object from memory
        """
        if (hasattr(obj, 'email') and obj.email) and \
                (hasattr(obj,'from_email') and obj.from_email):
            campaign_date = datetime.datetime.strptime(obj.initiated_at + " " + str(datetime.datetime.today().year), "%b %d %H:%M:%S %Y")
            obj.campdt = campaign_date.strftime("%Y%m%d")
            json_string = json.dumps(obj.__dict__)
            self.file_handler.stream.write(json_string + "\n")
        del self.entries[obj.queue_id]

    def parse_line(self, obj, line):
        """
        Parses the required information from the given line.
        """

        if "client=" in line:
            obj.initiated_at = re.findall(self.date_regex, line)[0]

        elif "X-MailerTag" in line:
            tag, frm, to = self.get_mailer_tag(line)
            obj.campaign = tag
            obj.from_email = frm
            obj.email = to
            self.entries[obj.queue_id] = obj

        elif "status=" in line:
            obj.status = re.findall(self.status_regex, line)[0]
            obj.sent_at = re.findall(self.date_regex, line)[0]
            self.entries[obj.queue_id] = obj

        elif "removed\n" in line:
            self.log_entry(obj)

        elif "Relay access denied" in line:
            obj.status = "denied"
            self.entries[obj.queue_id] = obj
            self.log_entry(obj)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        sys.stdout.write("Please specify the input file path: e.g. "\
                "python kombine.py /var/log/mail/mail.log\n")
    else:
        kombiner = Kombiner(sys.argv[1])
        kombiner.kombine()
