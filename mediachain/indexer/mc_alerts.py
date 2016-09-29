#!/usr/bin/env python

import mc_config
from tempfile import NamedTemporaryFile
import json
import urllib

import tornado
import tornado.gen
from tornado.httpclient import AsyncHTTPClient


class MCAlerts:
    
    def __init__(self,
                 ):
        self.done_alerts = set()
    
    @tornado.gen.coroutine
    def send_alert_tornado(self,
                           message = False,
                           file_bytes = False,
                           file_ext = 'txt',
                           alert_key = False,
                           only_first = True,
                           user_name = "bot-image-search",
                           channel = mc_config.MC_SLACK_CHANNEL,
                           slack_webhook_url = mc_config.MC_SLACK_WEBHOOK,
                           ):
        print ('PREPARING_ALERT', slack_webhook_url, '->', channel)

        alert_key = json.dumps(alert_key, sort_keys=True)

        if only_first and (alert_key in self.done_alerts):
            return
        self.done_alerts.add(alert_key)


        if not channel:
            channel = '#labs-tech-alerts'

        if not channel.startswith('#'):
            channel = '#' + channel
                        
        assert channel.startswith('#')
        
        hh = {"text": message,
              "username": user_name,
              "channel": channel,
              "icon_emoji": ":ghost:",
              }

        if '95.111' in repr(hh):
            return

        body = urllib.urlencode({'payload': json.dumps(hh)}).encode('utf8')

        print ('SENDING_ALERT', body)
        
        response = yield AsyncHTTPClient().fetch(slack_webhook_url,
                                                 method = 'POST',
                                                 connect_timeout = 10,
                                                 request_timeout = 10,
                                                 body = body,
                                                 allow_nonstandard_methods = True,
                                                 )
        
        d2 = response.body
        
        print ('ALERT_SENT', d2)
            
