#!/usr/bin/python
# -*- encoding: utf-8 -*-
import urllib, urllib2
from unidecode import unidecode
import yaml
import sys
import csv
import requests
import threading
import Queue
import datetime
from mailer import Mailer
from mailer import Message
import zipfile
import codecs
from bs4 import BeautifulSoup
def str_to_hex(text):
    text = text.strip("u").strip("'") 
    arabic_hex = [hex(ord(b)).replace("x","").upper().zfill(4) for b in text]
    arabic_hex.append("000A")
    text_update = "".join(arabic_hex)
    return text_update
DATAURL = "https://docs.google.com/spreadsheets/d/1xjPWC3pBZVTDo4bYJAHkr7TyHyDO9YBS7XDbMCgmbIE/export?format=csv"
r = requests.get(DATAURL)
datafile = "data" + str(datetime.datetime.now().strftime("%Y-%m-%d")) + ".csv"
if r.status_code is 200:
	f = open(datafile,"w")
	kl = r.text
	f.write(r.text)
	f.close()
CONTENTURL = "https://docs.google.com/spreadsheets/d/144fuYSOgi8md4n2Ezoj9yNMi6AigoXrkHA9rWIF0EDw/export?format=zip"
contentfile = "con" + str(datetime.datetime.now().strftime("%Y-%m-%d")) + ".zip"
r = requests.get(CONTENTURL)
if r.status_code is 200:
	f = open(contentfile,"wb")
	f.write(r.content)
	f.close()
zf = zipfile.ZipFile(contentfile)
data = zf.read("Sheet1.html")
soup = BeautifulSoup(data,"html5lib")
td = soup.findAll("td")
en = td[2].text.strip()
ar = td[3].text.strip()
class SendSMS(object):
      def __init__(self,SMS_DICT):
          self.url = "http://www.smscountry.com/smscwebservice_bulk.aspx"
          self.values = {'user' : 'wadiops', 'passwd' : 'w@d1Rock$', 'DR':'Y'}
          self.SMS_DICT = SMS_DICT

      def bombay_inbound(self, payload):
         queue_write = write_to_sqs('bombay_sms', to_json(payload))
         return queue_write

      def postsms(self, payload,ml):
         assert {'message', 'cell_phone'} & payload.viewkeys(), "Invalid Data Packet"
         self.values.update(payload)
         self.values.update({'mtype':ml})
         data = urllib.urlencode(self.values)
         request = urllib2.Request(self.url,data)
         wp = urllib2.urlopen(request)
         print wp.read()
         return True


def sendsend():
  global work
  while(work.qsize() > 0):
    x = work.get()
    if x[1].strip() in "Arabic":
      print x[0]
      message_text = self.SMS_DICT['ar']
      payload = {'message': str_to_hex(message_text),'mobilenumber':x[0].strip("=").strip()}
      self.postsms(payload,"OL")
    elif x[1].strip() in "English":
      print x[0]
      message_text = self.SMS_DICT['en']
      payload = {'message': message_text,'mobilenumber':x[0].strip("=").strip()}
      self.postsms(payload,"N")
  return

ss = SendSMS()
def yaml_loader(filename):
    f = open(filename)
    yml_dict = yaml.safe_load(f)
    f.close()
    return yml_dict
"""sms_con = csv.reader(con.split('\n'))
print next(sms_con)
print next(sms_con)
smsdict = []
i = 0
for x in sms_con:
	if i is 0:
		pass
	else:
		smsdict.append(x)
		i = i + 1
print smsdict"""
SMS_DICT = {'ar':ar,'en':en}
print SMS_DICT
data = []
cc = csv.reader(kl.split("\n"))
for x in cc:
	data.append(x)
work = Queue.Queue()
i = 0
for x in data:
	if i is 0:
		pass
		i = i + 1
	else:
		work.put(x)
totalsent = work.qsize()
print totalsent


threads = []
for i in range(2):
	thread = threading.Thread(target=sendsend)
	thread.start()
	threads.append(thread)
print "Waiting"
for thread in threads:
	thread.join()
print "Done"
message = Message(From="Jarvis",
                  To=["ashmeet@jlabs.co"],
                  Subject="Price Comparison")
message.Body = "Message sent to " + str(totalsent) + " customers"


sender = Mailer('smtp.gmail.com',port=587,use_tls=True)
sender.login("pricing@jlabs.co","coldplay@123")
sender.send(message)
