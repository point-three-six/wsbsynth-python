import praw
import requests
import json
import boto3
from time import time
from multiprocessing import Process, Manager
import pathlib
import re
import csv

PROD = False

curdir = pathlib.Path(__file__).parent.absolute()

session = boto3.session.Session(aws_access_key_id="AKIAJ37H4XBNDTXP6GJQ",
                                aws_secret_access_key="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",
                                region_name="us-east-2")

polly = session.client('polly')

if PROD:
    url = "http://167.71.255.106:3001"
else:
    url = "http://127.0.0.1:3001"

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

manager = Manager()

# messages that have been queued for sending.
# some of these messages may be pending synthesization.
queue = manager.list()


companies = {}

def init():
    global companies
    companies = load_symbols()

    reddit = praw.Reddit(client_id="Pe0D9IOmaExQ4w",
                     client_secret="LFc34iPA-Ku7jkE9d0yTpKyECrE",
                     user_agent="debian:WSBDD:v1.0 (by u/buckey5266)")

    new_posts = list(reddit.subreddit('wallstreetbets').new(limit=1000))

    # get the latest DD, results are newest to oldest
    dd = None
    for post in new_posts:
        if(post.link_flair_text == "Daily Discussion" or post.link_flair_text == "Weekend Discussion"):
            dd = post
            break

    # send every 3 seconds
    lastSend = 0

    # stream comments, filter to only comments in our DD
    for msg in reddit.subreddit("wallstreetbets").stream.comments(skip_existing=True):
        if(msg.link_id == dd.name and msg.parent_id.replace('t3_', '') == dd.id):
            redditor = msg.author
            body = msg.body

            # filter then synthesize
            if(is_valid(body)):
                extracted_symbols = extract_symbols(body)

                data = {"id" : msg.id, "username" : redditor.name, "body" : body, "mp3" : "", "symbols" : extracted_symbols}

                queue.append(data)

                # spawn synthesization process
                p = Process(target=synthesize, args=(msg.id,))
                p.start()


            d = time()
            if d - lastSend > 3:
                payload = extract_queue_payload(queue)
                num_msg_in_payload = len(payload)

                print("sent:")
                for msg in payload:
                    print(msg["id"])
                
                if(num_msg_in_payload > 0):
                    r = requests.post(url, data = json.dumps(payload), headers = headers)

                lastSend = d

def is_valid(msg):
    if len(msg) > 150:
        return False
    
    if ' ' not in msg:
        return False

    if re.search(r"nigger|ngger", msg, re.IGNORECASE):
        return False

    return True

def synthesize_filter(msg):
    msg = re.sub(r'\bEOD\b', 'End of Day', msg, re.IGNORECASE)
    msg = re.sub(r'\bEOW\b', 'End of Week', msg, re.IGNORECASE)
    msg = re.sub(r'\bEOM\b', 'End of Month', msg, re.IGNORECASE)
    msg = re.sub(r'\bEOY\b', 'End of Year', msg,re.IGNORECASE)
    msg = re.sub(r'(\d)+-(\d)+', r'\1 to \2', msg, re.IGNORECASE)
    msg = re.sub(r'\bDIS\b', 'Disney', msg, re.IGNORECASE)
    msg = re.sub(r'\bTSLA\b', 'Tesla', msg, re.IGNORECASE)
    msg = re.sub(r'\bAMZN\b', 'Amazon', msg, re.IGNORECASE)
    msg = re.sub(r"\$\.(\d{1,2})", r'\1 cents', msg, re.IGNORECASE)
    msg = re.sub(r'\btho\b', 'though', msg, re.IGNORECASE)
    msg = re.sub(r'\baf\b', 'as fuck', msg, re.IGNORECASE)
    msg = re.sub(r'\brn\b', 'right now', msg, re.IGNORECASE)
    msg = re.sub(r'\b401k\b', 'four oh one k', msg, re.IGNORECASE)
    msg = re.sub(r'\bidk\b', 'i don\'t know', msg, re.IGNORECASE)
    msg = re.sub(r'->', 'to', msg, re.IGNORECASE)
    msg = re.sub(r'\bama\b', 'a m a', msg, re.IGNORECASE)
    msg = re.sub(r'\bwth\b', 'what the hell', msg, re.IGNORECASE)
    msg = re.sub(r'\bath\b', 'all time high', msg, re.IGNORECASE)
    msg = re.sub(r'\bimo\b', 'in my opinion', msg, re.IGNORECASE)
    msg = re.sub(r'\bGNUS\b', 'Genius Brands', msg, re.IGNORECASE)
    msg = re.sub(r'\byk\b', 'you know', msg, re.IGNORECASE)
    msg = re.sub(r'\bwya\b', 'where ay at', msg, re.IGNORECASE)
    msg = re.sub(r'\botw\b', 'on the way', msg, re.IGNORECASE)
    msg = re.sub(r'\bytd\b', 'year to date', msg, re.IGNORECASE)
    msg = re.sub(r'\bbtw\b', 'by the way', msg, re.IGNORECASE)
    msg = re.sub(r'(https?:\/\/)?(www\.)?(.*\.(com|net|org|co|us|ru|gov|edu))(\/[^\s]+)?', r'\3', msg, re.IGNORECASE)


    return msg

def replace_symbols(msg, extracted_companies):
    for symbol in extracted_companies:
        msg = re.sub(r'\b\$?'+ symbol +r'\b', companies[symbol], msg)

    return msg

def extract_symbols(msg):
    # let's try to prevent some edge cases
    # remove single characters unless next to a $
    msg = re.sub(r'\b(?<!\$)\w{1,1}\b', '', msg, re.IGNORECASE | re.M)
    msg = re.sub(r'\bEOD|am|et|it|so|fly\b', '', msg, re.IGNORECASE | re.M)
    
    extracted = {}
    for symbol in companies:
        if re.search(r"\b\$?"+ symbol +r"\b", msg):
            extracted[symbol] = companies[symbol]

    return extracted

def synthesize(id):
    global queue
    msg = next((x for x in queue if x['id'] == id), None)

    text = synthesize_filter(msg["body"])

    if(msg["symbols"]):
        text = replace_symbols(text, msg["symbols"])
    
    r = polly.synthesize_speech(
        Engine = "standard",
        OutputFormat = "mp3",
        Text = text,
        VoiceId = "Brian"
    )
    
    
    fname = str(id) + ".mp3"

    if PROD:
        path = "/var/www/html/synthesized/"
    else:
        path = "/usr/local/var/www/wsbdd/synthesized/"
    
    with open(path + fname, 'wb') as f:
        f.write(r['AudioStream'].read())

    # because of a weird issue with proxy objects not
    # being able to be nested, we can only modify the
    # first level of queue and not any nested elements.
    # so rebuild the object and then replace the elemnt
    # at that index with the new dictionary.
    i = queue.index(msg)
    
    queue[i] = {
        "id" : msg["id"],
        "username" : msg["username"],
        "body" : msg["body"],
        "symbols" : msg["symbols"],
        "mp3" : fname
    }
    

def extract_queue_payload(queue):
    # payload is any message in queue that has been synthesized
    payload = [x for x in queue if x["mp3"]]

    # update queue, only keep messages in queue that have not
    # yet been synthesize
    # we need to use .remove() to have changes applied appropriately
    # to the proxy object.
    for msg in queue:
        if msg["mp3"]:
            queue.remove(msg)

    return payload

def load_symbols():
    files = ['amex.csv', 'nasdaq.csv', 'nyse.csv']
    
    symbols = {}

    for fname in files:
        with open(str(curdir) + '/exchanges/'+ fname) as f:
            reader = csv.reader(f, delimiter=',')

            for row in reader:
                symbols[row[0]] = row[1]
        
    return symbols
        

if __name__ == "__main__":
    init()
