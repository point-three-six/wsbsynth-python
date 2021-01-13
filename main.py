import praw
import prawcore
import requests
import json
import boto3
from time import time, sleep
from multiprocessing import Process, Manager
import pathlib
import re
import csv
import MySQLdb
import sys

PROD = True

curdir = pathlib.Path(__file__).parent.absolute()

if PROD:
    db = MySQLdb.connect(user="root", passwd="WSBDDmysqlrootytooty",db="wsbdd",charset="utf8mb4")
else:
    db = MySQLdb.connect(user="root", passwd="mysql99",db="wsbdd",charset="utf8mb4")

dbcur = db.cursor()

session = boto3.session.Session(aws_access_key_id="AKIAJ37H4XBNDTXP6GJQ",
                                aws_secret_access_key="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",
                                region_name="us-east-2")

polly = session.client('polly')

url_payload = "http://127.0.0.1:3001"
url_metadata = "http://127.0.0.1:3001/metadata"
url_mentions = "http://127.0.0.1:3001/mentions/"

headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}

manager = Manager()

# messages that have been queued for sending.
# some of these messages may be pending synthesization.
queue = manager.dict()

cur_dd = None

companies = {}

dict_symbols_ambig = []
dict_symbols_ignore = []
dict_common_names = []
rgx_symbols_ignore = r""

rainbow_names = []

def init():
    print("Python listening (prod: ", PROD ,") ...")

    global companies
    global dict_symbols_ambig
    global dict_symbols_ignore
    global dict_common_names
    global rgx_symbols_ignore
    global rainbow_names

    companies = load_symbols()
    dict_symbols_ambig = import_dict("data/stock-ambig.csv", False)
    dict_symbols_ignore = import_dict("data/stock-ignore.csv", False)
    dict_common_names = import_dict("data/stock-common.csv", True)
    rgx_symbols_ignore = r'|'.join(dict_symbols_ignore)
    rainbow_names = import_dict("data/rainbow-names.csv", False)

    reddit = praw.Reddit(client_id="Pe0D9IOmaExQ4w",
                     client_secret="LFc34iPA-Ku7jkE9d0yTpKyECrE",
                     user_agent="debian:WSBDD:v1.0 (by u/buckey5266)")

    # get the latest DD, results are newest to oldest
    dd = detect_dd(reddit)

    # cooldowns
    lastSend = 0
    lastDDDetect = time()
    lastMentionsUpdate = 0

    while True:
        try:
            # stream comments, filter to only comments in our DD
            for msg in reddit.subreddit("wallstreetbets").stream.comments(skip_existing=True):
                if(msg.link_id == dd.name and msg.parent_id.replace('t3_', '') == dd.id):
                    redditor = msg.author
                    body = msg.body
                    flair = msg.author_flair_text

                    # filter then synthesize
                    if(is_valid(body)) and redditor.name not in ["visionarymind"]:
                        extracted_symbols = extract_symbols(body)
                        body = replaces(body)

                        data = manager.dict({"id" : msg.id, "username" : redditor.name, "body" : body, "permalink" : msg.permalink, "mp3" : "", "symbols" : extracted_symbols, "flair" : "", "rainbow" : False, "special" : ""})

                        if flair:
                            data["flair"] = flair

                        if redditor.name.lower() in rainbow_names:
                            data["rainbow"] = True

                        if body.lower() == "guh":
                            data["special"] = "guh.mp3"

                        # store comment & symbol mentions in database if found.
                        store_comment(msg.id, body, redditor.name, extracted_symbols)

                        queue[msg.id] = data

                        # spawn synthesization process
                        p = Process(target=synthesize, args=(queue, msg.id))
                        p.start()

                    d = time()

                    if d - lastMentionsUpdate >= 10:
                        mentions = load_mentions()
                        r = requests.post(url_mentions, data = json.dumps(mentions), headers = headers)
                        lastMentionsUpdate = time()

                    if d - lastSend > 3:
                        payload = extract_queue_payload(queue)
                        num_msg_in_payload = len(payload)
                        
                        if(num_msg_in_payload > 0):
                            print("Sent payload (", str(num_msg_in_payload) ,")")
                            r = requests.post(url_payload, data = json.dumps(payload), headers = headers)

                        lastSend = d
                    
                    if d - lastDDDetect >= 120:
                        dd = detect_dd(reddit)
                        lastDDDetect = time()
                        
        except:
            print("Exception caught, attempting to continue after cooldown ...")
            # raise
            sleep(10)

def is_valid(msg):
    if msg.lower() == "guh":
        return True

    if len(msg) > 200:
        return False
    
    if ' ' not in msg:
        return False

    if re.search(r"nigger|ngger", msg, re.IGNORECASE):
        return False

    return True

def replaces(msg):
    msg = re.sub(r'&#x200B;', '', msg)

    return msg

def synthesize_filter(msg):
    # prevent spam
    msg = re.sub(r'\d{12,}', '', msg)
    msg = re.sub(r'[\U00010000-\U0010ffff\U0001F614]{4,}', '', msg)

    msg = re.sub(r'(?im)\bWSBSynth\b', 'Wallstreet Bets Synth', msg, re.IGNORECASE)
    msg = re.sub(r'(^| )\.(\d+)%', r'point \2 percent', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)([a-zA-Z])\1{5,}', '', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bEOD\b', 'End of Day', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bEOW\b', 'End of Week', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bEOM\b', 'End of Month', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bEOY\b', 'End of Year', msg,re.IGNORECASE)
    msg = re.sub(r'(?im)\bglhf\b', 'good luck have fun', msg)
    msg = re.sub(r'(?im)\bgghf\b', 'good game have fun', msg)
    msg = re.sub(r'(?im)\bf`d up|f\'d up\b', 'fucked up', msg)
    msg = re.sub(r'(?im)\bfk\b', 'fuck', msg)
    msg = re.sub(r'(?im)\bdgaf\b', 'don\'t give a fuck', msg)
    msg = re.sub(r'(?im)\bwtf\b', 'what the fuck', msg)
    msg = re.sub(r'(?im)\bgl\b', 'good luck', msg)
    msg = re.sub(r'(?im)\bffs\b', 'for fuck\'s sake', msg)
    msg = re.sub(r'(?im)\b(buying|selling|buy|sell) big|big (buying|selling|buy|sell)\b', '', msg)
    msg = re.sub(r'(?im)\b(\d)m( line )?chart\b', r'\1 month chart', msg,re.IGNORECASE)
    msg = re.sub(r'(?im)\b(IV|I\.{1,}V\.{1,})\b', 'I V', msg)
    msg = re.sub(r'(\d+)-(\d+)', r'\1 to \2', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bDIS\b', 'Disney', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bTSLA\b', 'Tesla', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bAMZN\b', 'Amazon', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\b(\d+) ?pts\b', r'\1 points', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\b(\d+) pt\b', r'\1 point', msg, re.IGNORECASE)
    msg = re.sub(r"\$\.(\d{1,2})", r'\1 cents', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\btho\b', 'though', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bfml\b', 'fuck my life', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bsob\b', 'son of a bitch', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\b(as?f)\b', 'as fuck', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\brn\b', 'right now', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\btfw\b', 'that face when', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bitm\b', 'in the money', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\botm\b', 'out of the money', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\b401k\b', 'four oh one k', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bev\b', 'electronic vehicles', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bidk\b', 'i don\'t know', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bill\b', 'i\'ll', msg, re.IGNORECASE)
    msg = re.sub(r'->', 'to', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bama\b', 'a m a', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bwth\b', 'what the hell', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bath\b', 'all time high', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bimo\b', 'in my opinion', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bGNUS\b', 'Genius Brands', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\byk\b', 'you know', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\b(\d+)dte\b', r'\1 DTE', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bwya\b', 'where ay at', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bbby\b', 'baby', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\botw\b', 'on the way', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bjfc\b', 'Jesus fucking Christ', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bytd\b', 'year to date', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bbtw\b', 'by the way', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\b52 wk\b', '52 week', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bgtfo\b', 'get the fuck out', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\brh\b', 'robinhood', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\btf\b', 'the fuck', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bstfu\b', 'shut the fuck', msg, re.IGNORECASE)
    msg = re.sub(r'\bTOS\b', 'think or swim', msg, re.IGNORECASE)
    msg = re.sub(r'\bIN\b', 'in', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bbtfd\b', 'buy the fucking dip', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bstfd\b', 'sell the fucking dip', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bhod\b', 'high of day', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bplz\b', 'please', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\b(tmr|tmw|tmrw)\b', 'tomorrow', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bim\b', 'i\'m', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bimo\b', 'in my opinion', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bath\b', 'all time high', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bwsb\b', 'wallstreet bets', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bdd\b', 'due diligence', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bpm\b', 'pre-market', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\btyvm\b', 'thank you very much', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bah\b', 'after hours', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\btbh\b', 'to be honest', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\btda\b', 'TD Ameritrade', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bomg\b', 'oh my god', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\baoc\b', 'Alexandria Ocasio-Cortez', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\bCpt ?TonyStark\b', 'Captain Tony Stark', msg, re.IGNORECASE)
    msg = re.sub(r'(?im)\b(\d+)b\b', r'\1 billion', msg, re.IGNORECASE)
    msg = re.sub(r'\bIT\b', 'it', msg)
    msg = re.sub(r'(?im)\bur\b', 'you\'re', msg, re.IGNORECASE)
    msg = re.sub(r'((https?:\/\/www\.)|(https?:\/\/)|(www\.))(.*?\.(com|biz|net|org|co|de|us|ru|gov|edu|be|me|ai|tv|it))(\/[^\s]+)', r'\5', msg, re.IGNORECASE)

    return msg

def replace_symbols(msg, extracted_companies):
    for symbol in extracted_companies:
        company = companies[symbol][0]

        # replace any occurrence of "corporation" or "inc"
        company = re.sub(r'(?im)\bcorporation|corp\.?\b', '', company)
        company = re.sub(r'(?im)\binc\.?\b', '', company)

        if len(company) <= 20:
            msg = re.sub(r'(?im)\b\$?'+ symbol +r'\b', company, msg)

    return msg

def extract_symbols(msg):
    msg = msg.upper()

    # some fixes
    msg = re.sub(r'(?im)\'|’', '', msg)
    msg = re.sub(r'(?im)\$([a-z])+', r'\1', msg)
    msg = re.sub(r'(?im)\bhold(ing)? onto\b', '', msg)
    msg = re.sub(r'(?im)\bcar stocks?\b', '', msg)
    msg = re.sub(r'(?im)\bthe mouse\b', 'DIS', msg)
    msg = re.sub(r'(?im)\b(average cost|cost me)', '', msg)
    msg = re.sub(r'(?im)\bford\b', '$F', msg)
    msg = re.sub(r'(?im)\bmy self\b', '', msg)
    msg = re.sub(r'(?im)\bthe spot\b', '', msg)
    msg = re.sub(r'(?im)\bhealth care\b', '', msg)
    msg = re.sub(r'(?im)\bcathie wood\b', '', msg)
    msg = re.sub(r'(?im)\bbeat up\b', '', msg)
    msg = re.sub(r'(?im)\brun down\b', '', msg)
    msg = re.sub(r'(?im)\bdrill team \d+\b', '', msg)
    msg = re.sub(r'(?im)\b:D\b', '', msg)
    msg = re.sub(r'(?im)\b(my|of) life\b', '', msg)
    msg = re.sub(r'(?im)\b(O\+|o juice)\b', '', msg)
    msg = re.sub(r'(?im)\bb t c\b', '', msg)
    msg = re.sub(r'(?im)\bbc (I|im)\b', '', msg)
    msg = re.sub(r'(?im)\b(my eyes|my kids)\b', '', msg)
    msg = re.sub(r'(?im)\br u\b', '', msg)
    msg = re.sub(r'(?im)\beyes on\b', '', msg)
    msg = re.sub(r'(?im)\b(buy|sell) low\b', '', msg)
    msg = re.sub(r'(?im)\brun up\b', '', msg)
    msg = re.sub(r'(?im)\b24 hr\b', '', msg)
    msg = re.sub(r'(?im)\bhedge funs?\b', '', msg)
    msg = re.sub(r'(?im)\bwell dump\b', '', msg)
    msg = re.sub(r'(?im)\bdrill team 6\b', '', msg)
    msg = re.sub(r'(?im)\bleg down\b', '', msg)
    msg = re.sub(r'(?im)\broll call\b', '', msg)
    msg = re.sub(r'(?im)\bbeat Q[1-4]\b', '', msg)
    msg = re.sub(r'(?im)\blong run\b', '', msg)
    msg = re.sub(r'(?im)\bjack shit\b', '', msg)
    msg = re.sub(r'(?im)\b(a|the) big\b', '', msg)
    msg = re.sub(r'(?im)\bbig sell off\b', '', msg)
    msg = re.sub(r'(?im)\bmy gut\b', '', msg)
    msg = re.sub(r'(?im)\bstay down\b', '', msg)
    msg = re.sub(r'(?im)\bpost earnings\b', '', msg)
    msg = re.sub(r'(?im)\blong ago\b', '', msg)
    msg = re.sub(r'(?im)\bjack ma\b', '', msg)
    msg = re.sub(r'(?im)\bs\/o\b', '', msg)
    msg = re.sub(r'(?im)\bin plus\b', '', msg)
    msg = re.sub(r'(?im)\bpush ups\b', '', msg)
    msg = re.sub(r'(?im)\blow key\b', '', msg)
    msg = re.sub(r'(?im)\b(r fuc?k|r so fuc?k|r not fuc?k|r (fukt|fukked))\b', '', msg)
    msg = re.sub(r'(?im)\bbeat earnings?\b', '', msg)

    msg = re.sub(r'(?im)\b({0})\b'.format(rgx_symbols_ignore), '', msg, re.IGNORECASE | re.M)
    
    extracted = {}
    for symbol in companies:
        if re.search(r"\b\$?"+ symbol +r"\b", msg):
            extracted[symbol] = companies[symbol][0]

    # in the case of ambiguous symbols
    # check to see if they are a false positive
    # if so, remove them.
    to_remove = []
    for symbol in list(extracted):
        if symbol in dict_symbols_ambig:
            if is_ambiguous_false_positive(symbol, msg):
                del extracted[symbol]

    # check for common names
    for name, symbol in dict_common_names:
        if re.search(r"\b\$?"+ re.escape(name) +r"\b", msg):
            extracted[symbol] = companies[symbol][0]
    
    return extracted

def is_ambiguous_false_positive(symbol, msg):
    symbol = symbol.upper()
    msg = msg.upper()

    # these are a list of regex patterns that, if found
    # are good enough to determine that the symbol is not
    # a false positive.
    valid_patterns = [
        r"\${0}\b".format(symbol),
        r"\d+k? in {0}\b".format(symbol),
        r"\d+ {0}\b".format(symbol),
        r"\b{0} \d+".format(symbol),
        r"\bon {0}\b".format(symbol),
        r"\b(riding|ride|short(ing)?|shorted|pumping|pump|dumping|dumped|dump|long(ing)?|longed|buy(ing)?|sell(ing)?|bought|sold|stocks?|my|of|into) {0}\b".format(symbol),
        r"\b{0} (short(ing)?|leaps?|shorted|pumpimng|pump|dumping|dumped|dump|long(ing)?|longed|buy(ing)?|sell(ing)?|bought|sold|stocks?|shares?|rocketing|rocket|earnings|dipping|dipped|dip|up|down|ripped|ripping|bagholder|to the moon)\b".format(symbol),
        r"\b{0} Q[1-4]".format(symbol),
        r"\$\d+ {0}\b".format(symbol),
        r"\b{0} \$\d+".format(symbol),
        r"\b{0} to \$?\d+".format(symbol),
        r"\b(puts?|calls?|hedges?|holding|hold|bagholding|baghold|leaps?)( on)? {0}\b".format(symbol),
        r"\b{0} (puts?|calls?|hedges?|gapping|gapped|gap|printing|prints|print)\b".format(symbol),
        r"\b{0} gang+\b".format(symbol)
    ]

    for pttrn in valid_patterns:
        if re.search(pttrn, msg, re.IGNORECASE):
            print(msg)
            print(pttrn)
            return False

    return True

def store_comment(msg_id, msg_body, msg_username, symbols):
    try:
        dbcur.execute("INSERT INTO comments (dd_id, comment_id, username, text, date) VALUES (%s, %s, %s, %s, NOW())", (cur_dd.id, msg_id, msg_username, msg_body))

        insert_id = dbcur.lastrowid

        if symbols:
            for symbol in symbols:
                dbcur.execute("INSERT INTO comments_symbols (comment_id, symbol_id) VALUES (%s, %s)", (insert_id, companies[symbol][1]))
        
        db.commit()
    except:
        print("Failed to insert comment and store mentions.")
        print(msg_id)
        print(msg_body)
        print(msg_username)
        print(symbols)

def synthesize(queue, id):
    msg = queue[id]

    text = synthesize_filter(msg["body"])

    if(msg["symbols"]):
        text = replace_symbols(text, msg["symbols"])

    if PROD:
        path = "/var/www/html/synthesized/"
    else:
        path = "/usr/local/var/www/wsbdd/synthesized/"

    if PROD:
        voices = ["Brian", "Amy"]
    else:
        voices = ["Brian"]

    for voice in voices:
        r = polly.synthesize_speech(
            Engine = "standard",
            OutputFormat = "mp3",
            Text = text,
            VoiceId = voice
        )
    
        fname = str(id) + "_"+ voice +".mp3"
        
        
        with open(path + fname, 'wb') as f:
            f.write(r['AudioStream'].read())

    # proxy object workaround
    queue[id] = {
        "id" : msg["id"],
        "username" : msg["username"],
        "flair" : msg["flair"],
        "rainbow" : msg["rainbow"],
        "body" : msg["body"],
        "symbols" : msg["symbols"],
        "permalink" : msg["permalink"],
        "mp3" : str(id) + "_Brian.mp3",
        "special" : msg["special"]
    }
    

def extract_queue_payload(queue):
    # payload is any message in queue that has been synthesized
    payload = []

    for id in list(queue.keys()):
        if queue[id]["mp3"]:
            payload.append(queue[id].copy())
            del queue[id]

    return payload

def load_symbols():
    num_symbols = dbcur.execute("SELECT id, symbol, company FROM symbols")
    symbols = { x[1] : (x[2], x[0]) for x in dbcur.fetchall() }

    print(num_symbols, "symbols loaded.")

    return symbols

def load_mentions(limit=10):
    dbcur.execute("SELECT s.symbol, count(cs.symbol_id) AS mentions FROM symbols s LEFT JOIN comments_symbols cs ON cs.symbol_id = s.id LEFT JOIN comments c ON c.id = cs.comment_id WHERE c.dd_id = %s GROUP BY (s.symbol) ORDER BY mentions DESC LIMIT %s", (cur_dd.id, limit))
    mentions = dbcur.fetchall()

    return mentions

def import_dict(fname, split):
    values = []
    with open(str(curdir) + "/" + fname) as f:
        values = f.readlines()
        if split:
            values = [value.rstrip('\n').split(',') for value in values]
        else:
            values = [value.rstrip('\n') for value in values]
        values.sort(key=len, reverse=True)
    return values

def build_rgx_dict_str(values):
    rgx_str = '|'.join(values)
    return values

def detect_dd(reddit):
    global cur_dd
    
    dd = None
    new_posts = list(reddit.subreddit('wallstreetbets').new(limit=800))
    for post in new_posts:
        if post.link_flair_text == "Daily Discussion" or post.link_flair_text == "Weekend Discussion":
        #if post.title.lower() == "daily discussion thread for november 10, 2020":
            print(post.title.lower())
            dd = post
            break

    # new dd
    if not cur_dd or cur_dd.id != dd.id:
        cur_dd = dd
        print(dd)
        metadata = {
            "url" : dd.url,
            "title" : dd.title
        }

        # log new dd to database
        if not dbcur.execute("SELECT (1) FROM threads WHERE dd_id = %s", (dd.id,)):
            dbcur.execute("INSERT INTO threads (dd_id, title, link, date) VALUES (%s, %s, %s, NOW())", (dd.id, dd.title, dd.url))
            db.commit()

        # send new dd info to node server
        r = requests.post(url_metadata, data = json.dumps(metadata), headers = headers)

    return dd
        

if __name__ == "__main__":
    init()
