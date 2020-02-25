import json
import time
import datetime
import os
import records
import pandas as pd
import numpy as np
import requests
from toolz import dissoc, get_in

BOTSERVER_URL = os.getenv('BOTSERVER_URL')
FB_PAGE_ID = os.getenv('FB_PAGE_ID')
BAILER_HOURS = float(os.getenv('BAILER_HOURS'))
BAILER_DAYS = int(os.getenv('BAILER_DAYS'))
BAILER_FORM_ENG = os.getenv('BAILER_FORM_ENG')
BAILER_FORM_HINDI = os.getenv('BAILER_FORM_HINDI')
FORMS = { 'eng': BAILER_FORM_ENG, 'hindi': BAILER_FORM_HINDI }

def get_ref(d):
    md = get_in(['message', 'metadata'], d)
    if md:
        return json.loads(md).get('ref')

def get_df(conn_string):
    db = records.Database(conn_string)
    dat = db.query('select * from messages')
    dat = (r.as_dict() for r in dat)
    dat = ({**json.loads(r['content']), 'userid': r['userid']} for r in dat)
    dat = (dissoc(d, 'recipient', 'sender') for d in dat)
    dat = ({**d, 'text': get_in(['message', 'text'], d), 'ref': get_ref(d)} for d in dat)
    dat = (dissoc(d, 'message', 'referral', 'user', 'page', 'event', 'postback') for d in dat)
    return pd.DataFrame(list(dat))

def get_times(df):
    df = df.sort_values('timestamp')
    args = np.argwhere((df.ref == 'QQQ').values)[:, 0]
    timestamp = df.iloc[args[0]].timestamp
    last_seen = df.iloc[args[-1]].timestamp
    try:
        next_seen = df.iloc[args[-1] + 1].timestamp
    except IndexError:
        next_seen = int(time.mktime(datetime.datetime.now().timetuple())) * 1000

    userid = df.userid.iloc[0]
    lang = df.lang.iloc[0]
    return pd.DataFrame([{ 'userid': userid, 'lang': lang, 'time': timestamp, 'pause': next_seen - last_seen}])


def get_blocked(df, hours):
    df = df.copy()

    engs = df.text.str.contains('Hey! Have a look at these videos') == True
    hindis = df.text.str.contains('Kripaya is sandesh ko Messenger par apne mitron ko bhejen') == True

    df.loc[engs, 'ref'] = 'QQQ'
    df.loc[hindis, 'ref'] = 'QQQ'

    eng_users = df[engs].userid.unique()
    hindi_users = df[hindis].userid.unique()

    eng_affected = df[df.userid.isin(eng_users)].reset_index(drop=True)
    eng_affected['lang'] = 'eng'

    hindi_affected = df[df.userid.isin(hindi_users)].reset_index(drop=True)
    hindi_affected['lang'] = 'hindi'

    affected = pd.concat([eng_affected, hindi_affected]).reset_index(drop=True).sort_values(['userid', 'timestamp'])
    pauses = affected.groupby('userid').apply(get_times).reset_index(drop=True)
    blocked = pauses[pauses.pause > 1000*60*60*hours].reset_index(drop=True)

    return blocked

def get_bailouts(blocked, days):
    blocked['bailout_time'] = blocked.time.map(lambda i: datetime.datetime.fromtimestamp(i/1000)) + datetime.timedelta(days=days)
    return blocked[blocked['bailout_time'] < datetime.datetime.now()]

def conn_string():
    user = os.getenv('CHATBASE_USER')
    password = os.getenv('CHATBASE_PASSWORD')
    db = os.getenv('CHATBASE_DATABASE')
    host = os.getenv('CHATBASE_HOST')
    port = os.getenv('CHATBASE_PORT')
    return f'cockroachdb://{user}:{password}@{host}:{port}/{db}'


def _bail(page, user, form):
    return { 'event': {'type': 'bailout',
                       'value': {'form': form}},
             'user': user,
             'page': page }

def bailout(user, lang):
    form = FORMS[lang]
    page = FB_PAGE_ID
    dat = _bail(page, user, form)
    res = requests.post(f'{BOTSERVER_URL}/synthetic', json=dat)
    return res

def main():
    df = get_df(conn_string())
    blocked = get_blocked(df, BAILER_HOURS)
    bails = get_bailouts(blocked, BAILER_DAYS)

    for i,r in bails.iterrows():
        bailout(r.userid, r.lang)

if __name__ == '__main__':
    main()
