import urllib, json
import csv
import sqlite3
import pdb
import argparse
import os
from time import sleep
import time

parser = argparse.ArgumentParser(description='UN commodity database extraction')
parser.add_argument('-create_db', dest='create_db', action='store_true')
parser.set_defaults(create_db=False)
args = parser.parse_args()

def load_country_codes(filename):
    f = open(filename,'rb')
    reader = csv.reader(f)
    headers = reader.next()
    codes = {}
    for h in headers:
        codes[h] = []
    for row in reader:
        for h, v in zip(headers, row):
            codes[h].append(v)

    return codes


def retrieve(url,waiting_time):
	not_complete = True
	while not_complete:
		try: 
			r=urllib.urlopen(url)
    			data=json.loads(r.read())
    			amount_rows=data['validation']['count']['value']
    			status = data['validation']['status']['value']
    			sleep(waiting_time)
			not_complete = False
		except:
			print "Unexpected error:", sys.exc_info()[0]
			sleep(60)

    	return data, amount_rows, status

def write2tradetable(conn,data):
    # WRITE retrieved DATA into DATABASE
    c = conn.cursor()
    rows = []
    for row in data['dataset']:
        rows.append((row['TradeValue'],row['cmdCode'],row['cmdDescE'],row['pt3ISO'],row['ptCode'],
                         row['ptTitle'],row['rgDesc'],row['rtCode'],row['rt3ISO'],row['rtTitle'],row['yr'],))

    sql_cmd = 'INSERT INTO TradeTable (TradeValue, cmdCode, cmdDescE, pt3ISO, ptCode, \
        ptTitle, rgDesc, rtCode, rt3ISO, rtTitle, yr) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'
    if len(rows)>0:
        c.executemany(sql_cmd,rows)
        conn.commit()

def write2statustable(conn,url,status,amount_rows,country,year,partner=None):
    sql_cmd = 'INSERT INTO URLSuccessTable (URL, status, items, country, year, partner) VALUES (?,?,?,?,?,?)'
    c = conn.cursor()
    params = (url,status,amount_rows,country,year,partner,)
    c.execute(sql_cmd,params)
    conn.commit()

def checkfromstatustable(conn,url):
    sql_cmd = 'SELECT status,items,country,year,partner from URLSuccessTable WHERE URL="{}"'.format(url)
    c = conn.cursor()
    c.execute(sql_cmd)
    data = c.fetchone()
    if data is None:
        status = -1
    else:
        status = data[0]

    return status,data

def create_database(filename):
    os.remove(filename)
    conn = sqlite3.connect(filename)
    c = conn.cursor()
    c.execute('CREATE TABLE TradeTable (TradeValue FLOAT, cmdCode VARCHAR, cmdDescE VARCHAR, \
        pt3ISO VARCHAR, ptCode VARCHAR, ptTitle VARCHAR, rgDesc VARCHAR, \
        rtCode VARCHAR, rt3ISO VARCHAR, rtTitle VARCHAR, yr VARCHAR, \
        CONSTRAINT TradeEntry PRIMARY KEY (TradeValue, cmdCode, cmdDescE, pt3ISO, ptCode, ptTitle, rgDesc, rtCode, rt3ISO, rtTitle, yr))')
    c.execute('CREATE TABLE URLSuccessTable ( URL VARCHAR PRIMARY KEY, status INTEGER, items INTEGER, country VARCHAR, year VARCHAR, partner VARCHAR)')
    conn.commit()
    conn.close()


def sub_query(conn,codes,year,country_code):
    for k,country_code_partner in enumerate(codes['Country Code']):
        country_partner = codes['Country Name'][k]
        url = 'http://comtrade.un.org/api/get?type=C&freq=A&px=S3&ps={}&r={}&p={}&rg=all&cc=ALL'.format(year,country_code,country_code_partner)
        sql_status,sql_data = checkfromstatustable(conn,url)
        if sql_status>-1:
            print "{} <-> {} | retrieving {} entries in year {}".format(sql_data[2],sql_data[4],sql_data[1],sql_data[3])
            continue # skip data already in database

        data, amount_rows, status = retrieve(url,3600/100) # delay of 36s to match 100 calls per hour restrictions
        if status == 0:
            write2tradetable(conn,data)
            print "{} <-> {} | retrieving {} entries in year {}".format(country,country_partner,amount_rows,year)

        # write status in any case to find out why retrieval of data failed
        write2statustable(conn,url,status,amount_rows,country,year,country_partner)



if args.create_db:
    create_database("UnComTrade.sqlite")

conn = sqlite3.connect("UnComTrade.sqlite")
# important - set ignore if string can't be read by database
conn.text_factory = lambda x: unicode(x, 'utf-8', 'ignore')
# LOAD Country Codes
codes = load_country_codes('CountryCodeUSFull.csv')
codes_query = load_country_codes('CountryCodeUSQuery.csv')
years = [2011, 2012, 2013, 2014, 2015]
# FIRST: check data rows for query complexity
for i,country_code in enumerate(codes_query['Country Code']):
    for year in years:
        start_time_inner_loop = time.time()
        country = codes_query['Country Name'][i]
        url = 'http://comtrade.un.org/api/get?type=C&freq=A&px=S3&ps={}&r={}&p=all&rg=all&cc=ALL'.format(year,country_code)
        sql_status,sql_data = checkfromstatustable(conn,url)
        # if sql status is zero we have the full data set in the database - skip and continue
        if sql_status==0:
            print "{} | retrieving {} entries in year {}".format(sql_data[2],sql_data[1],sql_data[3])
            continue # skip data already in database

        # data is not in the database and must be retrieved
        if sql_status<0:
            time_download_start = time.time()
            data, amount_rows, status = retrieve(url,2) # 2s delay in large queries due to download
            time_download = time.time() - time_download_start
            # no errors - data is complete
            if status == 0:
                time_write_database_start = time.time()
                write2tradetable(conn,data)
                time_write_database = time.time() - time_write_database_start
                print "{} | retrieving {} entries in year {} | Download: {} Database: {}".format(country,amount_rows,year,time_download,time_write_database)

            # Query is to complex or too big - break it up
            if status > 5001 and status < 5005:
                sub_query(conn,codes,year,country_code)
            # write status after in case there is a database error
            write2statustable(conn,url,status,amount_rows,country,year)

        # data was broken up into sub queries - check what has been retrieved
        if sql_status > 5001 and sql_status < 5005:
            sub_query(conn,codes,year,country_code)

# close clonnection before terminating programm
conn.close()
