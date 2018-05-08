##Scraping the data - Once URLs are located, we can pull the data. This script cleans the URL dataframe, retrieves data from the URL, and then writes it to file. 

import urllib.request, urllib.parse, urllib.error
import re
from bs4 import BeautifulSoup
from datetime import datetime
from threading import Timer
import pandas as pd
import numpy as np
from html.parser import HTMLParser
import sqlite3
import string; 
import time
import csv

#Cleaning data



d = {'00010':'temp',
    '00095':'conductance',
    '00300':'DO',
    '00060':'discharge',
    '00065':'gage_height',
    '00480':'salinity',
    '63160':'stream_elev',
    '32295':'DOM',
    '62361':'chl_tot',
    '32318':'chl_tot',
    '72137':'discharge_tide_filt',
    '62623':'tide_stage',
    '63680':'turbidity',
    '99133':'nitrate_nitrite',
    '99137':'nitrate_nitrite',
    '90860':'sal_psu_std_temp',
    '62620':'est_or_oce_elev',
    '62619':'est_or_oce_elev',
    '99409':'susp_sediments',
    '72279':'tide_elevation',
    '72147':'sensor_depth',
    '00400':'pH',
    '00301':'DO_ps',
    '00003':'samp_depth',
    '72019':'Water_level',
	'72167':'Matric_potential',
	'72167':'Matric_potential_12in',
	'72181':'Moisture_content_so',
	'72181':'Moisture_content_so_12',
	'72205':'Bulk_conductance_sl',
	'72205':'Bulk_conductance_sl_12',
	'72253':'Temperature_soil',
	'72253':'Temperature_soil_12',
	'74207':'Soil_moisture',
	'00035':'Wind_speed',
	'00036':'Wind_direction',
	'99232':'Time_period_vol_soil',
	'99263':'Voltage_ratio',
	'99234':'Sample_count',
	'00045':'Precipitation',
	'00020':'Air_temp',
	'00025':'Air_press',
	'00052':'Relative_humidity'}

#Set the time you'd like to start scraping - USGS prefers between 12 AM - 6 AM EST
now = datetime.today()
then = now.replace(day=now.day, hour=7, minute=16, second=0, microsecond=0)
delta_t = then-now
secs = delta_t.seconds+1

#Get to the data!
brk_list=[]
def scrape():
	print('\nStarting scraper\n')
	
	#Cleaning the data
	df = pd.read_csv('init_url.csv',converters={1: lambda x: str(x)})
	removelist='-'
	df = df.drop(['Unnamed: 0'],axis=1).replace(to_replace=r'[^\w'+removelist+']',value=' ',regex=True)
	df.columns = ['site_no','start_date','end_date','param_codes']
	df = df.set_index(['site_no']).stack().str.split(expand=True).stack().unstack(-2)
	df = df.reset_index().drop(['level_1'],axis=1)
	t0 = time.time()
	for i,j in df.iterrows():
		param = j.param_codes
		site_no = j.site_no
		sd = j.start_date
		ed = j.end_date

		url = "".join(('https://nwis.waterdata.usgs.gov/va/nwis/uv?cb_',str(param),'=on&format=rdb&site_no=',str(site_no),\
		'&period=&begin_date=',str(sd),'&end_date=',str(ed)))
		t2 = time.time()
		try:
			print('Accessing URL')
			f_name = "".join((d[str(param)],'_data_from_va_site_',str(site_no),'.txt'))
			html = urllib.request.urlopen(url,timeout=199)
			p_s = BeautifulSoup(html,'lxml')
			x = p_s.get_text()        
		except:
			print('Could not parse HTLML. Request may have timed out due to file size.')
			brk_list.append(url)
			continue

		try:
			print('\nWriting',f_name,'to file...')
			f = open(f_name,'w')
			f.write(x)
			f.close()
			print('USGS data written to',f_name,'\n')        
		except:
			print('There was a problem writing',f_name,'to file')
			continue
		if i % 10 == 3:
			print('Pausing scraper - for your health')
			time.sleep(2.5)
		t3 = time.time()
		print('Time elapsed:',(t3-t2),'s')
	t1 = time.time()
	df = pd.DataFrame(brk_list)
	df.to_csv('Broken_links.csv',sep=',')
	print('\n...This step lasted for',(t1-t0)/60/60/24,'days...\n') 
print('\n...Waiting for the right time...\n')
print('\nThis process will begin at:',then)
t = Timer(secs, scrape)
t.start()