## Combining all of our data to loop through USGS pages
import urllib.request, urllib.parse, urllib.error
import re
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import time
# Build list of site numbers from state of your choosing. 

state = input('Enter two-letter acronym for state of interest:')
state = state.lower()

url = "".join(('https://waterdata.usgs.gov/',str(state),'/nwis/current/?index_pmcode_ALL=ALL&index_pmcode_STATION\
_NM=1&index_pmcode_DATETIME=2&format=rdb_station_file&group_key=NONE&sort_key_2=site_no&html\
_table_group_key=NONE&rdb_compression=file&list_of_search_criteria=realtime_parameter_selection'))

site_no = [] 
try:
    html = urllib.request.urlopen(url).read()
    p_s = BeautifulSoup(html,'lxml')
    x = p_s.get_text()
    x = x.split()
    for i in x:
    	site_no.append(re.findall('\d{4,}',i))
except:
    print('Broken')
site_no = [i for i in site_no if i]

#Combine list of parameters and start times by looping through URLs of different sites

t0 = time.time()
url_data = []
for q in site_no:
	q = str(q)
	q = re.sub('\W','',q)
	url = "".join(('https://waterdata.usgs.gov/',str(state),'/nwis/uv/?site_no=',str(q),'&agency_cd=USGS&amp;'))
	print('\nCollecting data from:\n',url,'\n')
	try:
		html = urllib.request.urlopen(url).read()
		p_s = BeautifulSoup(html,'lxml')
		text = p_s.get_text()
	except:
		print('broken')
		continue

	#Strip all whitespace and new lines from string
	text = "".join([s for s in text.strip().splitlines(True) if s.strip()])

	#Create block of text where data is  
	ntext = text[text.index('Available Parameters for this site'):text.index('Output format')]
	ntext = ntext.split()

	#Use regex to pull dates and parameter codes
	dates = []
	param_list = []
	for i in ntext:
		param_list.append(re.findall('\d{5}',i))
		dates.append(re.findall('\d{2,4}[-]\d{2,4}[-]\d{2,4}',i))

	#Strip empty elements from list
	param_list = [i for i in param_list if i]
	dates = [i for i in dates if i]

	#Create list of starting and ending dates for time-series 
	sd = []
	ed = []
	for count, i in enumerate(dates):
		if count % 2 != 1:
			sd.append(i)
		elif count % 2 == 1:
			ed.append(i)
	
	url_data.append([q,sd,ed,param_list])
	if int(q) % 10 == 0:
		print('Pausing for your health\n')
		time.sleep(0.5)

t1 = time.time()

print('\n...This step lasted for',(t1-t0)/60/60/24,'days...\n') 
df = pd.DataFrame(url_data)
fname = 'init_url.csv'
df.to_csv(fname,sep=',')

