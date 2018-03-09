from bs4 import BeautifulSoup as bs
import requests
import configparser
import psycopg2
import json
import sys
import os
from io import StringIO
from IPython import embed
import time
import xml.etree.cElementTree as ET
# from urllib.request import urlopen
import pandas as pd
millis = int(round(time.time() * 1000))
print(millis)

class db_conn:
	def __init__(self, name):
		self.name = name
		self.prefixes = ['RDS_', 'HUB_']
		self.cf = ConfigParser.ConfigParser()
		self.cf.read(path+'/db.conf')
	def resolveEnv(self,con):
		if con.startswith(tuple(self.prefixes)):
			return os.environ.get(con)
		return con
	def get_host(self):
		host = self.resolveEnv(self.cf.get(self.name, 'host'))
		return host
	def get_user(self):
		user = self.resolveEnv(self.cf.get(self.name, 'user'))
		return user
	def get_password(self):
		password = self.resolveEnv(self.cf.get(self.name, 'password'))
		return password
	def get_dbname(self):
		dbname = self.resolveEnv(self.cf.get(self.name, 'dbname'))
		return dbname
	def get_port(self):
		port = self.resolveEnv(self.cf.get(self.name, 'port'))
		return port
    	
def postgres_conn(name):
	db = db_conn(name)
	conn = psycopg2.connect("host={0} port={1} dbname={2} user={3} password={4}".format(db.get_host(), db.get_port(),db.get_dbname(),db.get_user(),db.get_password()))
	return conn

def remove_namespace(doc, namespace):
    """Remove namespace in the passed document in place."""
    ns = u'{%s}' % namespace
    nsl = len(ns)
    for elem in doc.getiterator():
        if elem.tag.startswith(ns):
            elem.tag = elem.tag[nsl:]

def getvalueofnode(node):
    """ return node text or None """
    return node.text if node is not None else None

def upload_data_to_db(df_xml,table_name):
	#================================================
	# Main function to copy data from dataframe to db
	#================================================
	cur = conn.cursor()
	s = StringIO()
	df_xml.to_csv(s, header=False, index=True, sep='|')
	s.seek(0)
	s_new = StringIO(s.read()[0:-1])
	s_new.seek(0)
	columns = list(df_xml.columns)
	columns.insert(0, "record_id")

	debug_cik_no = df_xml['cik'][0]
	debug_period_no = df_xml['period_date'][0]

	try:
		cur.copy_from(s_new, table_name, columns=columns, sep='|', null='')
		conn.commit()
		print ("COPY {0} succeeded for cik:{1} - period:{2}.".format(table_name,debug_cik_no,debug_period_no))
	except Exception as e:
		print ("COPY {0} failed for cik:{1} - period:{2}.".format(table_name,debug_cik_no,debug_period_no))
		print ('Error msg: {}'.format(str(e)))
		conn.rollback()
	cur.close()

def get_df_from_xml(data_url):
	#==========================
	# parsing xml to dataframe
	#==========================
	dfcols = ['nameOfIssuer', 'titleOfClass', 'cusip', 'value', 'investmentDiscretion', 'otherManager', 'sshPrnamt', 'sshPrnamtType', 'putCall', 'sole', 'shared', 'voting_none']
	df_xml = pd.DataFrame(columns=dfcols)
	resp = requests.get(data_url)
	with open('temp_file.xml', 'wb') as foutput:
		foutput.write(resp.content)
	tree = ET.ElementTree(file='temp_file.xml')
	root = tree.getroot()

	remove_namespace(root, u'http://www.sec.gov/edgar/document/thirteenf/informationtable')
					
	for node in root:
		nameOfIssuer = node.find('nameOfIssuer')
		titleOfClass = node.find('titleOfClass')
		cusip = node.find('cusip')
		value = node.find('value')
		investmentDiscretion = node.find('investmentDiscretion')
		otherManager = node.find('otherManager')
		putCall = node.find('putCall')
		
		for child in node:
			if 'shrsOrPrnAmt' in child.tag:
				sshPrnamt = child.find('sshPrnamt')
				sshPrnamtType = child.find('sshPrnamtType')
			if 'votingAuthority' in child.tag:
				sole = child.find('Sole')
				shared = child.find('Shared')
				voting_none = child.find('None')
			
		if getvalueofnode(otherManager):
			otherManager = getvalueofnode(otherManager).rstrip()
		else:
			otherManager = getvalueofnode(otherManager)
		df_xml = df_xml.append(pd.Series([getvalueofnode(nameOfIssuer), getvalueofnode(titleOfClass), getvalueofnode(cusip),getvalueofnode(value),getvalueofnode(investmentDiscretion),otherManager,getvalueofnode(sshPrnamt),getvalueofnode(sshPrnamtType),getvalueofnode(putCall),getvalueofnode(sole),getvalueofnode(shared),getvalueofnode(voting_none)], index=dfcols),ignore_index=True)
	return df_xml

def get_info_urls(cik):

	dataRequest = bs(requests.get(request_host + base_form).content, 'lxml')
	table = dataRequest.find("table",{"class":"tableFile2"})

	for td in table.find_all('tr')[1:]:
		tds = td.find_all('td')
		if tds[0].text == '13F-HR' and tds[1].find('a',href=True)['href'].startswith( '/Archives' ):
			q_report = tds[1].find('a',href=True)['href']
			q_Request = bs(requests.get(request_host + q_report).content, 'lxml')
			#================================
			# Get filing date and period date
			#================================
			q_info = q_Request.find_all("div",{"class":"formGrouping"})
			info_filing_date = str(q_info[0].find_all('div')[1].text)
			info_period_date = str(q_info[1].find_all('div')[1].text)

			#===============================
			# Get xml url
			#===============================
			q_table = q_Request.find("table",{"class":"tableFile"})
			doc_list = []
			xml_url =''
			for n in q_table.find_all('td'):
				doc_list.append(n.text)
			xml_index = [index for index, v in enumerate(doc_list) if v == 'INFORMATION TABLE']
			for index in xml_index:
				index_url = index-1
				xml_index = q_table.find_all('td')[index_url].find('a',href=True)
				if  xml_index is not None and '.xml' in xml_index.text:
					xml_url = xml_index['href']
					break
			#======================================
			# parsing xml and add another 3 columns
			#======================================
			if xml_url:
				data_url = request_host + xml_url
				# data_url = request_host + '/Archives/edgar/data/928400/000090044016000185/Elkhorn13FChart1.xmldata.xml'
				try:
					df_xml = get_df_from_xml(data_url)
				except Exception as e:
					print ('ERROR: {0} - filling:{1}'.format(cik,info_filing_date))
					print ('Error msg: {}'.format(str(e)))
				df_xml['cik']=cik
				df_xml['filing_date']=info_filing_date
				df_xml['period_date']=info_period_date
				upload_data_to_db(df_xml,'information_table')
			else:
				print('no xml url found - {0} - period:{1}'.format(cik,info_period_date))
				break

def db_table_create():
	cur = conn.cursor()
	create_query = """CREATE TABLE IF NOT EXISTS information_table(
	record_id            INT     NOT NULL,
	nameOfIssuer         VARCHAR NOT NULL,
	titleOfClass         VARCHAR NOT NULL,
	cusip                VARCHAR NOT NULL,
	value                BIGINT,
	investmentDiscretion VARCHAR,
	otherManager         VARCHAR,
	sshPrnamt            VARCHAR,
	sshPrnamtType        VARCHAR,
	putCall              VARCHAR,
	sole                 VARCHAR,
	shared               VARCHAR,
	voting_none          VARCHAR,
	cik                  INT     NOT NULL,
	filing_date          DATE,
	period_date          DATE    NOT NULL,
	CONSTRAINT information_table_pkey
	PRIMARY KEY (record_id, cusip, cik, period_date))"""
	cur.execute(create_query)
	cur.close()
						
def main():
	global request_host,base_form,path,conn
	conn = psycopg2.connect("dbname='doris_postgre' user='doris' host='localhost' password='' port='5432'")
	db_table_create()
	abspath = os.path.abspath(sys.argv[0])
	path =  os.path.abspath(os.path.join(abspath ,'..'))

	request_host = 'https://www.sec.gov'

	hedgefund_list_path = path + '/hedgeFundList.csv'
	hedgefund_list_df = pd.read_csv(hedgefund_list_path)
	
	for i,row in hedgefund_list_df.iterrows():
		cik = row['CIK']
		base_form = '/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=13F-HR&dateb=&owner=include&count=100'.format(cik)
		get_info_urls(cik)

	# cik = '928400'
	# base_form = '/cgi-bin/browse-edgar?action=getcompany&CIK={0}&type=13F-HR&dateb=&owner=include&count=100'.format(cik)
	# get_info_urls(cik)
	# conn = postgres_conn('xin')



	print('Finish!')

	conn.close() 

if __name__ == "__main__":

	main()


