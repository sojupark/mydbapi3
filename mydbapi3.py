#-*- encoding:euckr -*-
import os,sys           
import datetime 
import socket
import types 
import decimal
import warnings
import subprocess
import threading
try:            
	import MySQLdb as mysqldb
except:
	try:
		import pymysql as mysqldb
	except:
		pass

load_ifx=False
try:
	import IfxPyDbi
	myout = str(subprocess.check_output(["esql", "-V"]))
	ifxver = float(myout[myout.find("CSDK Version")+len("CSDK Version"):myout.find(",")])
	if ifxver >= 4.1: # support IfxPy
		load_ifx=True
	load_ifx=False
	
except:                
	pass

               
try:                    
	import pyodbc
except:                
	try:
		import pypyodbc as pyodbc
	except:
		pass


try:
	import psycopg2 as pgsql
except:
	pass

try:
	import pymongo
except:
	pass

try: 
	import sqlite3 as sqlite
except:
	try:
		import sqlite
	except:
		pass



def myPrint(myDecoding, *mystrlist):
	if len(mystrlist) == 0:
		mystrlist = [myDecoding]
		myDecoding = 'euckr'

	for mystr in mystrlist:
		if isinstance(mystr, pyodbc.Row) or isinstance(mystr, list) or isinstance(mystr, tuple):
			print(','.join(str(x).encode(myDecoding, "replace").decode(myDecoding, 'replace') for x in mystr), end= ' ', flush=True)
		elif isinstance(mystr, dict):
			#print(','.join(str(x).encode(self.myDecoding, "replace").decode(self.myDecoding, 'replace') for x in mystr), end= ' ')
			print(str(mystr).encode(myDecoding, "replace").decode(myDecoding, 'replace'), end= ' ', flush=True)
		else:
			print(str(mystr).encode(myDecoding, "replace").decode(myDecoding, 'replace'), end= ' ', flush=True)
	print('')



def _exeQryImpl(mycur, myinfo, myType, tabname, pdic_col, mykeylist, IS_DEBUG, IS_PRINT, useColFilter, useDictLow):
	db_deli = myinfo['db_deli']
	tab_info = myinfo['tab_info']
	tab_key = myinfo['tab_key']
	myEncoding = myinfo['myEncoding']
	qryEscapeStr = myinfo['qryEscapeStr']
	re_quote = myinfo['quote']
	sql = ""


	dic_col = {}
	rdic_col = {}
	if useDictLow:
		for tmpk, tmpv in pdic_col.items():
			rdic_col[tmpk.lower()] = tmpv
	else:
		rdic_col = pdic_col


	if myType == 'U' and len(rdic_col) <= 1:
		print("abnormal--------------->")
		myPrint(myinfo['myDecoding'], tabname,rdic_col)
		return("abnormal")

	if IS_DEBUG or IS_PRINT:
		try:
			myPrint(myinfo['myDecoding'], tabname, rdic_col)
		except:
			myPrint(sys.exc_info())

	realtabname = tabname
	if myType == 'I' or myType == 'R' or myType == 'U' or myType == 'IO':
		if realtabname.find(db_deli) != -1:
			tabname = realtabname.split(db_deli)[1]


	colListStr = ""
	valListStr = ""
	keyListStr = ""
        # set insert column
	for mykey in rdic_col.keys():
		# select processing column
		if useColFilter:
			if mykey not in tab_info[tabname]:
				continue

		# check if data attribute is not string type
		if rdic_col[mykey] is None:
			dic_col[mykey] = rdic_col[mykey]
		else:
			mytype = str(type(rdic_col[mykey])).lower()
			if mytype.find("decimal") != -1:
				rdic_col[mykey] = decimal.Decimal(rdic_col[mykey])
			elif mytype.find("float") != -1:
				rdic_col[mykey] = float(rdic_col[mykey])
			elif mytype.find("int") != -1:
				rdic_col[mykey] = int(rdic_col[mykey])
			else:
				rdic_col[mykey] = str(rdic_col[mykey]).replace("\x00",'')

			dic_col[mykey] = str(rdic_col[mykey]).encode(myEncoding, 'replace').decode(myEncoding, 'replace')



	if myType == 'I' or myType == "IO" or myType == 'U':
		#try:
		for mykey in dic_col.keys():
			if dic_col[mykey] is None:
				valListStr += 'NULL,'
			else:
				if tab_info[tabname][mykey] == "str" or tab_info[tabname][mykey].find("date") != -1:
					valListStr += qryEscapeStr+"'" + dic_col[mykey].replace("'",re_quote) + "',"
				else:
					valListStr += dic_col[mykey] + ","
			colListStr += mykey + ","

		colListStr = colListStr[:-1]
		valListStr = valListStr[:-1]

		sql = """insert into %s(%s) values(%s) """ % (realtabname, colListStr, valListStr)


		i_sql = sql
		try:
			if myType == 'U':
				raise

			if IS_DEBUG or IS_PRINT:
				myPrint(sql)
			
			if not IS_DEBUG:
				mycur.execute(sql)
		except:
			if myType == "IO":
				raise
			else:
				valListStr = ""
				keyListStr = ""
				for mykey in dic_col.keys():
					if dic_col[mykey] is None:
						tmpStr = '=NULL'
					else:
						if tab_info[tabname][mykey] == "str":
							tmpStr = "="+qryEscapeStr+"'" + dic_col[mykey].replace("'",re_quote) + "'"
						elif tab_info[tabname][mykey].find("date") != -1:
							if dic_col[mykey].strip() == "":
								tmpStr = "=NULL"
							else:
								tmpStr = "="+qryEscapeStr+"'"+ dic_col[mykey].replace("'",re_quote) + "'"
						else:
							tmpStr = "=" + dic_col[mykey]
					
					if (tabname in tab_key and mykey in tab_key[tabname]) or mykey in mykeylist:
						keyListStr += mykey + tmpStr.replace("=NULL", " is NULL") + " and "
					else:
						valListStr += mykey + tmpStr + ","

				valListStr = valListStr[:-1].strip()
				keyListStr = keyListStr[:-4].strip()
	
				sql = """update %s set %s where %s """ % (realtabname, valListStr, keyListStr)

				if keyListStr == "":
					myPrint("error----> key does not exists!!")
					myPrint(sql)
					raise
				try:
					if IS_DEBUG or IS_PRINT:
						myPrint(sql)

					if not IS_DEBUG:
						mycur.execute(sql)
				except:
					myPrint(sql)
					raise
	elif myType == 'R':
		for mykey in dic_col.keys():
			if dic_col[mykey] is None:
				valListStr += 'NULL,'
			else:
				if tab_info[tabname][mykey] == "str" or tab_info[tabname][mykey].find("date") != -1:
					valListStr += qryEscapeStr+"'" + dic_col[mykey].replace("'",re_quote) + "',"
				else:
					valListStr += dic_col[mykey] + ","
			colListStr += mykey + ","

		colListStr = colListStr[:-1]
		valListStr = valListStr[:-1]

		sql = """replace into %s(%s) values(%s) """ % (realtabname, colListStr, valListStr)

		if IS_DEBUG or IS_PRINT:
			myPrint(sql)
		
		if not IS_DEBUG:
			mycur.execute(sql)


	elif myType == 'D':
		for mykey in dic_col.keys():
			if dic_col[mykey] is None:
				 tmpStr = " is NULL"
			else:
				if tab_info[tabname][mykey] == "str" or tab_info[tabname][mykey].find("date") != -1:
					tmpStr = "="+qryEscapeStr+"'" + dic_col[mykey].replace("'",re_quote) + "'"
				else:
					tmpStr = "=" + dic_col[mykey]
			keyListStr += mykey + tmpStr + " and "

		keyListStr = keyListStr[:-4].strip()
		sql = "delete from %s where %s " % (realtabname, keyListStr)
		if keyListStr == "":
			myPrint("error----> key does not exists!!")
			myPrint(sql)
			raise
		try:
			if IS_DEBUG or IS_PRINT:
				myPrint(sql)
			if not IS_DEBUG:
				mycur.execute(sql)
		except:
			myPrint(sql)
			raise
	return sql

def _execute(mycur, sql):
	try:
		mycur.execute(sql)
	except:
		raise

def _fetchone(mycur):
	ritems = None
	try:
		ritems = mycur.fetchone()
	except:
		raise

	return ritems
	
def _fetchall(mycur):
	ritems = None
	try:
		ritems = mycur.fetchall()
	except:
		raise

	return ritems

def _fetchmany(mycur,funm):
	ritems = None
	try:
		while True:
			ritems = mycur.fetchmany(fnum)
			if not ritems:
				break
			for mydata in ritems:
				yield mydata
	except:
		raise


def _executemany(mycur, sql, datalist):
	try:
		mycur.executemany(sql, datalist)
	except:
		raise


def _getCols(mycur, useDictLow=False):
	reval = []
	if useDictLow:	
		reval = [ x[0].lower() for x in mycur.description ]
	else:
		reval = [ x[0] for x in mycur.description ]
	return reval

def _goSelectGen(mycur,fnum, useDict, useDictLow):
	if useDict or useDictLow:
		myh = []
		while True:
			ritems = mycur.fetchmany(fnum)
			if not ritems:
				break
			if len(myh) == 0: #get first generation
				myh=_getCols(mycur, useDictLow)	

			for mydata in ritems:
				yield dict(zip(myh,mydata))	
	else:
		while True:
			ritems = mycur.fetchmany(fnum)
			if not ritems:
				break
			for mydata in ritems:
				yield mydata
				

def _exeQry(mycur, myinfo, myType, tabnm_or_sql, dataDic={}, force_mykeylist=[], IS_DEBUG=False, IS_PRINT=False, useColFilter=False, useDict=False, useDictLow=False):
	ritems = None
	myType = myType.upper()
	if myType[0] == 'G' or myType[0] == 'S':
		sql = tabnm_or_sql
		if IS_DEBUG or IS_PRINT:
			myPrint(sql)
		try:
			if not IS_DEBUG:
				mycur.execute(sql)
		except:
			myPrint(sys.exc_info())
			raise

		if myType[0] == 'S' or sql.lower().find("select") != -1 or sql.lower().find("execute")!= -1 or sql.lower().find("pragma")!= -1:
			try:
				if myType[1:] =='':
					ritems = mycur.fetchall()
					if ritems is not None and len(ritems) > 0 and (useDict or useDictLow):
						myh=_getCols(mycur, useDictLow)
						ritems_ = []
						for tmp in ritems:
							ritems_.append(dict(zip(myh,tmp)))
						ritems = ritems_
				else:
					try:
						fnum = int(myType[1:])
					except:
						print("G type Must follow a Number!!")
						raise

					if fnum == 1:
						ritems = mycur.fetchone()
						
						if ritems is not None  and len(ritems) > 0 and (useDict or useDictLow):
							myh=_getCols(mycur, useDictLow)
							ritems = dict(zip(myh,ritems))
					else:
						return _goSelectGen(mycur, fnum, useDict, useDictLow)
			except:
				myPrint(sys.exc_info())
				#raise
						

	elif myType == 'I' or myType == 'U' or myType == 'IO' or myType =='D' or myType == 'R':
		tabnm = tabnm_or_sql

		try:
			reVal = _exeQryImpl(mycur, myinfo, myType, tabnm, dataDic, force_mykeylist, IS_DEBUG, IS_PRINT, useColFilter, useDictLow)
			ritems = [reVal]
		except:
			raise

	elif myType == 'M' or myType == 'IM':
		sql = tabnm_or_sql

		if IS_DEBUG or IS_PRINT:
			myPrint(sql)
		try:
			if not IS_DEBUG:
				mycur.executemany(sql, dataDic)
			ritems = [sql]
		except:
			myPrint(sys.exc_info())
			raise


	return ritems
##############################################
#
# cursor class
#
class MydbCur(object):
	def __init__(self, _mydbinfo):
		self.__mydbinfo = _mydbinfo
		self.mycur = self.__mydbinfo['mycon'].cursor()

	def __del__(self):
		self.closeCur()

	def __enter__(self):
		return self	

	def closeCur(self):
		try:
			self.mycur.close()
		except:
			pass # Already closed

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.closeCur()

	def close(self):
		self.closeCur()


	def getMyDbInfo(self, mykey):
		return self.__mydbinfo[mykey]

	def execute(self, sql):
		_execute(self.mycur, sql)
	
	def fetchone(self):
		return _fetchone(self.mycur)

	def fetchall(self):
		return _fetchall(self.mycur)

	def fetchmany(self,funm):
		yield _fetchmany(self.mycur,funm)

	def executemany(self, sql, datalist):
		_executemany(self.mycur, sql, datalist)

	def getCols(self):
		return  _getCols(self.mycur)

	def exeQry(self, myType, tabnm_or_sql, dataDic={}, force_mykeylist=[], IS_DEBUG=False, IS_PRINT=False, useColFilter=False, useDict=False, useDictLow=False):
		return _exeQry(self.mycur, self.__mydbinfo, myType, tabnm_or_sql, dataDic, force_mykeylist, IS_DEBUG, IS_PRINT, useColFilter, useDict, useDictLow)


###################################################
#
# db connection, information class
# and internal basic cursor 
#
class Mydb(object):
	def __init__(self, _db_type="mysql", _dbnm="idcb", _myHost='localhost', _mySock="", _myPort="", _myUser="infomax", _charset="euckr", _myEncoding="euckr",  _useWarning=False, _myDecoding="euckr", _myAutocommit=True, _isSensitive=True):
		if _useWarning:
			warnings.filterwarnings(action='default')
		else:
			warnings.filterwarnings(action='ignore')

		self.__mydbinfo = {}
		import socket

		self.__mydbinfo['mydefdb_type'] = _db_type
		if self.__mydbinfo['mydefdb_type'] == "pgsql":
			self.__mydbinfo['qryEscapeStr'] = "E"
		else:
			self.__mydbinfo['qryEscapeStr'] = ""
		if _db_type == "ifx":
			self.__mydbinfo['quote'] = "''"
			self.__mydbinfo['db_deli'] = ":"
		else:
			self.__mydbinfo['quote'] = "\\'"
			self.__mydbinfo['db_deli'] = "."

		if _charset.lower() != "euckr":
			_myEncoding = _charset.lower()
			

		self.__mydbinfo['mydefdbnm'] = _dbnm
		self.__mydbinfo['myschema'] = _dbnm
		self.__mydbinfo['myEncoding'] = _myEncoding
		self.__mydbinfo['myDecoding'] = _myDecoding
		self.__mydbinfo['myAutocommit']= _myAutocommit
		self.__mydbinfo['mycon'] = None
		self.mycur = None
		self.__mydbinfo['hostname'] = socket.gethostname()
		if self.getConDb(_db_type, _dbnm, _myHost, _mySock, _myPort, _myUser, _myEncoding, _myDecoding, _myAutocommit) == -1:
			raise

		self.setColInfo(_isSensitive)


	def __del__(self):
		self.closeDb()
			#with
	def __enter__(self):
		if self.__mydbinfo['mydefdb_type'] in ['mongo']: #nosql

			return self.__mydbinfo['mycon']
		else: # rdbms
			return self	

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.closeDb()

	def __getattr__(self, mykey):
		return self.get(mykey)

	def cursor(self, myName=""):
		return MydbCur(self.__mydbinfo)
		
	def closeDb(self):
		try:
			self.__mydbinfo['mycon'].close()
		except:
			pass # already closed
	def close(self):
		self.closeDb()

	def execute(self, sql):
		_execute(self.mycur, sql)
	
	def fetchone(self):
		return _fetchone(self.mycur)

	def fetchall(self):
		return _fetchall(self.mycur)

	def fetchmany(self,funm):
		yield _fetchmany(self.mycur,funm)

	def executemany(self, sql, datalist):
		_executemany(self.mycur, sql, datalist)

	def getCols(self):
		return  _getCols(self.mycur)


	def getConDb(self, db_type, dbnm, myHost, mySock, myPort, myUser, myEncoding, myDecoding, myAutocommit):
		self.__mydbinfo['mydefdb_type'] = db_type
		self.__mydbinfo['mydefdbnm'] = dbnm

		if db_type == "mysql":
			if myUser == "infomax":
				myPasswd = "infomax!"
			elif myUser == 'myfeed':
				myPasswd = "infomax!"

			if myPort == "":
				if myHost.lower() == 'm25' or myHost.lower() == 'm26':
					myPort = 3508
				else:
					try:
						if "MYSQL_PORT" in os.environ and os.environ["MYSQL_PORT"].strip() !="":
							myPort = int(os.environ["MYSQL_PORT"])
						else:
						 	myPort = 3306
					except:
						myPort =3306


			if myHost == 'localhost':
				if mySock == "":
					try:
						if "IDCB_SOCK" in os.environ and os.environ["IDCB_SOCK"].strip() !="":
							mySock = os.environ["IDCB_SOCK"]
						elif "MYSQL_SOCK" in os.environ and os.environ["MYSQL_SOCK"].strip() != "":
							mySock = os.environ["MYSQL_SOCK"]
						else:
							for tarSock in ["/var/lib/mysql/mysql.sock", "/tmp/mysql.sock"]:
								if os.path.exists(tarSock):
									mySock = tarSock
									break
					except:
						pass
			else:
				mySock = ""

			if mySock == "":
				self.__mydbinfo['mycon'] = mysqldb.connect(host=myHost, user=myUser, passwd=myPasswd, db=dbnm, port=myPort, charset=myEncoding)
			else:
				self.__mydbinfo['mycon'] = mysqldb.connect(host=myHost, user=myUser, passwd=myPasswd, db=dbnm, unix_socket=mySock, port=myPort, charset=myEncoding)

			self.__mydbinfo['mycon'].autocommit(myAutocommit)

		elif db_type == "ifx":
			if myUser == "infomax":
				if myHost == "ftp1" or myHost == 'ftp2':
					myPasswd = 'infomax!@'
				else:
					myPasswd = "infomax!"

			if dbnm.find("@") != -1:
				self.__mydbinfo['mydefdbnm'] = dbnm.split("@")[0]
				self.__mydbinfo['hostname'] = dbnm.split("@")[1]
				dbnm = self.__mydbinfo['mydefdbnm'] 
				myHost = self.__mydbinfo['hostname'] 
				
				
			if myHost == "localhost" or myHost == "127.0.0.1":
				myHost=self.__mydbinfo['hostname'].lower()

			global load_ifx

			if load_ifx:
				# https://www.ibm.com/docs/en/informix-servers/12.10?topic=conversion-unicode-database-code-set
				ifx_code_set = {'euckr':'ko_kr.KS5601', 'utf8':'ko_kr.utf8'}
				if myEncoding == "euckr": # check default 
					try:
						_myEncoding = ifx_code_set[os.environ['LANG'].replace("-","").split(".")[1].lower()]
					except:
						pass
				if myDecoding == 'euckr':
					try:
						_myDecoding = ifx_code_set[myDecoding.lower()]
					except:
						pass
				
				con_str="SERVER="+myHost+";DATABASE="+dbnm+";HOST="+myHost+";UID="+myUser+";PWD="+myPasswd+";DB_LOCALE="+_myDecoding+";CLIENT_LOCALE="+_myEncoding
				try:
					self.__mydbinfo['mycon'] = IfxPyDbi.connect(con_str, "", "")
				except:
					load_ifx=False #pyodbc

					
			if not load_ifx:
				con_str="DRIVER={ifx};UID="+myUser+";PWD="+myPasswd+";SERVER="+myHost+";DATABASE="+dbnm+";"
				self.__mydbinfo['mycon'] = pyodbc.connect(con_str)

				self.__mydbinfo['mycon'].setdecoding(pyodbc.SQL_WCHAR, encoding=myEncoding)
				self.__mydbinfo['mycon'].setdecoding(pyodbc.SQL_CHAR, encoding=myEncoding)

				self.__mydbinfo['mycon'].setdecoding(pyodbc.SQL_WMETADATA, encoding='UTF-32LE')
				self.__mydbinfo['mycon'].setencoding(encoding=myEncoding)

			self.__mydbinfo['mycon'].autocommit = myAutocommit
				

		elif db_type == "alti":
			if myPort == "":
				myPort = 20300
			if myUser == "infomax":
				myPasswd = "infomax!"

			if myEncoding.lower().replace("-","") == 'euckr':
				myEncoding="cp949"

			con_str='DSN=alti;UID='+myUser+';PWD='+myPasswd+';SERVER='+myHost+';PORT='+str(myPort)+';DATABASE='+dbnm+";NLS_USE="+myEncoding
			self.__mydbinfo['mycon'] = pyodbc.connect(con_str)


			self.__mydbinfo['mycon'].setdecoding(pyodbc.SQL_WCHAR, encoding=myEncoding)
			#self.__mydbinfo['mycon'].setdecoding(pyodbc.SQL_CHAR, encoding=myEncoding)
			#self.__mydbinfo['mycon'].setdecoding(pyodbc.SQL_WMETADATA, encoding=myEncoding)
			self.__mydbinfo['mycon'].setencoding(encoding=myEncoding)
				

			self.__mydbinfo['mycon'].autocommit = myAutocommit

		
		elif db_type == "pgsql":
			if myPort == "":
				myPort = "5432"
			if myUser == 'infomax':
				myPasswd = 'infomax!'

			tmpstr = self.__mydbinfo['mydefdbnm']
			
			schema = 'public' #default
			if tmpstr.find(".") != -1: # set schema
				dbnm = tmpstr.split(".")[0]
				schema = tmpstr.split(".")[1]
			self.__mydbinfo['mydefdbnm'] = dbnm
			self.__mydbinfo['myschema'] = schema
			self.__mydbinfo['mycon'] = pgsql.connect(user=myUser, password=myPasswd, host=myHost, port=myPort, database=dbnm, options='-c search_path={schema}'.format(schema=schema))
			self.__mydbinfo['mycon'].set_session(autocommit=myAutocommit)
			self.__mydbinfo['mycon'].set_client_encoding(myEncoding)


			

		elif db_type == 'mongo':
			if myHost.find(":") !=-1:
				tmp = myHost.split(":")
				myHost = tmp[0]
				myPort = int(tmp[1])

			if myUser == 'infomax':
				ckHostList = ['feed1','feed2','m25','m26']
				if myHost in ckHostList or socket.gethostbyaddr(myHost)[0] in ckHostList:
					myPasswd = 'infomax!'
			self.__mydbinfo['mycon'] = pymongo.MongoClient(myHost, port=myPort, authSource=dbnm, username=myUser, password=myPasswd)

		elif db_type == 'sqlite':
			self.__mydbinfo['mycon'] = sqlite.connect(dbnm, isolation_level=None)
			def adapt_decimal(d):
				return str(d)
			def convert_decimal(s):
				return decimal.Decimal(s)

			sqlite.register_adapter(decimal.Decimal, adapt_decimal)
			sqlite.register_converter("decimal", convert_decimal)

		else:
			print("------> do not surport ", db_type)
			print("surport db list ---> ifx, alti, mysql, mongo, sqlite, pgsql")
			return -1	


		if db_type not in ['mongo']:
			self.mycur = self.__mydbinfo['mycon'].cursor()
		return



	def getColAttr(self, db_type, dbtpid):
		ifx_dic={
	0:'CHAR',
	1:'SMALLINT',
	2:'INTEGER',
	3:'FLOAT',
	4:'SMALLFLOAT',
	5:'DECIMAL',
	6:'SERIAL 1',
	7:'DATE',
	8:'MONEY',
	9:'NULL',
	10:'DATETIME',
	11:'BYTE',
	12:'TEXT',
	13:'VARCHAR',
	14:'INTERVAL',
	15:'NCHAR',
	16:'NVARCHAR',
	17:'INT8',
	18:'SERIAL8 1',
	19:'SET',
	20:'MULTISET',
	21:'LIST',
	22:'ROW (unnamed)',
	23:'COLLECTION',
	40:'Variable-length opaque type 2',
	41:'Fixed-length opaque type 2',
	43:'LVARCHAR (client-side only)',
	45:'BOOLEAN',
	52:'BIGINT',
	53:'BIGSERIAL 1',
	258:'INT',
	256: 'CHAR',
	261: 'DECIMAL',
	269: 'CHAR',
	308:'BIGINT',
	2061:'IDSSECURITYLABEL 2',
	4118:'ROW (named)'}
				
		alti_dic={
	1:"CHAR",
	12:"VARCHAR",
	-8:"NCHAR",
	-9:"NVARCHAR",
	2:"NUMERIC",
	2:"DECIMAL",
	6:"FLOAT",
	6:"NUMBER",
	8:"DOUBLE",
	7:"REAL",
	-5:"BIGINT",
	4:"INTEGER",
	5:"SMALLINT",
	9:"DATE",
	30:"BLOB",
	40:"CLOB",
	20001:"BYTE",
	20002:"NIBBLE",
	-7:"BIT",
	-100:"VARBIT",
	10003:"GEOMETRY"}
	
		try:
			if db_type == "ifx":
				dbtpid = ifx_dic[dbtpid]
			elif db_type == "alti":
				dbtpid = alti_dic[dbtpid]
			else:
				pass
		except:
			dbtpid = ""
	
		return dbtpid



	def setColInfo(self, isSensitive=True):
		db_type = self.__mydbinfo['mydefdb_type']
		myschema = self.__mydbinfo['mydefdbnm']
		mycur = self.mycur

		tab_info = {}
		tab_key = {}
		tab_col = {}
		tab_list = []

		tab_info_low = {}
		tab_col_low = {}
		tab_key_low = {}
		tab_list_low = []

		items = []
		# get schema
		if db_type == 'sqlite':
			cur = mycur
			# unique index
			for item in cur.execute("select a.tbl_name, a.sql from sqlite_master a where type = 'table'").fetchall():
				tabnm = item[0]
				tabinfo = item[1]
				keylist = {}
				for item2 in cur.execute("select sql  from sqlite_master where type = 'index' and tbl_name = '{tabnm}'".format(tabnm=tabnm)).fetchall():
					if item2[0] is not None and item2[0].lower().find("unique index") != -1:
						keylist.update({x.strip():1 for x in item2[0].split('{tabnm}('.format(tabnm=tabnm))[1].strip("()").split(",")})

				## column list in a table 
				colinfo = tabinfo.split(tabnm)[1].strip(" (),").split("primary key")
				if len(colinfo) > 1: # primary key
					keylist.update({x.strip():1 for x in colinfo[1].strip(" (),").split(",")})
				
				for __items3 in cur.execute("pragma table_info('{tabnm}')".format(tabnm=tabnm)).fetchall():
					c = __items3[1].strip()
					t = __items3[2].strip()
					p = 'PRI' if __items3[5] == 1 else ''
					if c in keylist:
						p = 'PRI'
					items.append([tabnm, p, c, t, 0])

		elif db_type == 'pgsql':
			mycatalog = self.__mydbinfo['mydefdbnm']
			myschema = self.__mydbinfo['myschema']

			sql = """ select a.table_name, case when b.column_name is null then '' else 'PRI' end, a.column_name, a.data_type, case when a.numeric_scale is null then 0 else a.numeric_scale end
         from information_schema.columns a 
         left outer join information_schema.key_column_usage b on b.table_catalog = a.table_catalog and b.table_schema = a.table_schema and b.table_name = a.table_name and b.column_name = a.column_name
         where a.table_catalog = '{mycatalog}' and a.table_schema='{myschema}' order by a.table_catalog, a.ordinal_position""".format(mycatalog=mycatalog, myschema=myschema)

		elif db_type == "mysql":
			sql = """select c.TABLE_NAME
, case when kcu.COLUMN_NAME is null then '' else 'PRI' end
, c.COLUMN_NAME, c.DATA_TYPE, ifnull(c.NUMERIC_SCALE,0)
from information_schema.COLUMNS c
left outer join information_schema.KEY_COLUMN_USAGE kcu on kcu.TABLE_SCHEMA = c.TABLE_SCHEMA and kcu.TABLE_NAME = c.TABLE_NAME and kcu.COLUMN_NAME = c.COLUMN_NAME
where c.TABLE_SCHEMA = '%s' 
order by c.TABLE_NAME, c.ordinal_position
			""" % (myschema)
		elif db_type == "ifx":
			sql = """
select st.tabname, 
NVL((select case 
when abs(sidxs.part1) = abs(sc.colno) then 'PRI'
when abs(sidxs.part2) = abs(sc.colno) then 'PRI'
when abs(sidxs.part3) = abs(sc.colno) then 'PRI'
when abs(sidxs.part4) = abs(sc.colno) then 'PRI'
when abs(sidxs.part5) = abs(sc.colno) then 'PRI'
when abs(sidxs.part6) = abs(sc.colno) then 'PRI'
when abs(sidxs.part7) = abs(sc.colno) then 'PRI'
when abs(sidxs.part8) = abs(sc.colno) then 'PRI'
when abs(sidxs.part9) = abs(sc.colno) then 'PRI'
when abs(sidxs.part10) = abs(sc.colno) then 'PRI'
when abs(sidxs.part11) = abs(sc.colno) then 'PRI'
when abs(sidxs.part12) = abs(sc.colno) then 'PRI'
when abs(sidxs.part13) = abs(sc.colno) then 'PRI'
when abs(sidxs.part14) = abs(sc.colno) then 'PRI'
when abs(sidxs.part15) = abs(sc.colno) then 'PRI'
when abs(sidxs.part16) = abs(sc.colno) then 'PRI'
else '' end
from sysindexes sidxs
where sidxs.tabid = st.tabid and sidxs.idxtype ='U' 
and sidxs.idxname = (select min(idxname) from sysindices where tabid = st.tabid and idxtype = 'U')), '')
, sc.colname, sc.coltype
        , case when mod(sc.collength,256) = 255 then 0 else mod(sc.collength, 256) end 
                from systables st, syscolumns sc
        where st.tabid = sc.tabid and sc.tabid >= 100
	order by sc.tabid, sc.colno
"""
		elif db_type == "alti":
			sql = """select st.table_name, (select decode(constraint_type,3,'pri','') from system_.sys_constraints_ where scc.table_id = table_id and scc.constraint_id = constraint_id), sc.column_name, sc.data_type
	, sc.scale  
	from system_.sys_tables_ st,  system_.sys_columns_ sc
	left outer join system_.sys_constraint_columns_ scc  
	on sc.table_id = scc.table_id and sc.column_id = scc.column_id 
	where st.table_id = sc.table_id and sc.column_name is not null
	order by st.table_name, sc.column_order"""

		if not items:
			try:
				mycur.execute(sql)
				items = mycur.fetchall()
			except:
				pass


		for item in items:
			tabnm = item[0]
			mycol = item[2]
			pri_ = ''
			if item[1] is not None:
				pri_ = item[1].lower()

			mycolstr = self.getColAttr(db_type, item[3]).lower()
			myprecise = item[4]
		
			if mycolstr.find("decimal") != -1:
			        mycol_attr = "decimal."+str(myprecise)
			elif mycolstr.find("float") != -1 or mycolstr.find("double") != -1:
			        mycol_attr = "float"
			elif mycolstr.find("bigint") != -1 or mycolstr.find("long") != -1 or mycolstr.find("int8") != -1:
			        mycol_attr = "long"
			elif mycolstr.find("int") != -1:
			        mycol_attr = "int"
			elif mycolstr.find("datetime") != -1:
			        mycol_attr = "datetime"
			elif mycolstr.find("date") != -1:
			        mycol_attr = "date"
			else:
			        mycol_attr = "str"
			
			if tabnm in tab_info:
			        tab_info[tabnm][mycol] = mycol_attr
			        tab_info_low[tabnm.lower()][mycol.lower()] = mycol_attr
			        tab_col[tabnm].append(mycol)
			        tab_col_low[tabnm.lower()].append(mycol.lower())
			else:
			        tab_info[tabnm] = {mycol:mycol_attr}
			        tab_info_low[tabnm.lower()] = {mycol.lower():mycol_attr}
			        tab_col[tabnm] = [mycol]
			        tab_col_low[tabnm.lower()] = [mycol.lower()]
			
			#primary key
			if pri_ == 'pri':
			        if tabnm in tab_key:
			                tab_key[tabnm][mycol] = mycol_attr
			                tab_key_low[tabnm.lower()][mycol.lower()] = mycol_attr
			        else:
			                tab_key[tabnm] = {mycol:mycol_attr}
			                tab_key_low[tabnm.lower()] = {mycol.lower():mycol_attr}

			if tabnm not in tab_list:
				tab_list.append(tabnm)
				tab_list_low.append(tabnm.lower())
		if isSensitive:
			self.__mydbinfo.update({'tab_list':tab_list, 'tab_info':tab_info, 'tab_key':tab_key, 'tab_col':tab_col})
		else:
			self.__mydbinfo.update({'tab_list':tab_list_low, 'tab_info':tab_info_low, 'tab_key':tab_key_low, 'tab_col':tab_col_low})
		return

	def getTabList(self):
		return self.__mydbinfo['tab_list']

	def getTabInfo(self):
		return self.__mydbinfo['tab_info'] 

	def getTabKey(self):
		return self.__mydbinfo['tab_key']

	def getTabKeyList(self, tabnm):
		return [ k for k in self.__mydbinfo['tab_key'][tabnm].keys() ]

	def getMyDbInfo(self, mykey):
		return self.__mydbinfo[mykey]

	def getTabColList(self, tabnm):
		return self.__mydbinfo['tab_col'][tabnm]

	def getMyKeyOrdList(self, tabnm):
		myKeyOrd = {}
		for mykey in self.__mydbinfo['tab_key'][tabnm].keys():
			if mykey in self.getTabColList(tabnm):
				myKeyOrd[mykey] = self.getTabColList(tabnm).index(mykey)

		return sorted(myKeyOrd, key=lambda k: myKeyOrd[k])
						

	def exeQry(self, myType, tabnm_or_sql, dataDic={}, force_mykeylist=[], IS_DEBUG=False, IS_PRINT=False, useColFilter=False, useDict=False, useDictLow=False):
		return _exeQry(self.mycur, self.__mydbinfo, myType, tabnm_or_sql, dataDic, force_mykeylist, IS_DEBUG, IS_PRINT, useColFilter, useDict, useDictLow)



###############################################################
#
# use database pool in threading env
#
class MyPool():
	def __init__(self, num_of_pool, db_type="", dbnm=""):
		self.__num_of_pool = num_of_pool
		self.__con_pool = []
		self.__con_pool_use = [0]*num_of_pool

		self.mylock = threading.Lock()

		if db_type != "":
			self.set(db_type, dbnm)

	def __enter__(self):
		return self


	def __exit__(self, exc_type, exc_val, exc_tb):
		for i in range(0, self.__num_of_pool):
			try:
				self.__con_pool[i].close()
			except:
				pass
	
	def set(self, db_type, dbnm):
		with self.mylock:
			for i in range(0, self.__num_of_pool):
				self.__con_pool.append(Mydb(db_type, dbnm))

	def get(self):
		mySlot = -1
		with self.mylock:
			while mySlot == -1:
				i = 0 
				for myStatus in self.__con_pool_use:
					if myStatus == 0:
						self.__con_pool_use[i] = 1
						mySlot = i
						break
					i+=1

		return self.__con_pool[mySlot]

	def free(self, mycon):
		self.__con_pool_use[self.__con_pool.index(mycon)] = 0


				

	def exeQry(self, myType, tabnm_or_sql, dataDic={}, force_mykeylist=[], IS_DEBUG=False, IS_PRINT=False, useColFilter=False, useDict=False, useDictLow=False):
		con = self.get()
		con.exeQry(myType, tabnm_or_sql, dataDic, force_mykeylist, IS_DEBUG, IS_PRINT, useColFilter, useDict, useDictLow)
		self.free(con)



###############################################
#
# get available remote server
#
class MyRemoteServ():
	def __init__(self, dbtype, mytype):
		self.mycon = None
		self.mydb = None
		self.myhost = None
		self.myhosts = []

		self.myconlist = []

		# primary connection and list
		myhostlist = [x.split("@") for x in self.getRQryList(mytype)]
		for mydb, myhost in myhostlist:
			try:
				self.myconlist.append(Mydb(dbtype, mydb, myhost))
				self.myhosts.append(myhost)
			except:
				myPrint(sys.exc_info())

		self.mycon = self.myconlist[0]
		self.mydb = myhostlist[0][0]
		self.myhost = myhostlist[0][1]

	def __del__(self):
		self.close()

	def __enter__(self): 
		return self

	def __exit__(self, exc_type, exc_val, exc_tb):
		self.close()

	def close(self):
		for mycon in self.myconlist:
			mycon.close()

	def getPrimary(self):
		return self.mycon

	def getRQryConList(self):
		return self.myconlist

	def getHostNm(self):
		return self.myhosts
		

	@staticmethod
	def getRQryList(mytype, dt=''):
		reval = []
		with open(os.getenv("HOME")+"/svc/env/remoteqry.lst") as f:
			 for l in f.readlines():
				 myline = l.strip()
				 if myline.find("#") != 0: # commentary
					 if myline.find(mytype) == 0:
						 try:
							 reval = [x+dt for x in myline.split(mytype,1)[1].split(":") if len(x) > 0]
						 except:
							 pass
						 break
		return reval


