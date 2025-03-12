파이썬의 각종디비(IBM informix, Altibase, postgresql, mysql, mongodb, sqlite) 에 대한 dbapi를 제공합니다.
IBM Informix 와 Altibase를 사용하기위해서는 odbc 설정 되어 있어야만 합니다.

import mydbapi3


class Mydb
각종 데이터베이스에 대한 python dbapi를 제공합니다. 자동키, retrieve 데이터 딕셔너리 제공 등 다양한 기능을 제공합니다.

_db_type="mysql", _dbnm="idcb", _myHost='localhost', _mySock="", _myPort="", _myUser="infomax", _charset="euckr", _myEncoding="euckr",  _useWarning=False, _myDecoding="euckr", _myAutocommit=True, _isSensitive=True

1. _db_type
   데이터베이스 타입
   ifx = IBM informix
   mysql = MYSQL
   alti = Altibase
   pgsql = postgresql
   mongo = mongodb
   sqlite = sqlite

3. _dbnm
   데이터베이스
5. _myHost
   호스트명
7. _mySocket
   소켓
9. _myPort
    포트
11. _myUser
  user에 따른 패스워드는 내부에서 따로 관리
12. _charset
    접속캐릭터셋
14. _myEncoding
    client encoding
16. _useWarning
    mysql에서만 작동 warning여부
17. _myDecoding
    client decoding
18. _myAutocommit
    autocommit
19. _isSensitive
    대소문자구분

    
 def exeQry(self, myType, tabnm_or_sql, dataDic={}, force_mykeylist=[], IS_DEBUG=False, IS_PRINT=False, useColFilter=False, useDict=False, useDictLow=False):
 1. myType
    G : tabnm_or_sql 의 sql을 실행합니다.
    I : tabnm_or_sql의 tabnm 을 dataDic={} 항목의 스키마의 키를 기본으로 자동으로 Insert and Update
    IO : tabnm_or_sql의 tabnm 을 dataDic={} 항목의 스키마의 키를 기본으로 자동으로 Insert Only 실패시 exception
    U : tabnm_or_sql의 tabnm 을 dataDic={} 항목의 스키마의 키를 기본으로 자동으로 Update 실패시 exception
    D : tabnm_or_sql의 tabnm 을 dataDic={} 항목의 스키마의 키를 기본으로 자동으로 Delete 실패시 exception
    R : tabnm_or_sql의 tabnm 을 dataDic={} 항목의 스키마의 키를 기본으로 자동으로 Replace 실패시 exception(구문을 지원하는 DBMS에서만 작동)
    M : tabnm_or_sql의 tabnm 을 dataDic=[] 항목의 스키마의 키를 기본으로 자동으로 executemany를 실행 실패시 exception(구문을 지원하는 DBMS에서만 작동)
2. tabnm_or_sql
   table 명이나 sql statement가 될 수 있다 myType 참조
3. dataDic
   셋팅할 컬럼값들의 딕셔너리(단, executemany에선 리스트 형태로 제공해야함)
4. force_mykeylist
   기존 tabnm_or_sql의 table의 스키마를 무시하고 셋팅한 컬럼을 강제 키로 셋팅
5. IS_DEBUG
   True : 실제로 commit하지 않고 최종 sql 형태만 인쇄
6. IS_PRINT
   True : 실행되는 sql을 print
7. useColFilter
   True : 셋팅할 dataDic에 tabnm_or_sql의 table에 존재하지 않는 컬럼이 있다면 exception없이 스킵하여 존재하는 컬럼만으로 처리
8. useDict
   True : tabnm_or_sql의 select sql statement를 실행할 시 데이터를 값리스트가 아닌 컬럼과 함께 딕셔너리 형태로 제공
9. useDictLow
   True : useDict와 동일한 기능이지만 딕셔너리 컬럼은 무조건 lowercase


===========================================================================================================================================================================



class MyPool()
mydbapi3를 기반으로 한 thread pool을 제공하는 API입니다.

num_of_pool, db_type="", dbnm=""
1. num_of_pool
  커넥션풀 갯수
2. db_type
   mydbapi3에서 제공하는 기본데이터베이스 타입 (1. _db_type 참조)
3. dbnm
   database 명

def get():
커넥션풀의 빈슬롯의 커넥션을 받아오고 해당 슬롯을 닫습니다.

def free():
커녁션풀을 반납하고 해당슬롯을 Available 하게 합니다.

def exeQry(self, myType, tabnm_or_sql, dataDic={}, force_mykeylist=[], IS_DEBUG=False, IS_PRINT=False, useColFilter=False, useDict=False, useDictLow=False):
mydbapi3의 exeQry를 래핑한 함수입니다. 다만 내부적으로 thread pool의 슬롯을 사용하고 바로 반납합니다.



class MyRemoteServ()
설정되어 있는 다중서버간 데이터 동기화를 위해 쓰입니다.

dbtype, mytype
1. dbtype
   mydbapi3에서 제공하는 기본데이터베이스 타입 (1. _db_type 참조)
2. mytype
   remoteqry.lst에 설정되어있는 type을 따릅니다.

@staticmethod
def getRQryList(mytype, dt=''):
remoteqry.lst에 설정되어있는 type을 따릅니다.
 
