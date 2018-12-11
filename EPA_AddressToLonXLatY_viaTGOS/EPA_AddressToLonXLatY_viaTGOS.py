import unittest, time, re, os, mmap, shutil, glob, time, logging
import pymssql # http://pymssql.org/en/latest/index.html 
import csv,linecache
import requests
import json
import codecs
from DB import ExecuteQuery as exeQry
from Setting import Config as conf
from datetime import datetime
from Twd97_Transfer import fromwgs84, towgs84
#Twd97_Transfer.towgs84(248170.787, 2652129.936)
#(23.97387462949248, 120.98202461950673)

#Twd97_Transfer.fromwgs84(23.973875, 120.982025)
#(248170.82582552364, 2652129.9773471127)

def WriteLog(strMsg,level="info"):
    print(strMsg)
    if (level == "debug"): 
        logging.debug("," + GetDateTime() + "," + strMsg)    
    elif (level == "info"):
        logging.info("," + GetDateTime() + "," + strMsg)
    elif (level == "warning"):
        logging.warning("," + GetDateTime() + "," + strMsg)
    elif (level == "error"):
        logging.error("," + GetDateTime() + "," + strMsg)

def GetDateTime():
    localtime = time.localtime(time.time())
    strDateTime = str(localtime[0]) + "-" + str(localtime[1]).zfill(2) + "-" + str(localtime[2]).zfill(2) + " " + str(localtime[3]).zfill(2) + ":" + str(localtime[4]).zfill(2) + ":" + str(localtime[5]).zfill(2) #'2009-01-05 22:14:39'

    return strDateTime

def GetDate():
    localtime = time.localtime(time.time())
    strDate = str(localtime[0]) + "-" + str(localtime[1]).zfill(2) + "-" + str(localtime[2]).zfill(2) #'2009-01-05'

    return strDate

def LogInit():
    LogPath =  conf.Value('Log','LogPath')  
    MkDirectory(LogPath)   
    logfilename = LogPath + "\\" + GetDate() + ".log"  

    LogLevel = conf.Value('Log','LogLevel')  
    if (LogLevel == "DEBUG"):    
        logging.basicConfig(filename=logfilename,level=logging.DEBUG)         
    elif (LogLevel == "INFO"): 
        logging.basicConfig(filename=logfilename,level=logging.INFO)
    elif (LogLevel == "WARNING"):
        logging.basicConfig(filename=logfilename,level=logging.WARNING)
    elif (LogLevel == "ERROR"): 
        logging.basicConfig(filename=logfilename,level=logging.ERROR)

def MkDirectory(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except:
        os.mkdir(directory) 

def Diff_Dates(d1, d2):
    return abs((d2 - d1).days)

def ConfigInit():
    global ServerIP
    global User
    global Password 
    global DBName
    global CSVFileName
    global TGOSAddressTransferUrl    
    global TGOSQueryAddrAppId
    global TGOSQueryAddrAPIKey

    global LnProcessed 

    ServerIP = conf.Value('DB','ServerIP')
    User = conf.Value('DB','User')
    Password = conf.Value('DB','Password')   
    DBName = conf.Value('DB','DBName')  

    CSVFileName = conf.Value('CSV','CSVFileFullPath')  

    TGOSQueryAddrAppId = conf.Value('URL','TGOSQueryAddrAppId')  
    TGOSQueryAddrAPIKey = conf.Value('URL','TGOSQueryAddrAPIKey')  
    TGOSAddressTransferUrl = conf.Value('URL','TGOSAddressTransferUrl') # Configparser and string with % error -- https://stackoverflow.com/questions/14340366/configparser-and-string-with
   

def GetDBConnection_CallByRef(): 
    try:
        ## get DB connection information, http://pymssql.org/en/latest/pymssql_examples.html
        # Server,User,Password,DBName are global variables
        mConn = pymssql.connect(ServerIP, User, Password, DBName)
        return mConn

    except Exception as e:
        ErrMsg = str(e)
        WriteLog(ErrMsg,level="error")


def GetLonXLatYByAddress(StartLine):
    funName = "GetLonXLatYByAddress(StartLine)"
    ErrMsg = ""
    try:
        WaitTime = int(conf.Value('Wait','WaitSecondTime')) 

        scriptpath = os.path.realpath(__file__) # 取得python程式目錄
        app_folder = scriptpath[:scriptpath.rfind("\\")]
        outFile = codecs.open(app_folder + "\\" + "Temple_WGS84_XY.csv", "w", 'utf-8') #寫入 UTF8編碼 檔案 # https://stackoverflow.com/questions/19591458/python-reading-from-a-file-and-saving-to-utf-8/19591815

        WriteLog("Read CSV File Content...",level="info")
        idx = int(StartLine)-1 # 第一行 idx為0
        f = linecache.getlines(CSVFileName)[idx:] # http://blog.51cto.com/wangwei007/1246214
        reader = csv.reader(f)
        #CSV內欄位格式:
        #編號,縣市  ,行政區,寺廟中文名,寺廟英文名,主神        ,地址              ,經度坐標,緯度坐標,外部OLD連結
        #1   ,高雄市,內門區,慈雲宮,              ,媽袓天上聖母,虎頭山內東里1鄰1號,NULL    ,NULL    ,http://linked-data.moi.gov.tw/resource/Temple/10775
        LnProcessed = StartLine
        for row in reader:
            RowStr = ','.join(row) # Convert list to string  
            SerNo = RowStr.split(',')[0].strip()  # 編號
            County = RowStr.split(',')[1].strip()  # 縣市
            Town = RowStr.split(',')[2].strip()  # 行政區
            Address = RowStr.split(',')[6].strip()  # 地址
            FullAddress = County + Town + Address  #所要查詢的門牌地址

            #坐標系統(SRS)EPSG:4326(WGS84)國際通用, EPSG:3825 (TWD97TM119) 澎湖及金馬適用,EPSG:3826 (TWD97TM121) 台灣地區適用,EPSG:3827 (TWD67TM119) 澎湖及金馬適用,EPSG:3828 (TWD67TM121) 台灣地區適用
            aSRS = "EPSG:3826";
            #0:最近門牌號機制,1:單雙號機制,2:[最近門牌號機制]+[單雙號機制]
            aFuzzyType = "0";
            #回傳的資料格式，允許傳入的代碼為：JSON、XML
            aResultDataType = "JSON";
            #模糊比對回傳門牌號的許可誤差範圍，輸入格式為正整數，如輸入 0 則代表不限制誤差範圍
            aFuzzyBuffer = "0";
            #是否只進行完全比對，允許傳入的值為：true、false，如輸入 true ，模糊比對機制將不被使用
            aIsOnlyFullMatch = "false";
            #是否鎖定縣市，允許傳入的值為：true、false，如輸入 true ，則代表查詢結果中的 [縣市] 要與所輸入的門牌地址中的 [縣市] 完全相同
            aIsLockCounty = "false";
            #是否鎖定鄉鎮市區，允許傳入的值為：true、false，如輸入 true ，則代表查詢結果中的 [鄉鎮市區] 要與所輸入的門牌地址中的 [鄉鎮市區] 完全相同
            aIsLockTown = "false";
            #是否鎖定村里，允許傳入的值為：true、false，如輸入 true ，則代表查詢結果中的 [村里] 要與所輸入的門牌地址中的 [村里] 完全相同
            aIsLockVillage = "false";
            #是否鎖定路段，允許傳入的值為：true、false，如輸入 true ，則代表查詢結果中的 [路段] 要與所輸入的門牌地址中的 [路段] 完全相同
            aIsLockRoadSection = "false";
            #是否鎖定巷，允許傳入的值為：true、false，如輸入 true ，則代表查詢結果中的 [巷] 要與所輸入的門牌地址中的 [巷] 完全相同
            aIsLockLane = "false";
            #是否鎖定弄，允許傳入的值為：true、false，如輸入 true ，則代表查詢結果中的 [弄] 要與所輸入的門牌地址中的 [弄] 完全相同
            aIsLockAlley = "false";
            #是否鎖定地區，允許傳入的值為：true、fals，如輸入 true ，則代表查詢結果中的 [地區] 要與所輸入的門牌地址中的 [地區] 完全相同
            aIsLockArea = "false";
            #號之、之號是否視為相同，允許傳入的值為：true、false
            aIsSameNumber_SubNumber = "false";
            #TGOS_Query_Addr_Url WebService網址
            aCanIgnoreVillage = "true";
            #找不時是否可忽略村里，允許傳入的值為：true、false
            aCanIgnoreNeighborhood = "true";
            #找不時是否可忽略鄰，允許傳入的值為：true、false
            aReturnMaxCount = "0";
            #如為多筆時，限制回傳最大筆數，輸入格式為正整數，如輸入 0 則代表不限制回傳筆數

            dictData = {}
            dictData['oAPPId'] = TGOSQueryAddrAppId
            dictData['oAPIKey'] = TGOSQueryAddrAPIKey
            dictData['oAddress'] = FullAddress
            dictData['oSRS'] = aSRS
            dictData['oFuzzyType'] = aFuzzyType
            dictData['oResultDataType'] = aResultDataType
            dictData['oFuzzyBuffer'] = aFuzzyBuffer
            dictData['oIsOnlyFullMatch'] = aIsOnlyFullMatch
            dictData['oIsLockCounty'] = aIsLockCounty
            dictData['oIsLockTown'] = aIsLockTown
            dictData['oIsLockVillage'] = aIsLockVillage
            dictData['oIsLockRoadSection'] = aIsLockRoadSection
            dictData['oIsLockLane'] = aIsLockLane
            dictData['oIsLockAlley'] = aIsLockAlley
            dictData['oIsLockArea'] = aIsLockArea
            dictData['oIsSameNumber_SubNumber'] = aIsSameNumber_SubNumber
            dictData['oCanIgnoreVillage'] = aCanIgnoreVillage
            dictData['oCanIgnoreNeighborhood'] = aCanIgnoreNeighborhood
            dictData['oReturnMaxCount'] = aReturnMaxCount

            WriteLog("Address start transfering to XY...",level="info")

            r = requests.post(TGOSAddressTransferUrl, dictData)
            print(r.status_code, r.reason)
            print(r.text)
            str_st = r.text

            str_st = str_st.replace("\n","").replace("\r\n","").replace("\r","").strip() # 先轉字串再清除字串中不必要的\n符號，最後再利用strip()去除字串前後空白
            strXML_Head = r'<?xml version="1.0" encoding="utf-8"?><string xmlns="http://tempuri.org/">'
            strXML_Tail = r'</string>'
            str_st = str_st.replace(strXML_Head,"").replace(strXML_Tail,"").strip() 

            json_data = json.loads(str_st)
            if (len(json_data['AddressList']) > 0):
                Twd97_x = json_data['AddressList'][0]['X']
                Twd97_y = json_data['AddressList'][0]['Y']
                LatY,LonX = towgs84(Twd97_x,Twd97_y)

                InsertDataToDB(SerNo,LonX,LatY) # X,Y 更新寫入DB

                print('writing Line:' + str(SerNo) + ' line to file.')
                outFile.write(','.join(row) + "," + str(LonX) + "," + str(LatY))
                outFile.write("\n")

            else: 
                print('writing Line:' + str(SerNo) + ' line to file.')
                outFile.write(','.join(row) + ",,")
                outFile.write("\n")

            LnProcessed = LnProcessed + 1 # 紀錄已完成處理到第幾列

            ##寫入完成等待數秒後再與伺服器連線，避免TGOS回應逾時
            #WaitTime = int(conf.Value('Wait','WaitSecondTime'))
            #time.sleep(WaitTime)  

        outFile.close()

    except Exception as e:
        ErrMsg = "(" + __file__ + "-" + funName + "):  " + str(e)
        if (ErrMsg.find(r"'cp950' codec can't encode character") != -1): # 無法處理時則跳過這列，往下一列繼續
            GetLonXLatYByAddress(LnProcessed+1)
            ErrMsg = ""

            
    finally:
        if ErrMsg != "" :
            WriteLog(ErrMsg,level="error")

def InsertDataToDB(TempleID,LonX,LatY):
    funName = "GetLonXLatYByAddress(StartLine)"
    ErrMsg = ""
    try:
        conn = GetDBConnection_CallByRef()
        conn.autocommit(True)
        cursor = conn.cursor()

        dictionary_with_the_updated = {
            'param1': LonX,
            'param2': LatY,
            'param3': TempleID
        }

        SQLCMD = "UPDATE tbTempleLocation \
                  SET LonX = %(param1)s,LatY = %(param2)s   \
                  WHERE SerNo = %(param3)s"

        cursor.execute(SQLCMD,dictionary_with_the_updated)
        WriteLog("資料更新寫入DB完成" ,level="info")

    except Exception as e:
        ErrMsg = "(" + __file__ + "-" + funName + "):  " + str(e)
        WriteLog(ErrMsg,level="error")

def main():
    try:   
        LogInit()
        ConfigInit()
        print(GetDateTime())
        StartLine = int(conf.Value('CSV','ReadFileStartLine'))
        GetLonXLatYByAddress(StartLine)

    except Exception as e:
        ErrMsg = "(" + __file__ + "-" + funName + "):  " + str(e)
        print(ErrMsg)
        WriteLog(ErrMsg,level="error")

    finally:
        print(GetDateTime())


if __name__ == '__main__':
    # execute only if run as the entry point into the program
    main()