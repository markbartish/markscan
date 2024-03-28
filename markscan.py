import os
import sys
import zlib
import sqlite3


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


# Find out users home dir
HOMEDIR = os.path.expanduser("~")
# Define the name of the hash database file residing in that home dir
DBFILENAME = ".markscan_htable"
# Define the full name of the hash database file to use
DBFILEFULLNAME = os.path.join(HOMEDIR, DBFILENAME)
# Define the path to scan
PATHTTOSCAN = os.getcwd()

def init_db(unconditionally_drop):
    con = sqlite3.connect(DBFILEFULLNAME)
    cur = con.cursor()
    if unconditionally_drop == True:
        cur.execute("DROP TABLE ht;")
    cur.execute("CREATE TABLE IF NOT EXISTS ht(hashkey INTEGER PRIMARY KEY, path VARCHAR(1024));")
    con.commit()
    cur.close()
    return con

def register_hash(dbConn, fileAbsPath, hkey):
    cur = dbConn.cursor()
    hlookupres = cur.execute(f'SELECT hashkey, path FROM ht WHERE hashkey = {hkey};')
    resultlist = hlookupres.fetchall()
    if (len(resultlist) > 0):
        tuple = resultlist[0]
        if fileAbsPath == tuple[1]:
            print(f'Знайдено вже сканований файл з однаковою адресою')
            print(f' Ідентітет: {bcolors.UNDERLINE}{hkey}{bcolors.ENDC}')
            print(f' Файл   : {bcolors.BOLD}{bcolors.WARNING}{fileAbsPath}{bcolors.ENDC}')
        else:
            print(f'Схоже я щось знайшов. Ось ці два файла мають ')
            print(f' Ідентітет: {bcolors.UNDERLINE}{hkey}{bcolors.ENDC}')
            print(f' Файл-1:    {bcolors.BOLD}{bcolors.OKGREEN}{fileAbsPath}{bcolors.ENDC}')
            print(f' Файл-2:    {bcolors.BOLD}{bcolors.OKGREEN}{tuple[1]}{bcolors.ENDC}')
    else:
        cur.execute(f'INSERT INTO ht(hashkey, path) VALUES({hkey}, \'{fileAbsPath}\');')
        dbConn.commit()
    cur.close()


def sum_files(dbConn, dirpath, filelist):
    for fname in filelist:
        fsum = 0
        filefullname = os.path.abspath(os.path.join(dirpath, fname))
        with open(filefullname, mode='rb') as file:
            fdata = file.read()
            fsum = zlib.crc32(fdata)
            register_hash(dbConn, filefullname, fsum)



def hashAllFilesInPath(dbConn, thePath):
    w = os.walk(thePath)
    for (dirpath, dirnames, filenames) in w:
        for d in dirnames:
            absd = os.path.abspath(os.path.join(thePath, d))
            hashAllFilesInPath(dbConn, absd)
        sum_files(dbConn, dirpath, filenames)

def printhelp():
    print(f'Треба ну хоча-б один з комманд:')
    print(f'    {sys.argv[0]} status            - Перевірка актуального статуса хеш-таблиці')
    print(f'    {sys.argv[0]} reset             - Знищення хеш-таблиці')
    print(f'    {sys.argv[0]} scan              - Cтворення хеш-таблиці в актуальної директорїї')

def cmdStatus(printOut = False):
    con = init_db(False)
    cur = con.cursor()
    hlookupres = cur.execute(f'SELECT COUNT(hashkey) FROM ht;')
    resultlist = hlookupres.fetchall()
    tuples = resultlist[0]
    if (printOut == True):
        print(f'Зараз хештаблиця містить {tuples[0]} елементів')
    cur.close()
    return tuples[0]

def cmdReset():
    wasRecords = cmdStatus()
    init_db(True)
    nowIsRecords = cmdStatus()

    if (nowIsRecords == 0):
        print(f'reset Виконано успішно:')
        print(f'В хештаблиці було {wasRecords} елементів а зараз їх там {nowIsRecords}')
    else:
        print(f'З reset щось пішло не так:')
        print(f'В хештаблиці було {wasRecords} елементів а зараз їх там {nowIsRecords}')
    exit(0)

def cmdScan():
    con = init_db(False)
    hashAllFilesInPath(con, PATHTTOSCAN)

if len(sys.argv) < 2:
    printhelp()
    exit(1)
else:
    print(f'{bcolors.OKCYAN}Оперую з хештаблицею: {DBFILEFULLNAME}{bcolors.ENDC}')
    if (sys.argv[1] == "status"):
        cmdStatus()
        exit(0)
    elif (sys.argv[1] == "reset"):
        cmdReset()
        exit(0)
    elif (sys.argv[1] == "scan"):
        cmdScan()
    else:
        printhelp()
        exit(1)
