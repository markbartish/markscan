import os
import sys
import zlib
import sqlite3

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
            print(f' ідентітет: {hkey}')
            print(f' адреса   : {fileAbsPath}')
        else:
            print(f'Знайдено вже сканований файл з НЕОДНАКОВОЮ адресою')
            print(f' ідентітет: {hkey}')
            print(f' адреса1: {fileAbsPath}')
            print(f' адреса2: {tuple[1]}')
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

def cmdStatus():
    con = init_db(False)
    cur = con.cursor()
    hlookupres = cur.execute(f'SELECT COUNT(hashkey) FROM ht;')
    resultlist = hlookupres.fetchall()
    print(f'Зараз хештаблиця містить {len(resultlist)} елементів')
    cur.close()

def cmdReset():
    init_db(True)
    exit(0)

def cmdScan():
    con = init_db(False)
    hashAllFilesInPath(con, PATHTTOSCAN)

if len(sys.argv) < 2:
    printhelp()
    exit(1)
else:
    print(f'Оперую з хештаблицею: {DBFILEFULLNAME}')
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
