import os
import sys
import zlib
import sqlite3
from time import sleep


class bcolors:
    HEADER        = '\033[95m'
    OKBLUE        = '\033[94m'
    OKCYAN        = '\033[96m'
    OKGREEN       = '\033[92m'
    WARNING       = '\033[93m'
    DBGGREY       = '\033[90m'
    FAIL          = '\033[91m'
    ENDC          = '\033[0m'
    BOLD          = '\033[1m'
    UNDERLINE     = '\033[4m'


def dprint(text):
    print(f'{bcolors.DBGGREY}{text}{bcolors.ENDC}')

def progress(percent=0, width=30):
    left = width * percent // 100
    right = width - left
    print('\r[', '#' * left, ' ' * right, ']',
          f' {percent:.0f}%',
          sep='', end='', flush=True)


# Find out users home dir
HOMEDIR = os.path.expanduser("~")
# Define the name of the hash database file residing in that home dir
DBFILENAME = ".markscan_htable"
# Define the full name of the hash database file to use
DBFILEFULLNAME = os.path.join(HOMEDIR, DBFILENAME)
# Define the path to scan
PATHTTOSCAN = os.getcwd()
# define verbose mode
verbose_mode = False

def init_db(unconditionally_drop):
    con = sqlite3.connect(DBFILEFULLNAME)
    cur = con.cursor()
    if unconditionally_drop == True:
        cur.execute("DROP TABLE IF EXISTS ht;")
    cur.execute("DROP TABLE IF EXISTS dublettes;")
    cur.execute("CREATE TABLE IF NOT EXISTS ht        (hashkey INTEGER PRIMARY KEY, fsize INTEGER NOT NULL, path VARCHAR(1024));")
    cur.execute("CREATE TABLE IF NOT EXISTS dublettes (hashkey INTEGER PRIMARY KEY, path1 VARCHAR(1024), path2 VARCHAR(1024));")
    con.commit()
    cur.close()
    return con


def print_findings(dbConn):
    cur = dbConn.cursor()
    dublettesres = cur.execute(f'SELECT hashkey, path1, path2 FROM dublettes;')
    resultlist = dublettesres.fetchall()
    if len(resultlist) == 0:
        print('Ніяких дублетів нема!', file=sys.stderr)
        return
    print(f'Ось знайдені дублети', file=sys.stderr)
    for r in resultlist:
        print(f' Ідентітет: {bcolors.UNDERLINE}{r[0]}{bcolors.ENDC}', file=sys.stderr)
        print(f' Файл-1:    {bcolors.BOLD}{bcolors.OKGREEN}{r[1]}{bcolors.ENDC}', file=sys.stderr)
        print(f' Файл-2:    {bcolors.BOLD}{bcolors.OKGREEN}{r[2]}{bcolors.ENDC}', file=sys.stderr)
    dublettescountres = cur.execute(f'SELECT COUNT(hashkey) FROM dublettes;')
    resultlist = dublettescountres.fetchall()
    dubls = int(resultlist[0][0])
    print(f'Усього {dubls} дублетів\n')
    cur.close()

def register_hash(dbConn, fileAbsPath, hkey, fsize):
    cur = dbConn.cursor()
    hlookupres = cur.execute(f'SELECT hashkey, path FROM ht WHERE hashkey = {hkey};')
    resultlist = hlookupres.fetchall()
    if (len(resultlist) > 0):
        tuple = resultlist[0]
        if fileAbsPath == tuple[1]:
            if verbose_mode == True:
                print(f'Знайдено вже сканований файл з однаковою адресою', flush=True)
                print(f' Ідентітет: {bcolors.UNDERLINE}{hkey}{bcolors.ENDC}', flush=True)
                print(f' Файл     : {bcolors.BOLD}{bcolors.WARNING}{fileAbsPath}{bcolors.ENDC}', flush=True)
        else:
            cnt_res = cur.execute(f'SELECT COUNT(hashkey) FROM dublettes WHERE hashkey = {hkey};')
            if (int(cnt_res.fetchall()[0][0]) == 0):
                cur.execute(f'INSERT INTO dublettes(hashkey, path1, path2) VALUES({hkey}, \'{tuple[1]}\', \'{fileAbsPath}\');')
            else:
                pass # should do some kind of warning
    else:
        cur.execute(f'INSERT INTO ht(hashkey, fsize, path) VALUES({hkey}, {fsize}, \'{fileAbsPath}\');')
        dbConn.commit()
    cur.close()

def sum_files(dbConn, dirpath, filelist):
    global verbose_mode
    print(f'Працюю з дір: {dirpath}', flush=True)
    if verbose_mode == False:
        progress(0)
    fidx = 0
    for fname in filelist:
        fsum = 0
        filefullname = os.path.abspath(os.path.join(dirpath, fname))
        with open(filefullname, mode='rb') as file:
            fdata = file.read()
            # file.close()
            fsize = len(fdata)
            if fsize > 0:
                fsum = zlib.crc32(fdata)
                register_hash(dbConn, filefullname, fsum, fsize)
        if verbose_mode == False:
            progress(int((fidx / len(filelist))*100))
        fidx += 1

    if verbose_mode == False:
        progress(100)
        print(f'\n', flush=True)
            

def hashAllFilesInPath(dbConn, thePath):
    w = os.walk(thePath)
    for (dirpath, dirnames, filenames) in w:
        sum_files(dbConn, dirpath, filenames)

def printhelp():
    print(f'Треба ну хоча-б один з комманд:')
    print(f'    python {sys.argv[0]} status            - Перевірка актуального статуса хеш-таблиці')
    print(f'    python {sys.argv[0]} reset             - Знищення хеш-таблиці')
    print(f'    python {sys.argv[0]} scan              - Cтворення хеш-таблиці в актуальної директорїї')

def cmdStatus(printOut = True):
    con = init_db(False)
    cur = con.cursor()
    hlookupres = cur.execute(f'SELECT COUNT(hashkey) FROM ht;')
    resultlist = hlookupres.fetchall()
    tuples = resultlist[0]
    if (printOut == True):
        print(f'Зараз хештаблиця містить {tuples[0]} елементів')
    cur.close()
    con.close()
    return tuples[0]

def cmdReset():
    wasRecords = cmdStatus(False)
    con = init_db(True)
    nowIsRecords = cmdStatus(False)

    if (nowIsRecords == 0):
        print(f'reset виконано успішно:')
        print(f'В хештаблиці було {wasRecords} елементів а зараз їх там {nowIsRecords}')
    else:
        print(f'З reset щось пішло не так:')
        print(f'В хештаблиці було {wasRecords} елементів а зараз їх там {nowIsRecords}')
    con.close()
    exit(0)


def cmdScan():
    con = init_db(False)
    global verbose_mode
    hashAllFilesInPath(con, PATHTTOSCAN)
    print_findings(con)
    con.close()

def main():
    if len(sys.argv) < 2:
        printhelp()
        exit(1)
    else:
        print(f'{bcolors.OKCYAN}Оперую з хештаблицею: {DBFILEFULLNAME}{bcolors.ENDC}\n', flush=True)
        if (sys.argv[1] == "status"):
            cmdStatus()
            exit(0)
        elif (sys.argv[1] == "reset"):
            cmdReset()
            exit(0)
        elif (sys.argv[1] == "scan"):
            if len(sys.argv) > 2 and sys.argv[2] == "-v":
                global verbose_mode
                verbose_mode = True
            cmdScan()
        else:
            printhelp()
            exit(1)

main()
