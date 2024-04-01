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

DUBLETTES_SQL = """
SELECT hashkeys.hashkey AS hkey, path FROM paths INNER JOIN hashkeys 
    ON hashkeys.id = paths.hashkey_id 
    WHERE hashkey_id IN (
        SELECT hashkey_id FROM (
            SELECT paths.hashkey_id, COUNT(path) AS cpaths FROM paths GROUP BY paths.hashkey_id
        ) WHERE cpaths > 1
    );
"""

def init_db(unconditionally_drop):
    con = sqlite3.connect(DBFILEFULLNAME)
    cur = con.cursor()
    if unconditionally_drop == True:
        cur.execute("DROP TABLE IF EXISTS scan_sessions;")
        cur.execute("DROP TABLE IF EXISTS hashkeys;")
        cur.execute("DROP TABLE IF EXISTS paths;")
        cur.execute("DROP TABLE IF EXISTS toprune;")
    cur.execute("CREATE TABLE IF NOT EXISTS scan_sessions (id INTEGER PRIMARY KEY, tstamp DATETIME NOT NULL, rootpath VARCHAR(4096));")
    cur.execute("CREATE TABLE IF NOT EXISTS hashkeys      (id INTEGER PRIMARY KEY, hashkey INTEGER NOT NULL UNIQUE);")
    cur.execute("CREATE TABLE IF NOT EXISTS paths         (id INTEGER PRIMARY KEY, hashkey_id INTEGER NOT NULL, fsize INTEGER NOT NULL, path VARCHAR(4096) UNIQUE, FOREIGN KEY(hashkey_id) REFERENCES hashkeys(id));")
    cur.execute("CREATE TABLE IF NOT EXISTS toprune       (id INTEGER PRIMARY KEY, path_id INTEGER NOT NULL UNIQUE, FOREIGN KEY(path_id) REFERENCES paths(id));")
    con.commit()
    cur.close()
    return con


def print_findings(dbConn):
    cur = dbConn.cursor()
    dublettesres = cur.execute(DUBLETTES_SQL)
    resultlist = dublettesres.fetchall()
    if len(resultlist) == 0:
        print('Ніяких дублетів нема!', file=sys.stderr)
        return
    print(f'Ось знайдені дублети', file=sys.stderr)
    ht = {}
    for r in resultlist:
        if (r[0] in ht):
            ht[r[0]].append(r[1])
        else:
            ht[r[0]] = [r[1]]
    for htelem in ht.items():
        print(f'Ідентітет: {bcolors.UNDERLINE}{htelem[0]}{bcolors.ENDC}', file=sys.stderr)
        num = 1
        for e in htelem[1]:
            print(f'    Файл-{num}: {bcolors.BOLD}{bcolors.OKGREEN}{e}{bcolors.ENDC}', file=sys.stderr)
            num += 1
    dubls = len(ht)
    print(f'Усього {dubls} ідентітетів яки існують в двох чи більше екземплярів.\n')
    cur.close()


# TODO: Now not handline the possible:
#   * hashkey does not exist in hashkeys but a path already is registered for some hashkey
#      (possible changed file contents)
#   * deleted files that exist in paths are to be handled by prune()
def register_hash(dbConn, fileAbsPath, hkey, fsize):
    cur = dbConn.cursor()
    #dprint('register_hash()')
    fileAbsPathEscaped = fileAbsPath.replace("'", "''")
    # OLD: hlookupres = cur.execute(f'SELECT hashkey FROM hashkeys WHERE hashkey = {hkey};')
    # NEW:

    # Check if hashkey exists in the hashkeys table
    hlookupres =    cur.execute(f'SELECT id, hashkey FROM hashkeys WHERE hashkey = {hkey};').fetchall()
    #dprint(f'hlookupres = {hlookupres}')
    hkcount = len(hlookupres)

    if (hkcount == 0):
        #dprint('hkcount == 0')
        cur.execute(f'INSERT INTO hashkeys (hashkey) VALUES ({hkey})')
        
    hlookupres =    cur.execute(f'SELECT id, hashkey FROM hashkeys WHERE hashkey = {hkey};').fetchall()
    hkcount = len(hlookupres)

    # Check that the path to be registered already exists and is connected to that hashkey

    pathscountres = cur.execute(f'SELECT COUNT(*) FROM paths WHERE hashkey_id = {hlookupres[0][0]} AND path = \'{fileAbsPathEscaped}\' ;').fetchall()

    pathscount = int(pathscountres[0][0])

    #dprint(f' pc = {pathscount}')
    #dprint(pathscountres[0][0])
    if (pathscount == 0):
        sqlString = f'INSERT INTO paths (hashkey_id, fsize, path) VALUES({hlookupres[0][0]}, {fsize}, \'{fileAbsPathEscaped}\');'
        try:    
            cur.execute(sqlString)
            dbConn.commit()
        except sqlite3.OperationalError:
            print(f'\n{bcolors.FAIL}Сорян, якась помилка при реєстрації у хештаблиці.{bcolors.ENDC}', flush=True)
            #dprint(sqlString)
            cur.close()
            dbConn.close()
            exit(1)
        # except sqlite3.IntegrityError:
        #     print(f'\n{bcolors.FAIL}Сорян, якась помилка при реєстрації у хештаблиці.{bcolors.ENDC}', flush=True)
        #     dprint(sqlString)
        #     cur.close()
        #     dbConn.close()
        #     exit(1)
    else:
        pass

    #TODO:
    #Consider that a paths crc32 sum can have changed since the last time

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
            fsize = len(fdata)
            if fsize > 0:
                fsum = zlib.crc32(fdata)
                register_hash(dbConn, filefullname, fsum, fsize)
        if verbose_mode == False:
            progrPerCent = int((fidx / len(filelist))*100)
            progress(progrPerCent)
        fidx += 1

    if verbose_mode == False:
        progress(100)
        print(f'\n', flush=True)
            

def hashAllFilesInPath(dbConn, thePath):
    w = os.walk(thePath)
    for (dirpath, dirnames, filenames) in w:
        sum_files(dbConn, dirpath, filenames)

def generateThePruneList(dbConn):
    cur = dbConn.cursor()
    resultset = cur.execute(f'SELECT path FROM paths;')
    for r in resultset:
        pathExists = print(os.path.exists(r[0]))
        if pathExists == True:
            print(f'Path in the list: {r[0]}: {bcolors.OKGREEN}  EXISTS{        bcolors.ENDC}')
        else:
            print(f'Path in the list: {r[0]}: {bcolors.FAIL   }  DOES NOT EXIST{bcolors.ENDC}')
    cur.close()

def printThePruneList(dbConn):
    cur = dbConn.cursor()
    resultset = cur.execute(f'SELECT path FROM paths INNER JOIN toprune ON paths.id = toprune.path_id;').fetchall()
    for r in resultset:
        print(f'{r[0]}')
    print(f'Тотально у списку prune {len(resultset)} елементів.')
    cur.close()

def doPrune(dbConn):
    cur = dbConn.cursor()
    cur.execute(f'DELETE FROM paths WHERE id IN (SELECT path_id FROM toprune)')
    cur.close()


def printhelp():
    print(f'Треба ну хоча-б один з комманд:')
    print(f'    python {sys.argv[0]} status            - Перевірка актуального статуса хеш-таблиці')
    print(f'    python {sys.argv[0]} reset             - Знищення хеш-таблиці')
    print(f'    python {sys.argv[0]} scan              - Cтворення хеш-таблиці в актуальної директорїї')
    print(f'    python {sys.argv[0]} prune             - Пошук адрес з хештаблиці яки (більше) не існують у файловій системі')

def cmdStatus(printOut = True):
    con = init_db(False)
    cur = con.cursor()
    hlookupres = cur.execute(f'SELECT COUNT(hashkey) FROM hashkeys;')
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
    #global verbose_mode
    hashAllFilesInPath(con, PATHTTOSCAN)
    print_findings(con)
    con.close()

def cmdPrune():
    con = init_db(False)
    global verbose_mode
    generateThePruneList(con)
    printThePruneList(con)
    # Ask the usr whether really prune and then prune

    doPrune(con)


def main():
    if len(sys.argv) < 2:
        printhelp()
        exit(1)
    else:
        if len(sys.argv) > 2 and sys.argv[2] == "-v":
            global verbose_mode
            verbose_mode = True
        print(f'{bcolors.OKCYAN}Оперую з хештаблицею: {DBFILEFULLNAME}{bcolors.ENDC}\n', flush=True)
        if (sys.argv[1] == "status"):
            cmdStatus()
            exit(0)
        elif (sys.argv[1] == "reset"):
            cmdReset()
            exit(0)
        elif (sys.argv[1] == "scan"):
            cmdScan()
            exit(0)
        elif (sys.argv[1] == "prune"):
            cmdPrune()
            exit(0)
        else:
            printhelp()
            exit(1)

main()
