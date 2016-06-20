import crawler
import mysql.connector
import multiprocessing
import Queue
import sys


def envEncode(line):
    return line.decode('utf-8').encode("utf-8")


def processing(thread_num, iq, target):
    while True:
        n = iq.get()
        f = crawler.Crawler(n)
        if target in f.externalLinks:
            print "*" * 20
            print "Find in " + n
            print "*" * 20
            iq.task_done()
            return
        iq.task_done()
        print "%d - %s" % (thread_num, n)


def connect_mysql():
    config = { 
        'user': 'root', 
        'password': 'root', 
        'host': 'localhost',
        'port': '8889', 
        'database': 'ex',
        }

    db = mysql.connector.connect(**config)       # name of the data base
    return db


def main():
    db = connect_mysql()
    cur = db.cursor()
    sites = multiprocessing.JoinableQueue()
    TARGET_SITE = sys.argv[1]
    num_process = 4
    cr = crawler.Crawler(TARGET_SITE)

    for site in cr.externalLinks:
        sites.put(envEncode(site))
        print site

    for i in xrange(num_process):
        worker = multiprocessing.Process(target=processing, args=(i, sites, TARGET_SITE))
        worker.daemon = True
        worker.start()
        
    sites.join()
    print "*" * 20
    print "No find in external site"
    print "*" * 20


if __name__ == "__main__":  

    main()
