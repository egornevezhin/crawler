# -*- coding: utf-8 -*
# Найденые баги:
# 


#   Код для последующей доработки студентами

#   библиотека для работы с url, позволяет скачивать Web-страницы
import urllib2  
#   библиотека для парсинга строки URL
import urlparse
#   библиотека для работы с регулярными выражениями
import re
#   из библиотеки BeautifulSoup берем только класс BeautifulSoup для парсинга HTML
import BeautifulSoup
from BeautifulSoup import BeautifulSoup 
#   из библиотеки time и вложенного в нее модуля time подключаем 
#   функцию для вывода времени согласно формату пользователя
from time import strftime
#   если возникает проблема с ограничением рекурсии, то можно подставить свое значение
import sys

import mysql.connector
import os
import json
import multiprocessing
import Queue

sys.setrecursionlimit(10000)

#   цель для паука
# TARGET_SITE = 'http://itgs.ifmo.ru/'

def envEncode(line):

    return line.decode('utf-8').encode("utf-8")
    #return line.decode('utf-8').encode("cp1251") в случае, если винда

def pTime():
    return strftime("%Y-%m-%d %H:%M:%S")

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
    
class Crawler:
    #   string  доменное имя сайта
    domain = ''
    #   string  адрес текущего url
    currentUrl = ''
    #   list    посещенные страницы
    visitedLinks = []
    #   list    ссылки на внешние ресурсы
    externalLinks = []
    #   list    ссылки, которые необходимо пройти
    linksToFollow = []
    #   list    исходный код страницы
    sourceCode = []
    #   Queue   очередь для ссылок
    in_queue = multiprocessing.JoinableQueue()
    #   int     количество потоков
    num_process = 4
    #   queue   посещенные страницы
    visited = multiprocessing.Queue()
    #   queue    ссылки на внешние ресурсы
    external = multiprocessing.Queue()
    #   queue    ссылки, которые необходимо пройти
    follow = multiprocessing.Queue()

    def __init__(self, startPage):
        '''
        Конструктор класса
        '''
        print envEncode("[%s] Экземпляр Crawler создан" % (pTime(),))
        self.domain = urlparse.urlsplit(startPage).netloc
        print envEncode("[%s] Доменное имя сайта: %s" % (pTime(), self.domain)) 
        startUrl = 'http://' + self.domain
        self.linksToFollow.append(startUrl)
        self.follow.put(startUrl)
        self.crawlUrl(startUrl)
        #while len(self.linksToFollow) > 0:
        #   self.crawlUrl(self.linksToFollow[0])
        
        
    def crawlUrl(self, url):
        '''
        Метод использует рекурсию. Условие остановки: не осталось не посещенных страниц.
        Логика обработки: скачать страницу, отобрать на ней непосещенные url
        Переходы на внешние ресурсы не происходят.
        @param  string  url ссылка для обработки пауком
        '''
        if urlparse.urlsplit(url).netloc == '':
            # исключение для внутренних ссылок
            url = 'http://' + self.domain + url
        #   если страница уже посещена, то повторно обрабатывать страницу не надо
        if url in self.visitedLinks:
            # страницу нужно убрать из очереди
            self.linksToFollow.remove(url)
            # self.follow.get()
            # и вывести количество оставшихся
            print envEncode("Ссылка уже посещена: " + str(url))
            print envEncode("[%s] Внешних ссылок: %d, ссылок посещено: %d, ссылок осталось: %d"\
                % (pTime(),len(self.externalLinks), len(self.visitedLinks), len(self.linksToFollow)))
            print "*" * 20
            return
        #   Помечаем текущую страницу, как посещенную
        self.visitedLinks.append(url)
        # self.visited.put(url)
        #   Убираем текущую ссылку из очереди
        self.linksToFollow.remove(url)
        # self.follow.get()
        self.currentUrl = url
        print envEncode("[+] %s" % (self.currentUrl,))
        try:
            #   Скачаем страницу и помещаем в виде строки в переменную
            html = urllib2.urlopen(self.currentUrl).read()
            #   Парсинг 
            self.parseWebPageContent(html)

            self.sourceCode.append(html)
            self.insertDB(url, html,)

            while not self.external.empty():
                lk = self.external.get()
                self.externalLinks.append(lk)
           
            
            while not self.follow.empty():
                lk = self.follow.get()
                if lk not in self.linksToFollow:
                    self.linksToFollow.append(lk)
        except:
            print envEncode("[ERROR] Ошибка загрузки %s" % (envEncode(self.currentUrl)))
            return
        print "*" * 20
        print envEncode("[%s] Внешних ссылок: %d, ссылок посещено: %d, ссылок осталось: %d"\
            % (pTime(),len(self.externalLinks), len(self.visitedLinks), len(self.linksToFollow)))
        print "*" * 20
        #   Продолжить обход
        #if len(self.linksToFollow) > 0:
        #   self.crawlUrl(self.linksToFollow[0])
        for link in self.linksToFollow:
            # print "NEXT ", link
            self.crawlUrl(link)

            
    def parseWebPageContent(self, html):
        '''
        Парсинг контента: извлечение и анализ ссылок, дополнение очереди
        string html HTML-код страницы
        '''
        #print envEncode(html)
        soup = BeautifulSoup(html)
        for a in soup.findAll('a'):
            self.in_queue.put(a)
            

        for i in xrange(self.num_process):
            worker = multiprocessing.Process(target=self.processing, args=(i, self.in_queue, self.follow, self.external,))
            worker.daemon = True
            worker.start()

        self.in_queue.join()

    def processing(self, thread_num, iq, follow, external):
        while True:
            n = iq.get()
            f = self.checkIfLinkShouldBeFollowed(n, external)
            if f:
                url = n['href']
                #   разбиваем ссылку
                urlsplitResult = urlparse.urlsplit(url)
                #   локальные ссылки не должны включаться
                    
                #   задано доменное имя?
                if urlsplitResult.scheme == '':
                    scheme = 'http'
                else:
                    scheme = urlsplitResult.scheme
                #   собираем ссылку обратно
                url = urlparse.urlunsplit((scheme, 
                        self.domain,
                        urlsplitResult.path,
                        urlsplitResult.query,
                        '',))
                # if url not in follow: #self.linksToFollow:
                follow.put(url)
                self.linksToFollow.append(url)
            iq.task_done()
            # print "%d - %s" % (thread_num, n)

        
    def checkIfLinkShouldBeFollowed(self, a, external):
        '''
        Метод возвращает bool
        Проверка ссылки:
            -- тип ссылки внутренний?
            -- ссылка ведет на страницу с текстовым содержанием?
            -- ссылка не является адресом электронной почты (mailto://)?
        dict a  ассоциативный массив, полученный через поиск по тэгам BeautifulSoup
        '''
        result = False
        #   регулярное выражение для проверки расширения 
        extPattern = re.compile('.*?(\.htm[l]?|\.php|\.phtml|\.sgml|\.jsp|\.asp|\/)$', re.IGNORECASE)
        if a.has_key('href') and a['href'] != None:
            link = a['href']
            urlsplitResult = urlparse.urlsplit(link)
            #   если домен совпадает или пуст, то считать ссылку внутренней
            if urlsplitResult.netloc == self.domain or urlsplitResult.netloc == '': 
                if (urlsplitResult.path != '' or urlsplitResult.query != '') \
                    and extPattern.match(urlsplitResult.path):
                    # print "[INTERNAL]", link
                    result = True
                else:
                    pass
                    # print "[IGNORE]", link
            else:
                #   игнорировать иные протоколы, кроме http и https
                if  urlsplitResult.scheme in ['http', 'https']:
                    if link not in self.externalLinks:
                        # code = urllib2.urlopen(link).read()
                        self.externalLinks.append(link)
                        external.put(link)
                        # print "[EXTERNAL]", link
        return result
        

    def insertDB(self, link, sourceCode):
        # дальше идет кусок исправления
        # в связи с тем, что в рунете появились так назывваемые
        # IDN(Internationalized Domain Names) ссылки, преходится их декодировать
        # русско-английские ссылки не работают
        #   mysql_connector 
        db = connect_mysql()
        #   mysql_cursor
        cur = db.cursor()
        try:
            try:
                cur.execute('INSERT INTO hlopotov (site, source_code) VALUES("' + str(link).decode('utf-8').encode("utf-8") + '",' + json.dumps(sourceCode).decode('utf-8').encode("utf-8") + ')')
                print "*" * 20
                print envEncode("Ссылка добавлена: " + str(link))
                print "*" * 20

            except (UnicodeEncodeError):

                # сюда попадают ссылки с руским языком
                # регуляркой вытаскиваем из них протокол
                # и исправляем ошибку, которая появляеься пре декодировке

                link = re.sub('\http://', '', link)
                link = re.sub('\https://', '', link)
                t = re.sub('\/-4tbm', 'p1ai/', link.encode('idna').encode('utf-8'))
                
                cur.execute('INSERT INTO hlopotov (site, source_code) VALUES("' + t + '",' + json.dumps(sourceCode) + ')')
                print "*" * 20
                print envEncode("Ссылка добавлена: " + str(t))
                print "*" * 20
        except (UnicodeDecodeError):

            # сюда попадают очень плохие ссылки, которые содержат в себе документы
            # или код с непереводящимися в utf-8 символами 
            print "*" * 20
            print envEncode("Ссылку не удалось добавить в базу: " + str(link))
            print "*" * 20
        db.commit()
        cur.close()
        db.close()
    
        
def main():
    '''
    Основная функция
    '''

    
    TARGET_SITE = sys.argv[1]
    # получение ссылки в качестве входного параметра
    crawler = Crawler(TARGET_SITE)  
    

    
if __name__ == "__main__":  
    '''
    Оставляет возможность подключать функции и классы
    этого файла без запуска основной функции.
    Т.о. этот код срабатывает только при python default_crawler.py
    '''
    main()
