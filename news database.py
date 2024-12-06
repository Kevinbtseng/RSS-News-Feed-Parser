import feedparser
import psycopg2
import logging
from datetime import date
from urllib.parse import urlparse

class newsDatabase:
    def __init__(self, dbConfig):
        self.dbConfig = dbConfig
        self.conn = None
        self.cursor = None
        self.connect()
        logging.basicConfig(filename='failedFeeds.log', level=logging.INFO, format='%(asctime)s %(message)s')


    def connect(self):
        self.conn = psycopg2.connect(**self.dbConfig)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def getTableName(self):
        dateForTable = date.today().strftime("%m_%d_%Y")

        tableName = (f"top_of_{dateForTable}")

        return tableName
   
    def getTableQuery(self):
        tableName = self.getTableName()
        tableQuery = f"""
        CREATE TABLE IF NOT EXISTS {tableName} (
                    title TEXT,
                    descr TEXT,
                    pubDate TIMESTAMPTZ,
                    tags TEXT[],
                    guid TEXT UNIQUE,
                    source TEXT
        );
        """
        return tableQuery

    def createTable(self):
        tableQuery = self.getTableQuery()

        self.cursor.execute(tableQuery)

        self.conn.commit()


    def insertArticle(self, title, descr, pubDate, tags, guid, source):
        tableName = self.getTableName()

        insertQuery = f"""
        INSERT INTO {tableName} (title, descr, pubDate, tags, guid, source)
        VALUES(%s, %s, %s, %s, %s, %s)
        ON CONFLICT (guid) DO UPDATE SET
        title = EXCLUDED.title,
        descr = EXCLUDED.descr,
        pubDate = EXCLUDED.pubDate,
        tags = EXCLUDED.tags,
        source = EXCLUDED.source;
        """

        self.cursor.execute(insertQuery, (title, descr, pubDate, tags, guid, source))
        self.conn.commit()

    def getSourceName(self, feedURL):
        source = urlparse(feedURL).netloc

        return source

    def logFailed(self, sourceName):
        logging.info(f"Failed to retrieve data from {sourceName}")

    def insertNewsEntries(self, feedURL):
        feed = feedparser.parse(feedURL)
        sourceName = self.getSourceName(feedURL)
        if feed.bozo:
            self.logFailed(sourceName)
            return
        else:
            for article in feed.entries:
                tags = []
                title = article.title
                descr = article.description
                pubDate = article.published
                guid = article.id
                try:
                    for tag in article.tags:
                        tags.append(tag.term)
                except AttributeError:
                    pass
                source = sourceName
                self.insertArticle(title, descr, pubDate, tags, guid, source)


if __name__ == "__main__":  
    dbConfig = {'host':"localhost",
                'dbname':"postgres",
                'user':"postgres",
                'password':"1234",
                'port':5432
            }
   
    db = newsDatabase(dbConfig)
    db.createTable()
    listOfURLs = ['https://rss.nytimes.com/services/xml/rss/nyt/HomePage.xml',
                  'https://moxie.foxnews.com/google-publisher/latest.xml',
                  'https://nypost.com/feed/',
                  'https://www.forbes.com/innovation/feed2',
                  'https://www.dailymail.co.uk/home/index.rss',
                  'https://www.cnbc.com/id/100003114/device/rss/rss.html',
                  'https://feeds.bbci.co.uk/news/rss.xml',
                  'https://www.newsweek.com/rss',
                  'https://www.cbsnews.com/latest/rss/main',
                  'https://feeds.nbcnews.com/nbcnews/public/news'
]


    for url in listOfURLs:
        db.insertNewsEntries(url)

    db.close()