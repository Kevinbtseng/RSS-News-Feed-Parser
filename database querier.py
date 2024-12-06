import psycopg2

class databaseQuerier:
    def __init__(self, dbConfig):
        self.dbConfig = dbConfig
        self.conn = None
        self.cursor = None
        self.connect()

    def connect(self):
        self.conn = psycopg2.connect(**self.dbConfig)
        self.cursor = self.conn.cursor()

    def close(self):
        self.cursor.close()
        self.conn.close()

    def searchDB(self, search):
        tableQuery = f"""
        SELECT * FROM master_table
	        WHERE title ILIKE '%{search}%' OR descr ILIKE '%{search}%';
    """
        try:
           self.cursor.execute(tableQuery)
           results = self.cursor.fetchall()
           return results
        except:
            newSearch = input(f"No searches found for {search}, please try a different search:\n")
            self.searchDB(newSearch)


if __name__ == "__main__":
    dbConfig = {'host':"localhost",
            'dbname':"postgres",
            'user':"postgres",
            'password':"1234",
            'port':5432
        }
    
    db = databaseQuerier(dbConfig)

    search = input("Enter a term to search for, or leave blank to display all articles:\n")
    if (search == ""):
        db.cursor.execute("SELECT * FROM master_table")
        results = db.cursor.fetchall()
    else:
        results = db.searchDB(search)
    count = 0
    for result in results:
        count += 1
        print(count)
        for element in result:
            (
                print(element)
            )
        print("\n")

        
