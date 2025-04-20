import mysql.connector

class DB:
    def __init__(self):
        try:
            self.conn = mysql.connector.connect(
                host="localhost",
                user="root",
                password="password",
                database="chats"
            )
            self.cursor = self.conn.cursor()
            print("Connected to MySQL")
        except mysql.connector.Error as e:
            print(f"MySQL error: {e}")

    def broadcast_insert(self, sender, message):
        query = "INSERT INTO `broadcast_messages` (`sender_id`, `content`) VALUES (%s, %s)"
        self.cursor.execute(query, (sender, message))
        self.conn.commit()

db = DB()