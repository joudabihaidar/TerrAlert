import psycopg2
import logging

class Database:
    def __init__(self, config, database_name):
        self.config = config
        self.config['dbname'] = database_name  # Set the database name in the config
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.config['host'],
                dbname=self.config['dbname'],  # Now using the provided database name
                user=self.config['user'],
                password=self.config['password'],
                port=self.config['port']
            )
            logging.info("Successfully connected to the database.")
        except Exception as e:
            logging.error(f"Error connecting to the database: {str(e)}")
            raise

    def close(self):
        if self.conn:
            self.conn.close()
            logging.info("Database connection closed.")

    def execute_query(self, query):
        cursor = self.conn.cursor()
        try:
            cursor.execute(query)
            self.conn.commit()
            logging.info(f"Executed query: {query}")
        except Exception as e:
            self.conn.rollback()
            logging.error(f"Error executing query: {str(e)}")
            raise
        finally:
            cursor.close()
