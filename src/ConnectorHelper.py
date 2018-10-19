import psycopg2
import os

class ConnectorHelper:
    def __init__(self):
        self.conn = None
        # Connect to Redshift
        host_red = os.environ["REDSHIFT_HOST"]
        dbname_redshift = os.environ["database_name"]
        user_redshift = os.environ["redshift_user"]
        password_redshift = os.environ["redshift_pwd"]
        port_redshift = 5439
        self.conn = psycopg2.connect(host=host_red, dbname=dbname_redshift,
                                user=user_redshift, password=password_redshift, port=port_redshift)
    # Creating Connection and returning it 
    def get_connection(self):
        return self.conn

    # Closing the connection
    def close_connection(self):
        self.conn.close()
