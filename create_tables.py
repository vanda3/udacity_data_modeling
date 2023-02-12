import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from sqlalchemy_schemadisplay import create_schema_graph
from sqlalchemy import MetaData


def create_database():
    """
    - Creates and connects to the sparkifydb
    - Returns the connection and cursor to sparkifydb
    """
    
    # connect to default database
    conn = psycopg2.connect("host=127.0.0.1 dbname=studentdb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the sparkify database. 
    
    - Establishes connection with the sparkify database and gets
    cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables needed. 
    
    - Finally, closes the connection. 
    """
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


def generate_schema():
    """
        Auto generates start schema png.
        NOTE: foreigner keys were giving problems due to empty data, removed. E.g. for songplays:
            CONSTRAINT fk1 
                FOREIGN KEY(start_time) 
                    REFERENCES time(start_time),
            CONSTRAINT fk2 
                FOREIGN KEY(user_id) 
                    REFERENCES users(user_id),
            CONSTRAINT fk3 
                FOREIGN KEY(song_id) 
                    REFERENCES songs(song_id),
            CONSTRAINT fk4 
                FOREIGN KEY(artist_id) 
                    REFERENCES artists(artist_id)
    """
    graph = create_schema_graph(metadata=MetaData('postgresql://student:student@127.0.0.1/sparkifydb'))
    graph.write_png('generated_star_diagram.png')
    
    
if __name__ == "__main__":
    main()
    generate_schema()