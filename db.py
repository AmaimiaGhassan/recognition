from sqlalchemy import create_engine, Table, MetaData, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError

# Global variable to store the engine instance
engine = None

def run_db():
    global engine

    # If a connection already exists, return it
    if engine is not None:
        return engine

    # Define the connection string
    connection_string = 'postgresql://postgres:password@localhost/faceid'

    # Create an engine instance
    engine = create_engine(connection_string)

    # Test the connection
    try:
        connection = engine.connect()
        print("Connected successfully to PostgreSQL!")
        connection.close()
        return engine
    except SQLAlchemyError as e:
        print(e)
        return None

# Insert a face recognition result into the database
def insert_face(engine, username, encoding, timestamp):
    if engine is None:
        print("Database connection is not established.")
        return

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define the table
    metadata = MetaData()
    faces_table = Table('faces_table', metadata, autoload_with=engine)

    # Insert a record
    try:
        session.execute(faces_table.insert().values(username=username, encoding=encoding, timestamp=timestamp))
        session.commit()
        print("Record inserted successfully.")
    except SQLAlchemyError as e:
        print(e)
        session.rollback()

# Fetch all the faces from the database

def fetch_faces(engine):
    if engine is None:
        print("Database connection is not established.")
        return None

    # Create a session
    Session = sessionmaker(bind=engine)
    session = Session()

    # Define the table
    metadata = MetaData()
    faces_table = Table('faces_table', metadata, autoload_with=engine)

    # Query the table
    try:
        result_proxy = session.execute(select(faces_table))
        result = [row._asdict() for row in result_proxy]
        return result
    except SQLAlchemyError as e:
        print(e)
        return None