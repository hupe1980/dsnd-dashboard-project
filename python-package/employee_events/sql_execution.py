from sqlite3 import connect
from pathlib import Path
from functools import wraps
import pandas as pd

# Using pathlib, create a `db_path` variable
# that points to the absolute path for the `employee_events.db` file
db_path = Path(__file__).parent / "employee_events.db"


# OPTION 1: MIXIN
# Define a class called `QueryMixin`
class QueryMixin:

    # Define a method named `pandas_query`
    # that receives an sql query as a string
    # and returns the query's result
    # as a pandas dataframe
    def pandas_query(self, sql_query: str, params=None) -> pd.DataFrame:
        # Create a connection to the database
        with connect(db_path) as conn:
            # Use pandas to read the sql query
            # and return the result as a dataframe
            df = pd.read_sql_query(sql_query, conn, params=params)

        return df

    # Define a method named `query`
    # that receives an sql_query as a string
    # and returns the query's result as
    # a list of tuples. (You will need
    # to use an sqlite3 cursor)
    def query(self, sql_query: str, params: tuple = ()) -> list[tuple]:
        # Create a connection to the database
        with connect(db_path) as conn:
            # Create a cursor object
            cursor = conn.cursor()
            # Execute the sql query
            cursor.execute(sql_query, params)
            # Fetch all results and return them
            result = cursor.fetchall()

        return result


# Leave this code unchanged
def query(func):
    """
    Decorator that runs a standard sql execution
    and returns a list of tuples
    """

    @wraps(func)
    def run_query(*args, **kwargs):
        query_string, params = func(*args, **kwargs)
        connection = connect(db_path)
        cursor = connection.cursor()
        result = cursor.execute(query_string, params).fetchall()
        connection.close()
        return result

    return run_query
