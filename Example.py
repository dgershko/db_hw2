import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Utility.DBConnector import ResultSet
from psycopg2 import sql


def dropTable() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("DROP TABLE IF EXISTS Users CASCADE")
    except DatabaseException.ConnectionInvalid as e:
        # do stuff
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        # do stuff
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        # do stuff
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after code try termination or exception handling
        conn.close()


def createTable() -> None:
    conn = None
    try:
        conn = Connector.DBConnector()
        conn.execute("CREATE TABLE Users(id INTEGER PRIMARY KEY, name TEXT NOT NULL)")
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        # will happen any way after try termination or exception handling
        conn.close()


def getUsers(printSchema) -> ResultSet:
    conn = None
    rows_effected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        rows_effected, result = conn.execute("SELECT * FROM Users", printSchema=printSchema)
        # rows_effected is the number of rows received by the SELECT
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return result


def addUser(ID: int, name: str) -> ReturnValue:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Users(id, name) VALUES({id}, {username})").format(id=sql.Literal(ID),
                                                                                       username=sql.Literal(name))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return ReturnValue.OK


def deleteUser(ID: int) -> int:
    conn = None
    rows_effected = 0
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("DELETE FROM Users WHERE id={0}").format(sql.Literal(ID))
        rows_effected, _ = conn.execute(query)
    except DatabaseException.ConnectionInvalid as e:
        print(e)
    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)
    except DatabaseException.CHECK_VIOLATION as e:
        print(e)
    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)
    except Exception as e:
        print(e)
    finally:
        conn.close()
        return rows_effected


if __name__ == '__main__':
    print("0. Creating all tables")
    createTable()
    print("1. Add user with ID 1 and name Roei")
    addUser(1, 'Roei')
    print("2. Add user with ID 2 and name Noa")
    addUser(2, 'Noa')
    print("3. Printing all users")
    users = getUsers(printSchema=True)  # will cause printing the users, because printSchema=true in getUsers()
    print('4. Printing user in the second row')
    print(users[1]['id'], users[1]['name'])
    print("5. Printing all IDs")
    for index in range(users.size()):
        print(users[index]['ID'])
    print("6. Delete user with ID 1")
    deleteUser(1)
    print("7. Printing all users")
    users = getUsers(printSchema=False)  # will not cause printing the users, because printSchema=false in getUsers()
    # print users
    for index in range(users.size()):  # for each user
        current_row = users[index]  # get the row
        for col in current_row:  # iterate over the columns
            print(str(col) + "=" + str(current_row[col]))
    print("8. Printing all users - no change")
    users = getUsers(printSchema=False)  # will not cause printing the users, because printSchema=false in getUsers()
    # print users
    for index in range(users.size()):  # for each user
        current_row = users[index]  # get the row
        for col in current_row:  # iterate over the columns
            print(str(col) + "=" + str(current_row[col]))
    print("9. Dropping all tables - empty database")
    dropTable()
