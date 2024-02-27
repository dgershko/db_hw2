from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------

def create_tables():
    conn = Connector.DBConnector()
    create_owner_table = """
    CREATE TABLE Owner (
        OID INT PRIMARY KEY CHECK(OID > 0),
        Name VARCHAR(50) NOT NULL
    );
    """
    create_customer_table = """
    CREATE TABLE Customer (
        CID INT PRIMARY KEY CHECK(CID > 0),
        Name VARCHAR(50) NOT NULL
    );
    """
    create_apt_table = """
    CREATE TABLE Apartment (
        AID INT PRIMARY KEY CHECK(AID > 0),
        Address VARCHAR(150) NOT NULL,
        City VARCHAR(50) NOT NULL,
        Country VARCHAR(50) NOT NULL,
        UNIQUE (Address, Country, City),
        Size INT NOT NULL CHECK(Size > 0),
        OwnerID INT,
        FOREIGN KEY (OwnerID) REFERENCES Owner(OID)
            ON DELETE SET NULL
            ON UPDATE CASCADE,
    );
    """
    create_reservation_table = """
    CREATE TABLE Reservation (
        ReservationID SERIAL PRIMARY KEY,
        CustomerID INT NOT NULL,
        ApartmentID INT NOT NULL,
        StartDate DATE NOT NULL,
        EndDate DATE NOT NULL,
        CHECK(EndDate > StartDate),
        UNIQUE(ApartmentID, StartDate),
        UNIQUE(ApartmentID, EndDate),
        Price DECIMAL NOT NULL CHECK(Price > 0)
        CONSTRAINT FkCustomer
            FOREIGN KEY (CustomerID) 
            REFERENCES Customer(CustomerID)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
        CONSTRAINT FkApartment
            FOREIGN KEY (ApartmentID) 
            REFERENCES Apartment(ApartmentID)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
    );
    """
    create_review_table = """
    CREATE TABLE Review (
        ReviewID  SERIAL PRIMARY KEY,
        CustomerID INT NOT NULL,
        ApartmentID INT NOT NULL,
        Rating INT NOT NULL CHECK(Rating >= 0 AND Rating <= 10),
        ReviewDate DATE NOT NULL,
        ReviewText TEXT
        CONSTRAINT FkCustomer
            FOREIGN KEY (CustomerID) 
            REFERENCES Customer(CustomerID)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
        CONSTRAINT FkApartment
            FOREIGN KEY (ApartmentID) 
            REFERENCES Apartment(ApartmentID)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
        UNIQUE(CustomerID, ApartmentID)
    );
    """
    full_query = create_customer_table + create_owner_table + create_apt_table + create_reservation_table + create_review_table
    conn.execute(full_query)
    conn.commit()
    conn.close()


def clear_tables():
    # Could potentially need to be DELETE instead of TRUNCATE
    clear_tables_query = """
    TRUNCATE TABLE Owner;
    TRUNCATE TABLE Customer;
    TRUNCATE TABLE Apartment;
    TRUNCATE TABLE Reservation;
    TRUNCATE TABLE Review;
    """
    conn = Connector.DBConnector()
    conn.execute(clear_tables_query)
    conn.commit()
    conn.close()

def drop_tables():
    drop_tables_query = """
    DROP TABLE Owner;
    DROP TABLE Customer;
    DROP TABLE Apartment;
    DROP TABLE Reservation;
    DROP TABLE Review;
    """
    conn = Connector.DBConnector()
    conn.execute(drop_tables_query)
    conn.commit()
    conn.close()


#Add an owner to the database
def add_owner(owner: Owner) -> ReturnValue:
    owner_id = owner.get_owner_id()
    owner_name = owner.get_owner_name()
    add_owner_query = f"""
    INSERT INTO Owner (OID, Name)
    VALUES ({owner_id}, '{owner_name}')
    """
    conn = Connector.DBConnector()
    try:
        conn.execute(add_owner_query)
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.commit()
    conn.close()

def get_owner(owner_id: int) -> Owner: #Doron
    conn = Connector.DBConnector()
    _, result_set = conn.execute("SELECT OID, Name FROM Owner WHERE OID = {owner_id}")
    owner_object = Owner(result_set[0]['OID'], result_set[0]['Name'])
    conn.commit()
    conn.close()
    return owner_object


#Delete an owner from the database.
def delete_owner(owner_id: int) -> ReturnValue: #Daniel
    # TODO: implement
    pass

#Add an apartment to the database.
def add_apartment(apartment: Apartment) -> ReturnValue: #Doron
    conn = Connector.DBConnector()
    apartment_id = apartment.get_id()
    apartment_address = apartment.get_address()
    apartment_city = apartment.get_city()
    apartment_country = apartment.get_country()
    apartment_size = apartment.get_size()
    add_apartment_query = f"""
        INSERT INTO Apartment (AID, Address, City, Country, Size, OwnerID)
        VALUES ({apartment_id}, '{apartment_address}', '{apartment_city}', '{apartment_country}', '{apartment_size}')
        """
    conn.execute(add_apartment_query)
    conn.commit()
    conn.close()

#Get an apartment from the database.
def get_apartment(apartment_id: int) -> Apartment: #Daniel
    # TODO: implement
    pass

#Delete an apartment from the database.
def delete_apartment(apartment_id: int) -> ReturnValue: #Doron
    conn = Connector.DBConnector()
    delete_apartment_query = f"""DELETE FROM Apartment WHERE AID = {apartment_id}"""
    conn.execute(delete_apartment_query)
    conn.commit()
    conn.close()

#Add a customer to the database.
def add_customer(customer: Customer) -> ReturnValue: #Daniel
    # TODO: implement
    pass

#Get a customer from the database.
def get_customer(customer_id: int) -> Customer: #Doron
    conn = Connector.DBConnector()
    get_customer_query = f"""SELECT CID, Name FROM Customer WHERE CID = {customer_id}"""
    _, result_set = conn.execute(get_customer_query)
    customer_object = Customer(result_set[0]['CID'], result_set[0]['Name'])
    conn.commit()
    conn.close()
    return customer_object

#Delete a customer from the database.
def delete_customer(customer_id: int) -> ReturnValue: #Daniel
    # TODO: implement
    pass

#Customer made a reservation of apartment from start_date to end_date and paid total_price
def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date, total_price: float) -> ReturnValue: #Doron
    conn = Connector.DBConnector()
    customer_made_reservation_query = f"""INSERT INTO Reservation (CustomerID, ApartmentID, StartDate, EndDate, Price) 
                                       VALUES ({customer_id}, '{apartment_id}', '{start_date}', '{end_date}', '{total_price}')"""
    conn.execute(customer_made_reservation_query)
    conn.commit()
    conn.close()

#Remove a reservation from the database.
def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue: #Daniel
    # TODO: implement
    pass

#Customer reviewed apartment on date review_date and gave it rating stars, with text review_text.
def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int, review_text: str) -> ReturnValue: #Doron
    # TODO: implement
    pass

#Customer decided to update their review of apartment on update_date and changed his rating to new_rating and the review text to new_text
def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int, new_text: str) -> ReturnValue: #Daniel
    # TODO: implement
    pass

#Owner owns apartment. An apartment can be owned by at most one owner.
def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue: #Doron
    # TODO: implement
    pass

#Owner dropped apartment and does not own it anymore.
def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue: #Daniel
    # TODO: implement
    pass

#Get the owner of apartment.
def get_apartment_owner(apartment_id: int) -> Owner: #Doron
    # TODO: implement
    pass

#Get a list of all apartments owned by owner.
def get_owner_apartments(owner_id: int) -> List[Apartment]: #Daniel
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------

#Get the average rating across all reviews of apartment.
def get_apartment_rating(apartment_id: int) -> float:
    # TODO: implement
    pass

#Get the average of averages of ratings from all reviews of apartments owned by owner.
def get_owner_rating(owner_id: int) -> float:
    # TODO: implement
    pass

#Get the customer that made the most reservations.
def get_top_customer() -> Customer:
    # TODO: implement
    pass

#Output: a list of tuples of (owner_name, total_reservation_count) of all owners in the database.
def reservations_per_owner() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

#Return all owners that own an apartment in every city there are apartments in.
def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    pass

#Get the apartment that has the best reviews compared to its average nightly price.
def best_value_for_money() -> Apartment:
    # TODO: implement
    pass


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass

""" In this query you will need to approximate what the given customer will rate an apartment they haven’t been in, based on their and other users’ reviews.
You will use the following method for the approximation:
For every customer (other than the one you were given) that has reviewed an apartment the
given customer also reviewed (or multiple apartments), you will calculate the ratio between
their ratings (or the average of ratios if there is more than one apartment reviewed by both).
Then, for every customer you calculated a ratio for, the approximated ratio for an apartment
they reviewed would be the ratio multiplied by the rating they gave the other apartment. If
you get multiple approximations for the same apartment, return their average.
Generate an approximation for all apartments where it is possible. """
def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass

"""
class ConnectionInvalid(_Exceptions):
class NOT_NULL_VIOLATION(_Exceptions):
class FOREIGN_KEY_VIOLATION(_Exceptions):
class UNIQUE_VIOLATION(_Exceptions):
class CHECK_VIOLATION(_Exceptions):
class database_ini_ERROR(_Exceptions):
class UNKNOWN_ERROR(_Exceptions):
"""
"""
class ReturnValue(Enum):
    OK = 0
    NOT_EXISTS = 1
    ALREADY_EXISTS = 2
    ERROR = 3
    BAD_PARAMS = 4
"""
def handle_errors(e: DatabaseException):
    e_name  = e.__str__()
    if e_name == 'UNIQUE_VIOLATION':
        return ReturnValue['ALREADY_EXISTS']
    elif e_name == 'NOT_NULL_VIOLATION':
        return ReturnValue['BAD_PARAMS']
    elif e_name == 'FOREIGN_KEY_VIOLATION':
        return ReturnValue['BAD_PARAMS']
    elif e_name == 'CHECK_VIOLATION':
        return ReturnValue['BAD_PARAMS']
    elif e_name == 'database_ini_ERROR':
        return ReturnValue['ERROR']
    elif e_name == 'UNKNOWN_ERROR':
        return ReturnValue['ERROR']
    elif e_name == 'ConnectionInvalid':
        return ReturnValue['ERROR']

exception_list = (DatabaseException.ConnectionInvalid,
                  DatabaseException.NOT_NULL_VIOLATION,
                  DatabaseException.FOREIGN_KEY_VIOLATION,
                  DatabaseException.UNIQUE_VIOLATION,
                  DatabaseException.CHECK_VIOLATION,
                  DatabaseException.database_ini_ERROR,
                  DatabaseException.UNKNOWN_ERROR
                  )
