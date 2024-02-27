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
        OwnerID INT NOT NULL CHECK(OwnerID > 0),
        FOREIGN KEY (OwnerID) REFERENCES Owner(OID)
    );
    """
    create_reservation_table = """
    CREATE TABLE Reservation (
        RID INT PRIMARY KEY CHECK(RID > 0),
        CustomerID INT NOT NULL CHECK(CustomerID > 0),
        ApartmentID INT NOT NULL CHECK(ApartmentID > 0),
        FOREIGN KEY (CustomerID) REFERENCES Customer(CID),
        FOREIGN KEY (ApartmentID) REFERENCES Apartment(AID),
        StartDate DATE NOT NULL,
        EndDate DATE NOT NULL,
        CHECK(EndDate > StartDate),
        UNIQUE(ApartmentID, StartDate),
        UNIQUE(ApartmentID, EndDate),
        Price DECIMAL NOT NULL CHECK(Price > 0)
    );
    """
    create_review_table = """
    CREATE TABLE Review (
        CustomerID INT NOT NULL CHECK(CustomerID > 0),
        ApartmentID INT NOT NULL CHECK(ApartmentID > 0),
        FOREIGN KEY (CustomerID) REFERENCES Customer(CID),
        FOREIGN KEY (ApartmentID) REFERENCES Apartment(AID),
        Rating INT NOT NULL CHECK(Rating >= 0 AND Rating <= 10),
        ReviewDate DATE NOT NULL,
        ReviewText TEXT
    );
    """
    full_query = create_customer_table + create_owner_table + create_apt_table + create_reservation_table + create_review_table
    conn.execute(full_query)
    conn.commit()
    conn.close()


def clear_tables():
    # TODO: implement
    pass


def drop_tables():
    # TODO: implement
    pass


def add_owner(owner: Owner) -> ReturnValue:
    # TODO: implement
    pass


def get_owner(owner_id: int) -> Owner:
    # TODO: implement
    pass


def delete_owner(owner_id: int) -> ReturnValue:
    # TODO: implement
    pass


def add_apartment(apartment: Apartment) -> ReturnValue:
    # TODO: implement
    pass


def get_apartment(apartment_id: int) -> Apartment:
    # TODO: implement
    pass


def delete_apartment(apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def add_customer(customer: Customer) -> ReturnValue:
    # TODO: implement
    pass


def get_customer(customer_id: int) -> Customer:
    # TODO: implement
    pass


def delete_customer(customer_id: int) -> ReturnValue:
    # TODO: implement
    pass


def customer_made_reservation(customer_id: int, apartment_id: int, start_date: date, end_date: date, total_price: float) -> ReturnValue:
    # TODO: implement
    pass


def customer_cancelled_reservation(customer_id: int, apartment_id: int, start_date: date) -> ReturnValue:
    # TODO: implement
    pass


def customer_reviewed_apartment(customer_id: int, apartment_id: int, review_date: date, rating: int, review_text: str) -> ReturnValue:
    # TODO: implement
    pass


def customer_updated_review(customer_id: int, apartmetn_id: int, update_date: date, new_rating: int, new_text: str) -> ReturnValue:
    # TODO: implement
    pass


def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:
    # TODO: implement
    pass


def get_apartment_owner(apartment_id: int) -> Owner:
    # TODO: implement
    pass


def get_owner_apartments(owner_id: int) -> List[Apartment]:
    # TODO: implement
    pass


# ---------------------------------- BASIC API: ----------------------------------

def get_apartment_rating(apartment_id: int) -> float:
    # TODO: implement
    pass


def get_owner_rating(owner_id: int) -> float:
    # TODO: implement
    pass


def get_top_customer() -> Customer:
    # TODO: implement
    pass


def reservations_per_owner() -> List[Tuple[str, int]]:
    # TODO: implement
    pass


# ---------------------------------- ADVANCED API: ----------------------------------

def get_all_location_owners() -> List[Owner]:
    # TODO: implement
    pass


def best_value_for_money() -> Apartment:
    # TODO: implement
    pass


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    # TODO: implement
    pass


def get_apartment_recommendation(customer_id: int) -> List[Tuple[Apartment, float]]:
    # TODO: implement
    pass
