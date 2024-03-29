from typing import List, Tuple
from psycopg2 import sql
from datetime import date, datetime

import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Utility.DBConnector import ResultSet, ResultSetDict

from Business.Owner import Owner
from Business.Customer import Customer
from Business.Apartment import Apartment


# ---------------------------------- CRUD API: ----------------------------------


def create_tables():
    conn = Connector.DBConnector()
    create_owner_table = """
    CREATE TABLE IF NOT EXISTS Owner (
        OwnerID INT PRIMARY KEY CHECK(OwnerID > 0),
        Name VARCHAR(50) NOT NULL
    );
    """
    create_customer_table = """
    CREATE TABLE IF NOT EXISTS Customer (
        CustomerID INT PRIMARY KEY CHECK(CustomerID > 0),
        Name VARCHAR(50) NOT NULL
    );
    """
    create_apt_table = """
    CREATE TABLE IF NOT EXISTS Apartment (
        ApartmentID INT PRIMARY KEY CHECK(ApartmentID > 0),
        Address VARCHAR(150) NOT NULL,
        City VARCHAR(50) NOT NULL,
        Country VARCHAR(50) NOT NULL,
        UNIQUE (Address, Country, City),
        Size INT NOT NULL CHECK(Size > 0)
    );
    """
    create_owns_table = """
    CREATE TABLE IF NOT EXISTS Owns (
        OwnerID INT NOT NULL REFERENCES Owner(OwnerID) ON DELETE CASCADE ON UPDATE CASCADE CHECK(OwnerID>0),
        ApartmentID INT NOT NULL REFERENCES Apartment(ApartmentID) ON DELETE CASCADE ON UPDATE CASCADE CHECK(ApartmentID>0),
        PRIMARY KEY (ApartmentID)
    );
    """
    create_reservation_table = """
    CREATE TABLE IF NOT EXISTS Reservation (
        ReservationID SERIAL PRIMARY KEY,
        CustomerID INT NOT NULL,
        ApartmentID INT NOT NULL,
        StartDate DATE NOT NULL,
        EndDate DATE NOT NULL,
        CHECK(EndDate >= StartDate),
        Price DECIMAL NOT NULL CHECK(Price > 0),
        CONSTRAINT FkCustomer
            FOREIGN KEY (CustomerID) 
            REFERENCES Customer(CustomerID)
            ON DELETE CASCADE
            ON UPDATE CASCADE,
        CONSTRAINT FkApartment
            FOREIGN KEY (ApartmentID) 
            REFERENCES Apartment(ApartmentID)
            ON DELETE CASCADE
            ON UPDATE CASCADE
    );
    """
    # ReviewID SERIAL PRIMARY KEY,
    create_review_table = """
    CREATE TABLE IF NOT EXISTS Review (
        CustomerID INT NOT NULL CHECK(CustomerID>0),
        ApartmentID INT NOT NULL CHECK(ApartmentID>0),
        PRIMARY KEY (CustomerID, ApartmentID),
        Rating INT NOT NULL CHECK(Rating >= 1 AND Rating <= 10),
        ReviewDate DATE NOT NULL,
        ReviewText TEXT,
        CONSTRAINT FkCustomer
            FOREIGN KEY (CustomerID) 
            REFERENCES Customer(CustomerID)
            ON DELETE CASCADE,
        CONSTRAINT FkApartment
            FOREIGN KEY (ApartmentID) 
            REFERENCES Apartment(ApartmentID)
            ON DELETE CASCADE
    );
    """
    apt_avg_rating_view = """
    CREATE OR REPLACE VIEW Ratings AS
    SELECT ApartmentID, AVG(Rating) as AvgRating
    FROM Review
    GROUP BY ApartmentID;
    """
    owner_apts_view = """
    CREATE OR REPLACE VIEW OwnerApartments AS
    SELECT o.OwnerID ,a.*
    FROM Owner o
    JOIN Owns os ON o.OwnerID = os.OwnerID
    JOIN Apartment a ON os.ApartmentID = a.ApartmentID;
    """
    # owner rating view provides a view of the average rating of each owner, using the OwnerApartments view and the Ratings view
    owner_avg_rating_view = """
    CREATE OR REPLACE VIEW OwnerRating AS
    SELECT oa.OwnerID as OwnerID, AVG(r.AvgRating) as AvgRating
    FROM OwnerApartments oa 
    JOIN Ratings r ON oa.ApartmentID = r.ApartmentID
    GROUP BY oa.OwnerID;
    """
    customer_reservation_count_view = """
    CREATE OR REPLACE VIEW CustomerReservations AS
    SELECT CustomerID, COUNT(*) as Reservations
    FROM Reservation
    GROUP BY CustomerID;
    """
    owner_reservation_count_view = """
    CREATE OR REPLACE VIEW OwnerReservation AS
    SELECT o.Name, COUNT(*) as ReservationCount
    FROM OwnerApartments oa
    JOIN Reservation r ON oa.ApartmentID = r.ApartmentID
    JOIN Owner o ON oa.OwnerID = o.OwnerID
    GROUP BY oa.OwnerID, o.Name;
    """
    uniq_CityCountry_count_view = """
    CREATE VIEW TotalCityCountryCount AS
    SELECT COUNT(DISTINCT(City, Country)) AS TotalCityCountryCount
    FROM Apartment;
    """
    owner_CityCountry_count_view = """
    CREATE VIEW OwnerCityCountryCount AS
    SELECT o.OwnerID, o.Name, COUNT(DISTINCT(a.City, a.Country)) AS OwnerCityCountryCount
    FROM Owner o
    JOIN Owns ow ON o.OwnerID = ow.OwnerID
    JOIN Apartment a ON ow.ApartmentID = a.ApartmentID
    GROUP BY o.OwnerID;
    """
    avg_nightly_price_view = """
    CREATE VIEW AvgNightlyPrices AS
    SELECT Reservation.ApartmentID, AVG(Reservation.Price / NULLIF(Reservation.EndDate - Reservation.StartDate, 0)) AS AvgNightlyPrice
    FROM Reservation
    GROUP BY Reservation.ApartmentID;
    """
    avg_apt_ratings_view = """
    CREATE OR REPLACE VIEW AvgAptRating AS 
    SELECT apt.ApartmentId, COALESCE(AVG(rv.Rating), 0) AS AvgRating
    FROM Apartment apt
    LEFT JOIN Review rv ON rv.ApartmentID = apt.ApartmentID
    GROUP BY apt.ApartmentID;
    """
    apt_value_for_money_view = """
    CREATE VIEW ApartmentValue AS
    SELECT ap.ApartmentID, (ar.AvgRating / ap.AvgNightlyPrice) AS Value
    FROM AvgNightlyPrices ap
    JOIN AvgAptRating ar ON ar.ApartmentID = ap.ApartmentID;
    """
    monthly_reservation_profits_view = """
    CREATE VIEW MonthlyReservationProfits AS
    SELECT EXTRACT(YEAR FROM EndDate) AS Year, EXTRACT(MONTH FROM EndDate) AS Month, SUM(Price * 0.15) AS Profit
    FROM Reservation
    GROUP BY Year, Month;
    """
    review_ratios_view = """
    CREATE VIEW RatingRatio AS
    SELECT rv1.CustomerID as CustomerID, rv2.CustomerID as OtherCustomerID, AVG(rv1.Rating::float / rv2.Rating) AS AvgRatio
    FROM Review rv1
    JOIN Review rv2 ON rv1.ApartmentID = rv2.ApartmentID AND rv1.CustomerID != rv2.customerID
    GROUP BY rv1.CustomerID, rv2.CustomerID
    """
    full_query = (
        create_customer_table
        + create_owner_table
        + create_apt_table
        + create_owns_table
        + create_reservation_table
        + create_review_table
        + apt_avg_rating_view
        + owner_apts_view
        + owner_avg_rating_view
        + customer_reservation_count_view
        + owner_reservation_count_view
        + uniq_CityCountry_count_view
        + owner_CityCountry_count_view
        + avg_nightly_price_view
        + avg_apt_ratings_view
        + apt_value_for_money_view
        + monthly_reservation_profits_view
        + review_ratios_view
    )
    conn.execute(full_query)
    conn.commit()
    conn.close()


def clear_tables():
    # Could potentially need to be DELETE instead of TRUNCATE
    clear_tables_query = """
    TRUNCATE TABLE Owns CASCADE;
    TRUNCATE TABLE Review CASCADE;
    TRUNCATE TABLE Reservation CASCADE;
    TRUNCATE TABLE Apartment CASCADE;
    TRUNCATE TABLE Customer CASCADE;
    TRUNCATE TABLE Owner CASCADE;
    """
    conn = Connector.DBConnector()
    conn.execute(clear_tables_query)
    conn.commit()
    conn.close()


def drop_tables():
    drop_tables_query = """
    DROP TABLE Owns CASCADE;
    DROP TABLE Review CASCADE;
    DROP TABLE Reservation CASCADE;
    DROP TABLE Apartment CASCADE;
    DROP TABLE Customer CASCADE;
    DROP TABLE Owner CASCADE;
    """
    conn = Connector.DBConnector()
    conn.execute(drop_tables_query)
    conn.commit()
    conn.close()


# Add an owner to the database
def add_owner(owner: Owner) -> ReturnValue:
    owner_id = owner.get_owner_id()
    owner_name = owner.get_owner_name()
    add_owner_query = sql.SQL(
        """
    INSERT INTO Owner (OwnerID, Name)
    VALUES ({owner_id}, {name})
    """
    ).format(owner_id=sql.Literal(owner_id), name=sql.Literal(owner_name))
    conn = Connector.DBConnector()
    try:
        conn.execute(add_owner_query)
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.commit()
    conn.close()
    return ReturnValue["OK"]


def get_owner(owner_id: int) -> Owner:  # Doron
    conn = Connector.DBConnector()
    get_owner_query = sql.SQL(
        """
    SELECT OwnerID, Name FROM Owner WHERE OwnerID = {owner_id}
    """
    ).format(owner_id=sql.Literal(owner_id))
    try:
        num_rows, result_set = conn.execute(get_owner_query)
    except exception_list:
        conn.close()
        return Owner.bad_owner()
    if num_rows < 1:
        conn.close()
        return Owner.bad_owner()
    conn.close()
    return create_owner_from_response(result_set[0])


# Delete an owner from the database.
def delete_owner(owner_id: int) -> ReturnValue:  # Daniel
    conn = Connector.DBConnector()
    delete_owner_query = sql.SQL(
        """
    DELETE FROM Owner WHERE OwnerID = {owner_id}
    """
    ).format(owner_id=sql.Literal(owner_id))
    try:
        affected_lines, _ = conn.execute(delete_owner_query)
        conn.commit()
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.close()
    if affected_lines < 1:
        if owner_id is None or owner_id <= 0:
            return ReturnValue["BAD_PARAMS"]
        return ReturnValue["NOT_EXISTS"]
    return ReturnValue["OK"]


# Add an apartment to the database.
def add_apartment(apartment: Apartment) -> ReturnValue:  # Doron
    conn = Connector.DBConnector()
    apartment_id = apartment.get_id()
    apartment_address = apartment.get_address()
    apartment_city = apartment.get_city()
    apartment_country = apartment.get_country()
    apartment_size = apartment.get_size()
    add_apartment_query = sql.SQL(
        """
    INSERT INTO Apartment (ApartmentID, Address, City, Country, Size)
    VALUES ({apartment_id}, {address}, {city}, {country}, {size})
    """
    ).format(
        apartment_id=sql.Literal(apartment_id),
        address=sql.Literal(apartment_address),
        city=sql.Literal(apartment_city),
        country=sql.Literal(apartment_country),
        size=sql.Literal(apartment_size),
    )
    try:
        conn.execute(add_apartment_query)
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.commit()
    conn.close()
    return ReturnValue["OK"]


# Get an apartment from the database.
def get_apartment(apartment_id: int) -> Apartment:  # Daniel
    conn = Connector.DBConnector()
    get_apt_query = sql.SQL(
        """
    SELECT * FROM Apartment WHERE ApartmentID = {apartment_id}
    """
    ).format(apartment_id=sql.Literal(apartment_id))
    try:
        num_rows, result_set = conn.execute(get_apt_query)
    except exception_list as e:
        conn.close()
        return Apartment.bad_apartment()
    if num_rows < 1:
        conn.close()
        return Apartment.bad_apartment()
    conn.close()
    apt_data = result_set[0]
    return create_apartment_from_response(apt_data)


# Delete an apartment from the database.
def delete_apartment(apartment_id: int) -> ReturnValue:  # Doron
    conn = Connector.DBConnector()
    delete_apartment_query = sql.SQL(
        """
    DELETE FROM Apartment WHERE {apartment_id} > 0 AND ApartmentID = {apartment_id}
    """
    ).format(apartment_id=sql.Literal(apartment_id))
    try:
        rows_affected, _ = conn.execute(delete_apartment_query)
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    if rows_affected != 1:
        conn.close()
        if apartment_id is None or apartment_id <= 0:
            return ReturnValue.BAD_PARAMS
        return ReturnValue["NOT_EXISTS"]
    conn.commit()
    conn.close()
    return ReturnValue["OK"]


# Add a customer to the database.
def add_customer(customer: Customer) -> ReturnValue:  # Daniel
    conn = Connector.DBConnector()
    customer_id = customer.get_customer_id()
    customer_name = customer.get_customer_name()
    add_customer_query = sql.SQL(
        """
    INSERT INTO Customer (CustomerID, Name)
    VALUES({customer_id}, {customer_name})
    """
    ).format(
        customer_id=sql.Literal(customer_id), customer_name=sql.Literal(customer_name)
    )
    try:
        conn.execute(add_customer_query)
        conn.commit()
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.close()
    return ReturnValue["OK"]


# Get a customer from the database.
def get_customer(customer_id: int) -> Customer:  # Doron
    conn = Connector.DBConnector()
    get_customer_query = sql.SQL(
        """
    SELECT CustomerID, Name FROM Customer WHERE CustomerID = {customer_id}
    """
    ).format(customer_id=sql.Literal(customer_id))
    rows, result_set = conn.execute(get_customer_query)
    conn.close()
    if rows < 1:
        return Customer.bad_customer()
    return create_customer_from_response(result_set[0])


# Delete a customer from the database.
def delete_customer(customer_id: int) -> ReturnValue:  # Daniel
    conn = Connector.DBConnector()
    delete_customer_query = sql.SQL(
        """
    DELETE FROM Customer WHERE {customer_id}>0 AND CustomerID = {customer_id}
    """
    ).format(customer_id=sql.Literal(customer_id))
    try:
        rows_affected, _ = conn.execute(delete_customer_query)
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.close()
    if rows_affected < 1:
        if customer_id <= 0 or customer_id is None:
            return ReturnValue["BAD_PARAMS"]
        return ReturnValue["NOT_EXISTS"]
    return ReturnValue["OK"]


# Customer made a reservation of apartment from start_date to end_date and paid total_price
def customer_made_reservation(
    customer_id: int,
    apartment_id: int,
    start_date: date,
    end_date: date,
    total_price: float,
) -> ReturnValue:  # Doron
    conn = Connector.DBConnector()
    customer_made_reservation_query = sql.SQL(
        """
    INSERT INTO Reservation (CustomerID, ApartmentID, StartDate, EndDate, Price) 
    SELECT {customer_id}, {apartment_id}, {start_date}, {end_date}, {total_price}
    WHERE {apartment_id} > 0
    AND {customer_id} > 0
    AND NOT EXISTS (
        SELECT * FROM Reservation
        WHERE ApartmentId = {apartment_id} 
        AND StartDate < {end_date} AND EndDate > {start_date}
    );
    """
    ).format(
        customer_id=sql.Literal(customer_id),
        apartment_id=sql.Literal(apartment_id),
        start_date=sql.Literal(start_date),
        end_date=sql.Literal(end_date),
        total_price=sql.Literal(total_price),
    )
    try:
        rows_affected, _ = conn.execute(customer_made_reservation_query)
        if rows_affected == 0:
            conn.close()
            return ReturnValue.BAD_PARAMS
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.commit()
    conn.close()
    return ReturnValue["OK"]


# Remove a reservation from the database.
def customer_cancelled_reservation(
    customer_id: int, apartment_id: int, start_date: date
) -> ReturnValue:  # Daniel
    conn = Connector.DBConnector()
    customer_cancelled_reservation_query = sql.SQL(
        """
    DELETE FROM Reservation WHERE CustomerID = {customer_id} AND ApartmentID = {apartment_id} AND StartDate = {start_date}
    """
    ).format(
        customer_id=sql.Literal(customer_id),
        apartment_id=sql.Literal(apartment_id),
        start_date=sql.Literal(start_date),
    )
    try:
        rows_affected, _ = conn.execute(customer_cancelled_reservation_query)
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.commit()
    conn.close()
    if rows_affected != 1:
        if (
            customer_id is None
            or customer_id <= 0
            or apartment_id is None
            or apartment_id <= 0
        ):
            return ReturnValue["BAD_PARAMS"]
        return ReturnValue["NOT_EXISTS"]
    return ReturnValue["OK"]


# Customer reviewed apartment on date review_date and gave it rating stars, with text review_text.
def customer_reviewed_apartment(
    customer_id: int,
    apartment_id: int,
    review_date: date,
    rating: int,
    review_text: str,
) -> ReturnValue:  # Doron
    conn = Connector.DBConnector()
    customer_reviewed_apartment_query = sql.SQL(
        """
    Insert into Review (CustomerID, ApartmentID, ReviewDate, Rating, ReviewText)
    SELECT {customer_id}, {apartment_id}, {review_date}, {rating}, {review_text}
    WHERE EXISTS (
        SELECT * FROM Reservation
        WHERE CustomerID = {customer_id}
        AND ApartmentID = {apartment_id}
        AND (EndDate <= {review_date})
    );
    """
    ).format(
        customer_id=sql.Literal(customer_id),
        apartment_id=sql.Literal(apartment_id),
        review_date=sql.Literal(review_date),
        rating=sql.Literal(rating),
        review_text=sql.Literal(review_text),
    )
    try:
        rows_affected, _ = conn.execute(customer_reviewed_apartment_query)
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.commit()
    conn.close()
    if rows_affected < 1:
        if (
            rating is None
            or rating < 1
            or rating > 10
            or apartment_id is None
            or apartment_id <= 0
            or customer_id is None
            or customer_id <= 0
        ):
            return ReturnValue["BAD_PARAMS"]
        return ReturnValue["NOT_EXISTS"]
    return ReturnValue["OK"]


# Customer decided to update their review of apartment on update_date and changed his rating to new_rating and the review text to new_text
def customer_updated_review(
    customer_id: int,
    apartment_id: int,
    update_date: date,
    new_rating: int,
    new_text: str,
) -> ReturnValue:  # Daniel
    conn = Connector.DBConnector()
    customer_updated_review_query = sql.SQL(
        """
    UPDATE Review 
    SET reviewdate = {update_date}, rating = {new_rating}, reviewtext = {new_text}
    WHERE CustomerID = {customer_id} AND ApartmentID = {apartment_id} AND reviewdate<={update_date}
    """
    ).format(
        update_date=sql.Literal(update_date),
        new_rating=sql.Literal(new_rating),
        new_text=sql.Literal(new_text),
        customer_id=sql.Literal(customer_id),
        apartment_id=sql.Literal(apartment_id),
    )
    try:
        rows_affected, _ = conn.execute(customer_updated_review_query)
        if rows_affected == 0:
            if (
                customer_id is None
                or customer_id <= 0
                or apartment_id is None
                or apartment_id <= 0
                or new_rating is None
                or new_rating < 1
                or new_rating > 10
            ):
                return ReturnValue["BAD_PARAMS"]
            return ReturnValue["NOT_EXISTS"]
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.commit()
    conn.close()
    return ReturnValue["OK"]


# Owner owns apartment. An apartment can be owned by at most one owner.
def owner_owns_apartment(owner_id: int, apartment_id: int) -> ReturnValue:  # Doron
    conn = Connector.DBConnector()
    owner_owns_apartment_query = sql.SQL(
        """
    INSERT INTO Owns (OwnerID, ApartmentID)
    VALUES ({owner_id}, {apartment_id})
    """
    ).format(
        owner_id=sql.Literal(owner_id),
        apartment_id=sql.Literal(apartment_id),
    )
    try:
        conn.execute(owner_owns_apartment_query)
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.commit()
    conn.close()
    return ReturnValue["OK"]


# Owner dropped apartment and does not own it anymore.
def owner_drops_apartment(owner_id: int, apartment_id: int) -> ReturnValue:  # Daniel
    conn = Connector.DBConnector()
    owner_drops_apartment_query = sql.SQL(
        """
    DELETE FROM Owns
    WHERE OwnerID = {owner_id} AND ApartmentID = {apartment_id}
    """
    ).format(owner_id=sql.Literal(owner_id), apartment_id=sql.Literal(apartment_id))
    try:
        rows_affected, _ = conn.execute(owner_drops_apartment_query)
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.commit()
    conn.close()
    if rows_affected != 1:
        if (
            owner_id is None
            or owner_id <= 0
            or apartment_id is None
            or apartment_id <= 0
        ):
            return ReturnValue["BAD_PARAMS"]
        return ReturnValue["NOT_EXISTS"]
    return ReturnValue["OK"]


# Get the owner of apartment.
def get_apartment_owner(apartment_id: int) -> Owner:  # Doron
    conn = Connector.DBConnector()
    get_apartment_owner_query = sql.SQL(
        """
    SELECT o.OwnerID, o.Name
    FROM Owner o
    JOIN Owns os ON o.OwnerID = os.OwnerID
    WHERE os.ApartmentID = {apartment_id}
    """
    ).format(apartment_id=sql.Literal(apartment_id))
    try:
        num_rows, result_set = conn.execute(get_apartment_owner_query)
    except exception_list as e:
        conn.close()
        return Owner.bad_owner()
    conn.close()
    if num_rows < 1:
        return Owner.bad_owner()
    return create_owner_from_response(result_set[0])


# Get a list of all apartments owned by owner.
def get_owner_apartments(owner_id: int) -> List[Apartment]:  # Daniel
    conn = Connector.DBConnector()
    get_owner_apartments_query = sql.SQL(
        """
    SELECT *
    FROM OwnerApartments    
    WHERE OwnerID = {owner_id}                      
    """
    ).format(owner_id=sql.Literal(owner_id))
    try:
        num_apts, apts_data = conn.execute(get_owner_apartments_query)
    except exception_list as e:
        conn.close()
        return [Apartment.bad_apartment()]
    conn.close()
    if num_apts < 1:
        return []
    apts = [create_apartment_from_response(apt_data) for apt_data in apts_data]
    return apts


# ---------------------------------- BASIC API: ----------------------------------


# Get the average rating across all reviews of apartment.
def get_apartment_rating(apartment_id: int) -> float:
    conn = Connector.DBConnector()
    get_apartment_rating_query = sql.SQL(
        """
    SELECT AvgRating FROM Ratings WHERE ApartmentID = {apartment_id}
    """
    ).format(apartment_id=sql.Literal(apartment_id))
    try:
        rows, result = conn.execute(get_apartment_rating_query)
    except exception_list as e:
        conn.close()
        return handle_errors(e)
    conn.close()
    if rows < 1:
        return 0
    return result[0]["AvgRating"]


# Get the average of averages of ratings from all reviews of apartments owned by owner.
def get_owner_rating(owner_id: int) -> float:
    conn = Connector.DBConnector()
    get_owner_rating_query = sql.SQL(
        """
SELECT COALESCE(AVG(COALESCE(r.AvgRating, 0)), 0) AS AvgRating
FROM Owner o
LEFT JOIN OwnerApartments oa ON o.OwnerID = oa.OwnerID
LEFT JOIN Ratings r ON oa.ApartmentID = r.ApartmentID
WHERE o.OwnerID = {owner_id}
    """
    ).format(owner_id=sql.Literal(owner_id))
    try:
        rows, result = conn.execute(get_owner_rating_query)
    except exception_list as e:
        conn.rollback()
        conn.close()
        return handle_errors(e)
    conn.close()
    if rows < 1:
        return 0
    return result[0]["AvgRating"]


# Get the customer that made the most reservations.
def get_top_customer() -> Customer:
    conn = Connector.DBConnector()
    try:
        get_top_customer_query = sql.SQL(
        """
        SELECT c.CustomerID, c.Name
        FROM Customer c
        JOIN (
            SELECT CustomerID, Reservations
            FROM CustomerReservations
            ORDER BY Reservations DESC, CustomerID ASC
            LIMIT 1
        ) rc ON c.CustomerID = rc.CustomerID;
        """
        )
        rows, result = conn.execute(get_top_customer_query)
    except Exception as e:
        conn.close()
        return Customer.bad_customer()
    if rows < 1:
        conn.close()
        return Customer.bad_customer()
    conn.close()
    return Customer(customer_id=result.rows[0][0], customer_name=result.rows[0][1])


# Output: a list of tuples of (owner_name, total_reservation_count) of all owners in the database.
def reservations_per_owner() -> List[Tuple[str, int]]:
    conn = Connector.DBConnector()
    try:
        reservations_per_owner_query = sql.SQL(
            """
SELECT o.Name AS owner_name, COALESCE(SUM(r.ReservationCount), 0) AS total_reservation_count
FROM Owner o
LEFT JOIN OwnerReservation r ON o.Name = r.Name
GROUP BY o.Name;
        """
        )
        _, resultSet = conn.execute(reservations_per_owner_query)
        if resultSet.isEmpty():
            conn.close()
            return []
        owners = []
        for row in resultSet.rows:
            owners.append((row[0], row[1]))
        conn.close()
        return owners
    except Exception as e:
        conn.close()
        return []


# ---------------------------------- ADVANCED API: ----------------------------------


# Return all owners that own an apartment in every city there are apartments in.
def get_all_location_owners() -> List[Owner]:
    conn = Connector.DBConnector()
    try:
        query = sql.SQL(
        """
        SELECT o.OwnerID, o.Name
        FROM OwnerCityCountryCount o, TotalCityCountryCount
        WHERE o.OwnerCityCountryCount = TotalCityCountryCount.TotalCityCountryCount;
        """
        )
        _, resultSet = conn.execute(query)
        if resultSet.isEmpty():
            conn.close()
            return []
        owners = [create_owner_from_response(owner) for owner in resultSet]
        conn.close()
        return owners
    except Exception as e:
        conn.close()
        return []


# Get the apartment that has the best reviews compared to its average nightly price.
def best_value_for_money() -> Apartment:
    conn = Connector.DBConnector()
    try:
        query = sql.SQL(
        """
        SELECT apt.*
        FROM Apartment apt
        JOIN ApartmentValue av ON av.ApartmentID = apt.ApartmentID
        ORDER BY av.Value DESC
        LIMIT 1;
        """
        )
        rows_effected, result_set = conn.execute(query)
        if result_set.isEmpty():
            return Apartment.bad_apartment()
        conn.close()
        return create_apartment_from_response(result_set[0])

    except Exception as e:
        conn.close()
        return Apartment.bad_apartment()


def profit_per_month(year: int) -> List[Tuple[int, float]]:
    conn = Connector.DBConnector()
    try:
        query = sql.SQL(
        """
        WITH MonthSeries AS (SELECT generate_series(1, 12) AS Month)
        SELECT MS.Month, COALESCE(MRP.Profit, 0) AS Profit
        FROM MonthSeries MS
        LEFT JOIN MonthlyReservationProfits MRP ON MS.Month = MRP.Month AND MRP.Year = {year}
        ORDER BY MS.Month;
        """
        ).format(year=sql.Literal(year))
        rows_effected, resultSet = conn.execute(query)
        if resultSet.isEmpty():
            return []
        profits = []
        for row in resultSet.rows:
            profits.append((row[0], row[1]))
        return profits
    except Exception as e:
        return []
    finally:
        conn.close()


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
    conn = Connector.DBConnector()
    try:
        query = sql.SQL(
        """
        SELECT apt.*, 
            AVG(LEAST(10, GREATEST(1, rv.Rating * (SELECT avgRatio FROM RatingRatio rt WHERE rt.CustomerID = {customer_id} AND rt.OtherCustomerID = rv.CustomerID)))) AS PredictedRating
        FROM Apartment apt
        JOIN Review rv ON rv.ApartmentID = apt.ApartmentID AND rv.CustomerID != {customer_id}
        WHERE NOT EXISTS (
            SELECT 1
            FROM Reservation res
            WHERE res.CustomerID = {customer_id} AND res.ApartmentID = apt.ApartmentID
        )
        GROUP BY apt.ApartmentID
        """).format(customer_id=sql.Literal(customer_id))
        rows_effected, resultSet = conn.execute(query)
        return [(create_apartment_from_response(row), row["PredictedRating"]) for row in resultSet]

    except Exception as e:
        print(f"An error occurred: {e}")
        return []

    finally:
        conn.close()


# Utility functions:


def create_owner_from_response(res: ResultSetDict) -> Owner:
    try:
        return Owner(res["OwnerID"], res["Name"])
    except KeyError:
        return Owner.bad_owner()


def create_customer_from_response(res: ResultSetDict) -> Customer:
    try:
        return Customer(res["CustomerID"], res["Name"])
    except KeyError:
        return Customer.bad_customer()


def create_apartment_from_response(res: ResultSetDict) -> Apartment:
    try:
        return Apartment(
            id=res["ApartmentID"],
            address=res["Address"],
            city=res["City"],
            country=res["Country"],
            size=res["Size"],
        )
    except KeyError:
        return Apartment.bad_apartment()


def handle_errors(e: DatabaseException):
    e_name = e.__str__()
    # print(f"handling error: {e_name}")
    if e_name == "FOREIGN_KEY_VIOLATION":
        return ReturnValue["NOT_EXISTS"]
    if e_name == "UNIQUE_VIOLATION":
        return ReturnValue["ALREADY_EXISTS"]
    elif (
        e_name == "NOT_NULL_VIOLATION"
        or e_name == "FOREIGN_KEY_VIOLATION"
        or e_name == "CHECK_VIOLATION"
    ):
        return ReturnValue["BAD_PARAMS"]
    elif (
        e_name == "database_ini_ERROR"
        or e_name == "UNKNOWN_ERROR"
        or e_name == "ConnectionInvalid"
    ):
        return ReturnValue["ERROR"]


exception_list = (
    DatabaseException.ConnectionInvalid,
    DatabaseException.NOT_NULL_VIOLATION,
    DatabaseException.FOREIGN_KEY_VIOLATION,
    DatabaseException.UNIQUE_VIOLATION,
    DatabaseException.CHECK_VIOLATION,
    DatabaseException.database_ini_ERROR,
    DatabaseException.UNKNOWN_ERROR,
)
