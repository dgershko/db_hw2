import unittest
from Solution import *
from Utility.ReturnValue import ReturnValue
from datetime import date, datetime
import time


class TestCRUD(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        create_tables()

    # def setUp(self):
    # This method will be called before each test
    # Set up your test environment

    def tearDown(self):
        # This method will be called after each test
        # Clean up your test environment
        clear_tables()

    def test_owner(self):
        print("Running Test: test_owner...")
        self.assertEqual(add_owner(Owner(1, 'Allan')), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(4, 'Jacob')), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(3, 'Allan')), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(1, 'Lia')), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(add_owner(Owner(1, None)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_owner(Owner(-1, 'Lala')), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_owner(Owner(None, 'Hal')), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_owner(Owner(0, 'Ak')), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_owner(Owner(2, None)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_owner(Owner(10, '')), ReturnValue.OK)

        self.assertEqual(get_owner(1), Owner(1, 'Allan'))
        self.assertEqual(get_owner(3), Owner(3, 'Allan'))
        self.assertEqual(get_owner(4), Owner(4, 'Jacob'))
        self.assertEqual(get_owner(2), Owner.bad_owner())
        self.assertEqual(get_owner(-1), Owner.bad_owner())
        self.assertEqual(get_owner(0), Owner.bad_owner())
        self.assertEqual(get_owner(5), Owner.bad_owner())

        self.assertEqual(delete_owner(1), ReturnValue.OK)
        self.assertEqual(delete_owner(1), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_owner(-1), ReturnValue.BAD_PARAMS)
        self.assertEqual(delete_owner(0), ReturnValue.BAD_PARAMS)
        self.assertEqual(delete_owner(2), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_owner(3), ReturnValue.OK)
        print("// ==== test_owner: SUCCESS! ==== //")

    def test_apartment(self):
        print("Running Test: test_apartment...")
        self.assertEqual(add_apartment(Apartment(1, "Nosh", "Haifa", "ISR", 150)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(2, "Nosh", "Haifa", "ISR", 150)), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(add_apartment(Apartment(1, "Nour", "Nah", "Port", 120)), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(add_apartment(Apartment(2, "Marv", "Nah", "ISR", 150)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(-1, "Nosh", "Haifa", "ISR", 150)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_apartment(Apartment(0, "Nosh", "Haifa", "ISR", 10)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_apartment(Apartment(3, "Nosh", "Haifa", "ISR", 0)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_apartment(Apartment(3, "Nosh", "Haifa", "ISR", -10)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_apartment(Apartment(4, None, "Haifa", "ISR", 150)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_apartment(Apartment(5, "Nosh", None, "ISR", 150)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_apartment(Apartment(6, "Nosh", "Haifa", None, 150)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_apartment(Apartment(3, "Noar", "Hai", "Port", 0)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_apartment(Apartment(10, "", "", "", 2323)), ReturnValue.OK)

        self.assertEqual(get_apartment(1), Apartment(1, "Nosh", "Haifa", "ISR", 150))
        self.assertEqual(get_apartment(2), Apartment(2, "Marv", "Nah", "ISR", 150))
        self.assertEqual(get_apartment(0), Apartment.bad_apartment())
        self.assertEqual(get_apartment(-1), Apartment.bad_apartment())
        self.assertEqual(get_apartment(3), Apartment.bad_apartment())
        self.assertEqual(get_apartment(4), Apartment.bad_apartment())
        self.assertEqual(get_apartment(5), Apartment.bad_apartment())
        self.assertEqual(get_apartment(6), Apartment.bad_apartment())
        self.assertEqual(get_apartment(7), Apartment.bad_apartment())

        self.assertEqual(delete_apartment(1), ReturnValue.OK)
        self.assertEqual(delete_apartment(1), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_apartment(0), ReturnValue.BAD_PARAMS)
        self.assertEqual(delete_apartment(-1), ReturnValue.BAD_PARAMS)
        self.assertEqual(delete_apartment(3), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_apartment(2), ReturnValue.OK)
        self.assertEqual(delete_apartment(2), ReturnValue.NOT_EXISTS)

        print("// ==== test_apartment: SUCCESS! ==== //")

    def test_customer(self):
        print("Running Test: test_customer...")
        self.assertEqual(add_customer(Customer(2, "Allan")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(1, "Someone")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(2, "Else")), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(add_customer(Customer(2, None)), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_customer(Customer(None, "Who")), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_customer(Customer(-1, "Who")), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_customer(Customer(0, "Who")), ReturnValue.BAD_PARAMS)
        self.assertEqual(add_customer(Customer(3, "Allan")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(10, "")), ReturnValue.OK)

        self.assertEqual(get_customer(1), Customer(1, "Someone"))
        self.assertEqual(get_customer(2), Customer(2, "Allan"))
        self.assertEqual(get_customer(3), Customer(3, "Allan"))
        self.assertEqual(get_customer(4), Customer.bad_customer())
        self.assertEqual(get_customer(0), Customer.bad_customer())
        self.assertEqual(get_customer(-1), Customer.bad_customer())

        self.assertEqual(delete_customer(1), ReturnValue.OK)
        self.assertEqual(delete_customer(1), ReturnValue.NOT_EXISTS)
        self.assertEqual(delete_customer(0), ReturnValue.BAD_PARAMS)
        self.assertEqual(delete_customer(-2), ReturnValue.BAD_PARAMS)
        self.assertEqual(delete_customer(2), ReturnValue.OK)
        self.assertEqual(delete_customer(2), ReturnValue.NOT_EXISTS)

        print("// ==== test_customer: SUCCESS! ==== //")

    def test_customer_reservation_review(self):
        print("Running Test: test_customer_reservation_review...")
        self.assertEqual(add_apartment(Apartment(1, "Nosh", "Haifa", "ISR", 150)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(2, "Marv", "Nah", "ISR", 150)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(3, "Marlis", "Nahra", "ISR", 150)), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(2, "Allan")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(1, "Someone")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(3, "else")), ReturnValue.OK)

        d1 = date(2023, 4, 1)
        d2 = date(2023, 5, 1)
        d3 = date(2023, 5, 20)
        d4 = date(2024, 5, 10)
        d5 = date(2026, 5, 18)
        d6 = date(2026, 5, 20)
        d7 = date(2026, 5, 25)
        d8 = date(2026, 5, 26)

        td1 = date(2023, 3, 20)
        td2 = date(2023, 4, 2)
        td3 = date(2023, 4, 25)

        td4 = date(2024, 5, 5)
        td5 = date(2024, 5, 15)

        vd1 = date(2025, 5, 1)
        vd2 = date(2025, 6, 1)
        self.assertEqual(customer_made_reservation(2, 1, d1, d2, 1500), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(2, 1, d3, d4, 14000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(2, 1, d2, d3, 1500), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(2, 2, d1, d2, 1200), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(2, 2, d3, d4, 11000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(1, 2, d2, d3, 11000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(3, 1, td1, td2, 1700), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(3, 1, td2, td3, 1700), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(3, 1, td4, td5, 1700), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(-1, 1, vd1, vd2, 1700), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(0, 1, vd1, vd2, 1700), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(1, 0, vd1, vd2, 1700), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(1, -1, vd1, vd2, 1700), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(1, 1, vd1, vd2, 0), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(1, 1, vd1, vd2, -5), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(4, 1, vd1, vd2, -5), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(4, 1, vd1, vd2, 10), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_made_reservation(3, 5, vd1, vd2, -5), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_made_reservation(3, 5, vd1, vd2, 10), ReturnValue.NOT_EXISTS)

        self.assertEqual(customer_reviewed_apartment(2, 1, td1, 10, "AWESOME"), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_reviewed_apartment(2, 1, td3, 10, "AWESOME"), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_reviewed_apartment(2, 1, d2, 10, "AWESOME"),ReturnValue.OK)  # what about empty string ?
        self.assertEqual(customer_reviewed_apartment(2, 2, d2, 1, "TOOOOO bad!!"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(1, 2, d3, 5, "fine"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(2, 1, d2, 8, "Not bad!!"), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(customer_reviewed_apartment(2, 1, d3, 7, "something else"), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(customer_reviewed_apartment(3, 3, vd2, 7, "something else"), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_reviewed_apartment(3, 4, vd2, 7, "something else"), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_reviewed_apartment(4, 3, vd2, 7, "something else"), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_reviewed_apartment(3, 3, vd2, 0, "something else"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_reviewed_apartment(3, 3, vd2, 11, "something else"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_reviewed_apartment(3, 3, vd2, -5, "something else"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_reviewed_apartment(0, 3, vd2, 2, "something else"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_reviewed_apartment(2, 0, vd2, 2, "something else"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_reviewed_apartment(-5, 3, vd2, 2, "something else"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_reviewed_apartment(2, -3, vd2, 2, "something else"), ReturnValue.BAD_PARAMS)

        self.assertEqual(add_customer(Customer(4, "kk")), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(4, 1, d6, d7, 1500), ReturnValue.OK)
        self.assertEqual(customer_updated_review(2, 1, d1, 10, "OK"), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_updated_review(2, 1, d2, 9, "OK."), ReturnValue.OK)
        self.assertEqual(customer_updated_review(2, 1, d3, 1, "OPS..."), ReturnValue.OK)
        self.assertEqual(customer_updated_review(2, 1, d3, 7, "OK..."), ReturnValue.OK)
        self.assertEqual(customer_updated_review(2, 1, d4, 8, "OK.."), ReturnValue.OK)
        self.assertEqual(customer_updated_review(2, 1, d5, 10, "OK"), ReturnValue.OK)
        self.assertEqual(customer_updated_review(2, 1, d4, 10, "OK"), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_updated_review(4, 1, d8, 10, "OK"), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_reviewed_apartment(4, 1, d7, 2, "EH"), ReturnValue.OK)
        self.assertEqual(customer_updated_review(4, 1, d8, 10, "OK"), ReturnValue.OK)
        self.assertEqual(customer_updated_review(4, 1, d7, 10, "OK"), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_updated_review(2, 2, d3, 11, "OK"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_updated_review(2, 2, d3, 0, "OK"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_updated_review(2, 2, d3, -2, "OK"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_updated_review(2, 0, d3, 1, "OK"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_updated_review(0, 2, d3, 6, "OK"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_updated_review(2, -1, d3, 1, "OK"), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_updated_review(-5, 2, d3, 6, "OK"), ReturnValue.BAD_PARAMS)

        self.assertEqual(customer_cancelled_reservation(2, 1, d2), ReturnValue.OK)
        self.assertEqual(customer_cancelled_reservation(2, 1, d2), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_cancelled_reservation(4, 2, d1), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_cancelled_reservation(4, 6, d1), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_cancelled_reservation(6, 1, d1), ReturnValue.NOT_EXISTS)
        self.assertEqual(customer_cancelled_reservation(0, 1, d1), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_cancelled_reservation(-1, 1, d1), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_cancelled_reservation(0, 1, d1), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_cancelled_reservation(2, -1, d1), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_cancelled_reservation(2, 0, d1), ReturnValue.BAD_PARAMS)
        self.assertEqual(customer_cancelled_reservation(2, 2, d1), ReturnValue.OK)
        self.assertEqual(customer_cancelled_reservation(2, 1, d3), ReturnValue.OK)
        self.assertEqual(customer_cancelled_reservation(2, 1, d1), ReturnValue.OK)

        print("// ==== test_customer_reservation_review: SUCCESS! ==== //")

    def test_owner_apartment(self):

        print("Running Test: test_owner_apartment...")

        self.assertEqual(add_apartment(Apartment(1, "Nosh", "Haifa", "ISR", 150)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(2, "Marv", "Nah", "ISR", 150)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(3, "Marlis", "Nahra", "ISR", 150)), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(2, "Allan")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(1, "Someone")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(3, "else")), ReturnValue.OK)

        self.assertEqual(owner_owns_apartment(1, 1), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(2, 1), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(owner_owns_apartment(1, 4), ReturnValue.NOT_EXISTS)
        self.assertEqual(owner_owns_apartment(0, 1), ReturnValue.BAD_PARAMS)
        self.assertEqual(owner_owns_apartment(-1, 1), ReturnValue.BAD_PARAMS)
        self.assertEqual(owner_owns_apartment(1, -1), ReturnValue.BAD_PARAMS)
        self.assertEqual(owner_owns_apartment(1, 0), ReturnValue.BAD_PARAMS)
        self.assertEqual(owner_owns_apartment(1, 2), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(3, 2), ReturnValue.ALREADY_EXISTS)
        self.assertEqual(owner_owns_apartment(2, 3), ReturnValue.OK)

        self.assertEqual(add_apartment(Apartment(4, "sad", "f", "ISR", 150)), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(4, "none")), ReturnValue.OK)
        self.assertEqual(get_apartment_owner(1), Owner(1, "Someone"))
        self.assertEqual(get_apartment_owner(2), Owner(1, "Someone"))
        self.assertEqual(get_apartment_owner(3), Owner(2, "Allan"))
        self.assertEqual(get_apartment_owner(4), Owner.bad_owner())
        self.assertEqual(get_apartment_owner(0), Owner.bad_owner())
        self.assertEqual(get_apartment_owner(-1), Owner.bad_owner())
        self.assertEqual(get_apartment_owner(5), Owner.bad_owner())

        self.assertEqual(get_owner_apartments(1),
                         [Apartment(1, "Nosh", "Haifa", "ISR", 150), Apartment(2, "Marv", "Nah", "ISR", 150)])
        self.assertEqual(get_owner_apartments(2), [Apartment(3, "Marlis", "Nahra", "ISR", 150)])
        self.assertEqual(get_owner_apartments(3), [])
        self.assertEqual(get_owner_apartments(4), [])
        self.assertEqual(get_owner_apartments(5), [])
        self.assertEqual(get_owner_apartments(0), [])
        self.assertEqual(get_owner_apartments(-1), [])

        self.assertEqual(owner_drops_apartment(1, 1), ReturnValue.OK)
        self.assertEqual(get_owner_apartments(1), [Apartment(2, "Marv", "Nah", "ISR", 150)])
        self.assertEqual(get_apartment_owner(1), Owner.bad_owner())
        self.assertEqual(owner_drops_apartment(1, 1), ReturnValue.NOT_EXISTS)
        self.assertEqual(owner_drops_apartment(5, 1), ReturnValue.NOT_EXISTS)
        self.assertEqual(owner_drops_apartment(1, 5), ReturnValue.NOT_EXISTS)
        self.assertEqual(owner_drops_apartment(0, 1), ReturnValue.BAD_PARAMS)
        self.assertEqual(owner_drops_apartment(-1, 1), ReturnValue.BAD_PARAMS)
        self.assertEqual(owner_drops_apartment(1, -1), ReturnValue.BAD_PARAMS)
        self.assertEqual(owner_drops_apartment(1, 0), ReturnValue.BAD_PARAMS)

        print("// ==== test_owner_apartment: SUCCESS! ==== //")

    def test_BASIC_API(self):
        print("Running Test: test_BASIC_API...")
        reservationsPerOwner = reservations_per_owner()
        self.assertEqual(reservationsPerOwner, [])
        self.assertEqual(add_owner(Owner(1, "OA")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(2, "OB")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(3, "OC")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(4, "OD")), ReturnValue.OK)
        reservationsPerOwner = reservations_per_owner()
        self.assertNotEqual(reservationsPerOwner, [])
        for owner_name, reservations_num in reservationsPerOwner:
            if owner_name == "OA":
                self.assertEqual(reservations_num, 0)
            if owner_name == "OB":
                self.assertEqual(reservations_num, 0)
            if owner_name == "OC":
                self.assertEqual(reservations_num, 0)
            if owner_name == "OD":
                self.assertEqual(reservations_num, 0)
        self.assertEqual(add_customer(Customer(12, "CA")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(13, "CB")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(14, "CC")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(15, "CD")), ReturnValue.OK)
        reservationsPerOwner = reservations_per_owner()
        self.assertNotEqual(reservationsPerOwner, [])
        for owner_name, reservations_num in reservationsPerOwner:
            if owner_name == "OA":
                self.assertEqual(reservations_num, 0)
            if owner_name == "OB":
                self.assertEqual(reservations_num, 0)
            if owner_name == "OC":
                self.assertEqual(reservations_num, 0)
            if owner_name == "OD":
                self.assertEqual(reservations_num, 0)
        self.assertEqual(add_apartment(Apartment(5, "RA", "RA", "RA", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(6, "RB", "RB", "RB", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(7, "RC", "RC", "RC", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(8, "RD", "RD", "RD", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(9, "RE", "RE", "RE", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(10, "RF", "RF", "RF", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(11, "RG", "RG", "RG", 80)), ReturnValue.OK)
        reservationsPerOwner = reservations_per_owner()
        self.assertNotEqual(reservationsPerOwner, [])
        for owner_name, reservations_num in reservationsPerOwner:
            if owner_name == "OA":
                self.assertEqual(reservations_num, 0)
            if owner_name == "OB":
                self.assertEqual(reservations_num, 0)
            if owner_name == "OC":
                self.assertEqual(reservations_num, 0)
            if owner_name == "OD":
                self.assertEqual(reservations_num, 0)
        self.assertEqual(owner_owns_apartment(1, 5), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(1, 6), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(1, 7), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(2, 8), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(2, 9), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(2, 10), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(3, 11), ReturnValue.OK)
        reservationsPerOwner = reservations_per_owner()
        self.assertNotEqual(reservationsPerOwner, [])
        d1 = date(2023, 4, 1)
        d2 = date(2023, 5, 1)
        d3 = date(2023, 5, 10)
        d4 = date(2023, 5, 20)
        self.assertEqual(customer_made_reservation(12, 5, d1, d2, 5000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(12, 7, d2, d3, 5000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(12, 9, d1, d2, 5000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(13, 6, d1, d2, 5000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(13, 8, d3, d4, 5000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(13, 5, d2, d3, 5000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(14, 11, d1, d2, 5000), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(12, 5, d4, 5, "Ok"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(12, 7, d4, 8, "Ok"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(12, 9, d4, 9, "Ok"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(13, 5, d4, 7, "Ok"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(13, 6, d4, 10, "Ok"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(13, 8, d4, 6, "Ok"), ReturnValue.OK)
        self.assertEqual(get_apartment_rating(5), 6)
        self.assertEqual(get_apartment_rating(6), 10)
        self.assertEqual(get_apartment_rating(7), 8)
        self.assertEqual(get_apartment_rating(8), 6)
        self.assertEqual(get_apartment_rating(9), 9)
        self.assertEqual(get_apartment_rating(10), 0)
        self.assertEqual(get_apartment_rating(11), 0)
        self.assertEqual(get_owner_rating(1), 8)
        self.assertEqual(get_owner_rating(2), 5)
        self.assertEqual(get_owner_rating(3), 0)
        self.assertEqual(get_owner_rating(4), 0)
        self.assertEqual(get_top_customer(), Customer(12, "CA"))
        self.assertEqual(customer_made_reservation(13, 11, d3, d4, 5000), ReturnValue.OK)
        self.assertEqual(get_top_customer(), Customer(13, "CB"))
        self.assertEqual(add_owner(Owner(20, "OE")), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(21, "RH", "RH", "RH", 80)), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(22, "CE")), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(20, 21), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(22, 21, d1, d2, 1050), ReturnValue.OK)
        reservationsPerOwner = reservations_per_owner()
        self.assertNotEqual(reservationsPerOwner, [])
        for owner_name, reservations_num in reservationsPerOwner:
            if owner_name == "OA":
                self.assertEqual(reservations_num, 4)
            if owner_name == "OB":
                self.assertEqual(reservations_num, 2)
            if owner_name == "OC":
                self.assertEqual(reservations_num, 2)
            if owner_name == "OD":
                self.assertEqual(reservations_num, 0)
            if owner_name == "OE":
                self.assertEqual(reservations_num, 1)
        print("// ==== test_BASIC_API: SUCCESS! ==== //")

    def test_Advanced_API(self):
        print("Running Test: test_Advanced_API...")
        self.assertEqual(add_owner(Owner(1, "OA")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(2, "OB")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(3, "OC")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(4, "OD")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(12, "CA")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(13, "CB")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(14, "CC")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(15, "CD")), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(5, "RA", "Haifa", "ISR", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(6, "RB", "Haifa", "ISR", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(7, "RC", "Akko", "ISR", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(8, "RD", "Nahariya", "ISR", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(9, "RE", "Haifa", "Canada", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(10, "RF", "Akko", "Canada", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(11, "RG", "Toronto", "Canada", 80)), ReturnValue.OK)  #

        self.assertEqual(get_all_location_owners(), [])
        self.assertEqual(owner_owns_apartment(1, 5), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [])
        self.assertEqual(owner_owns_apartment(1, 7), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [])
        self.assertEqual(owner_owns_apartment(1, 8), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [])
        self.assertEqual(owner_owns_apartment(1, 11), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [])
        self.assertEqual(owner_owns_apartment(1, 10), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [])
        self.assertEqual(owner_owns_apartment(1, 9), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [Owner(1, "OA")])

        self.assertEqual(add_apartment(Apartment(20, "RH", "Toronto", "Canada", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(21, "RI", "Akko", "Canada", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(22, "RJ", "Akko", "Canada", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(23, "Rk", "Nahariya", "ISR", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(24, "RL", "Haifa", "Canada", 80)), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(25, "RM", "Akko", "ISR", 80)), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(2, 6), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [Owner(1, "OA")])
        self.assertEqual(owner_owns_apartment(2, 20), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [Owner(1, "OA")])
        self.assertEqual(owner_owns_apartment(2, 21), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [Owner(1, "OA")])
        self.assertEqual(owner_owns_apartment(2, 23), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [Owner(1, "OA")])
        self.assertEqual(owner_owns_apartment(2, 25), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [Owner(1, "OA")])
        self.assertEqual(owner_owns_apartment(2, 24), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [Owner(1, "OA"), Owner(2, "OB")])
        self.assertEqual(add_apartment(Apartment(26, "RN", "Metola", "ISR", 80)), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [])
        self.assertEqual(owner_owns_apartment(2, 26), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [Owner(2, "OB")])
        self.assertEqual(add_apartment(Apartment(27, "RO", "Metola", "ISR", 80)), ReturnValue.OK)
        self.assertEqual(owner_owns_apartment(1, 27), ReturnValue.OK)
        self.assertEqual(get_all_location_owners(), [Owner(1, "OA"), Owner(2, "OB")])

        # --------------------------------------- PROFIT TEST START --------------------------------------- #
        profitPerMonth: List[Tuple[int, float]] = []
        for i in range(1, 13):
            profitPerMonth.append((i, 0))

        self.assertEqual(profit_per_month(2023), profitPerMonth)
        self.assertEqual(profit_per_month(2024), profitPerMonth)
        # January
        d1 = date(2023, 1, 10)
        d2 = date(2023, 1, 20)
        d3 = date(2023, 1, 22)
        d4 = date(2023, 1, 27)
        self.assertEqual(customer_made_reservation(12, 5, d1, d2, 1000), ReturnValue.OK)  # 100 per night
        profitPerMonth[0] = (1, 1000 * 0.15)
        self.assertEqual(profit_per_month(2023), profitPerMonth)
        self.assertEqual(best_value_for_money(), Apartment(5, "RA", "Haifa", "ISR", 80))
        self.assertEqual(customer_made_reservation(12, 6, d3, d4, 2000), ReturnValue.OK)  # 400 per night
        profitPerMonth[0] = (1, 3000 * 0.15)
        self.assertEqual(profit_per_month(2023), profitPerMonth)

        # March
        d5 = date(2023, 3, 15)
        d6 = date(2023, 3, 19)
        self.assertEqual(customer_made_reservation(13, 5, d5, d6, 2000), ReturnValue.OK)  # 500 per night
        profitPerMonth[2] = (3, 2000 * 0.15)
        self.assertEqual(profit_per_month(2023), profitPerMonth)

        # April
        d7 = date(2023, 4, 1)
        d8 = date(2023, 4, 5)
        d9 = date(2023, 4, 10)
        d10 = date(2023, 4, 15)
        d11 = date(2023, 4, 20)
        d12 = date(2023, 5, 1)
        self.assertEqual(customer_made_reservation(12, 8, d7, d8, 4000), ReturnValue.OK)  # 1000 per night
        profitPerMonth[3] = (4, 4000 * 0.15)
        self.assertEqual(profit_per_month(2023), profitPerMonth)
        self.assertEqual(customer_made_reservation(12, 8, d9, d10, 3000), ReturnValue.OK)  # 600 per night
        profitPerMonth[3] = (4, 7000 * 0.15)
        self.assertEqual(profit_per_month(2023), profitPerMonth)
        self.assertEqual(customer_made_reservation(12, 8, d11, d12, 2000), ReturnValue.OK)  # 200 per night
        profitPerMonth[4] = (5, 2000 * 0.15)
        self.assertEqual(profit_per_month(2023), profitPerMonth)

        # June + July
        d13 = date(2023, 6, 10)
        d14 = date(2023, 6, 15)
        d15 = date(2023, 7, 15)
        self.assertEqual(customer_made_reservation(12, 9, d13, d14, 8000), ReturnValue.OK)  # 1600 per night
        profitPerMonth[5] = (6, 8000 * 0.15)
        self.assertEqual(profit_per_month(2023), profitPerMonth)
        self.assertEqual(customer_made_reservation(12, 9, d14, d15, 6720), ReturnValue.OK)  # 224 per night
        profitPerMonth[6] = (7, 6720 * 0.15)
        self.assertEqual(profit_per_month(2023), profitPerMonth)

        # Jan 2024
        d16 = date(2024, 1, 1)
        d17 = date(2024, 1, 11)
        self.assertEqual(customer_made_reservation(14, 9, d16, d17, 10000), ReturnValue.OK)  # 1000 per night
        self.assertEqual(profit_per_month(2023), profitPerMonth)
        profitPerMonth = []
        for i in range(1, 13):
            profitPerMonth.append((i, 0))
        profitPerMonth[0] = (1, 10000 * 0.15)
        self.assertEqual(profit_per_month(2024), profitPerMonth)

        # --------------------------------------- PROFIT TEST END --------------------------------------- #
        d18 = date(2025, 1, 1)
        self.assertEqual(customer_reviewed_apartment(12, 6, d18, 4, "ok"), ReturnValue.OK)
        self.assertEqual(best_value_for_money(), Apartment(6, "RB", "Haifa", "ISR", 80))
        self.assertEqual(customer_reviewed_apartment(12, 9, d18, 10, "ok"), ReturnValue.OK)
        self.assertEqual(best_value_for_money(), Apartment(9, "RE", "Haifa", "Canada", 80))
        self.assertEqual(customer_reviewed_apartment(14, 9, d18, 6, "ok"), ReturnValue.OK)
        self.assertEqual(best_value_for_money(), Apartment(6, "RB", "Haifa", "ISR", 80))
        self.assertEqual(customer_reviewed_apartment(12, 8, d18, 10, "ok"), ReturnValue.OK)
        self.assertEqual(best_value_for_money(), Apartment(8, "RD", "Nahariya", "ISR", 80))
        self.assertEqual(customer_reviewed_apartment(13, 5, d18, 8, "ok"), ReturnValue.OK)
        self.assertEqual(best_value_for_money(), Apartment(5, "RA", "Haifa", "ISR", 80))
        self.assertEqual(customer_reviewed_apartment(12, 5, d18, 10, "ok"), ReturnValue.OK)
        self.assertEqual(best_value_for_money(), Apartment(5, "RA", "Haifa", "ISR", 80))
        self.assertEqual(add_apartment(Apartment(1, 'A', 'A', 'A', 10000)), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(1, 'CCCA')), ReturnValue.OK)
        dAAA1 = date(2022, 2, 2)
        dAAA2 = date(2022, 2, 5)
        self.assertEqual(customer_made_reservation(1, 1, dAAA1, dAAA2, 900), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(1, 1, dAAA2, 10, "YA"), ReturnValue.OK)
        self.assertEqual(best_value_for_money(), Apartment(1, 'A', 'A', 'A', 10000))

        print("// ==== test_Advanced_API: SUCCESS! ==== //")

    def test_Advanced_API2(self):
        print("Running Test: test_Advanced_API2...")
        self.assertEqual(add_owner(Owner(1, "OA")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(2, "OB")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(3, "OC")), ReturnValue.OK)
        self.assertEqual(add_owner(Owner(4, "OD")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(12, "CA")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(13, "CB")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(14, "CC")), ReturnValue.OK)
        self.assertEqual(add_customer(Customer(15, "CD")), ReturnValue.OK)
        self.assertEqual(add_apartment(Apartment(5, "RA", "Haifa", "ISR", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(6, "RB", "Haifa", "ISR", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(7, "RC", "Akko", "ISR", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(8, "RD", "Nahariya", "ISR", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(9, "RE", "Haifa", "Canada", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(10, "RF", "Akko", "Canada", 80)), ReturnValue.OK)  #
        self.assertEqual(add_apartment(Apartment(11, "RG", "Toronto", "Canada", 80)), ReturnValue.OK)

        d1 = date(2023, 4, 1)
        d2 = date(2023, 5, 1)
        d3 = date(2030, 5, 20)
        d4 = date(2024, 4, 1)
        d5 = date(2024, 5, 1)
        d6 = date(2025, 4, 1)
        d7 = date(2025, 5, 1)
        d8 = date(2026, 4, 1)
        d9 = date(2026, 5, 1)
        self.assertEqual(customer_made_reservation(12, 9, d1, d2, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(12, 10, d1, d2, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(12, 11, d1, d2, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(13, 5, d4, d5, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(13, 6, d4, d5, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(13, 10, d4, d5, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(13, 11, d4, d5, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(14, 5, d6, d7, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(14, 6, d6, d7, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(14, 11, d6, d7, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(15, 5, d8, d9, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(15, 7, d8, d9, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(15, 10, d8, d9, 1000), ReturnValue.OK)
        self.assertEqual(customer_made_reservation(15, 11, d8, d9, 1000), ReturnValue.OK)

        self.assertEqual(customer_reviewed_apartment(12, 9, d3, 1, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(12, 10, d3, 5, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(12, 11, d3, 8, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(13, 5, d3, 4, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(13, 6, d3, 3, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(13, 10, d3, 10, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(13, 11, d3, 5, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(14, 5, d3, 5, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(14, 6, d3, 5, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(14, 11, d3, 9, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(15, 5, d3, 6, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(15, 7, d3, 8, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(15, 10, d3, 5, "Eh"), ReturnValue.OK)
        self.assertEqual(customer_reviewed_apartment(15, 11, d3, 1, "Eh"), ReturnValue.OK)

        apt5: float = 6.21481481481481481482
        apt6: float = 3.79722222222222222223
        apt7: float = 10.0000000000000000
        result = get_apartment_recommendation(12)
        self.assertEqual(result, [(Apartment(5, "RA", "Haifa", "ISR", 80), apt5),
                                  (Apartment(6, "RB", "Haifa", "ISR", 80), apt6),
                                  (Apartment(7, "RC", "Akko", "ISR", 80), apt7)])
        apt9 = 1.31250000000000000000
        result = get_apartment_recommendation(13)
        self.assertEqual(result, [(Apartment(7, "RC", "Akko", "ISR", 80), apt7),
                                  (Apartment(9, "RE", "Haifa", "Canada", 80), apt9)])
        apt9 = 1.12500000000000000000
        apt10 = 8.54166666666666666667
        result = get_apartment_recommendation(14)
        self.assertEqual(result, [(Apartment(7, "RC", "Akko", "ISR", 80), apt7),
                                  (Apartment(9, "RE", "Haifa", "Canada", 80), apt9),
                                  (Apartment(10, "RF", "Akko", "Canada", 80), apt10)])
        apt6 = 2.73888888888888888890
        apt9 = 1.00000000000000000000
        result = get_apartment_recommendation(15)
        self.assertEqual(result, [(Apartment(6, "RB", "Haifa", "ISR", 80), apt6),
                                  (Apartment(9, "RE", "Haifa", "Canada", 80), apt9)])
        print("// ==== test_Advanced_API2: SUCCESS! ==== //")

    @classmethod
    def tearDownClass(cls):
        drop_tables()


if __name__ == "__main__":
    unittest.main()
