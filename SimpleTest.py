import unittest
from datetime import date, datetime
import Solution as Solution
from Utility.ReturnValue import ReturnValue
from AbstractTest import AbstractTest

from Business.Apartment import Apartment
from Business.Owner import Owner
from Business.Customer import Customer

'''
    Simple test, create one of your own
    make sure the tests' names start with test
'''


class Test(AbstractTest):
    def test_add_customer(self) -> None:
        c1 = Customer(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'regular customer')
        c2 = Customer(2, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c2), 'invalid name')
        c3 = Customer(1, 'a2')
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.add_customer(c3), 'duplicate id')
        c4 = Customer(1, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_customer(c4), 'duplicate ID and invalid name')

    def test_get_customer(self) -> None:
        c1 = Customer(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'regular customer')
        self.assertEqual(c1, Solution.get_customer(1), 'get customer')
        self.assertEqual(Customer.bad_customer(), Solution.get_customer(3), 'get non-existant customer')

    def test_delete_customer(self) -> None:
        c1 = Customer(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'regular customer')
        self.assertEqual(ReturnValue.OK, Solution.delete_customer(1), 'delete customer')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.delete_customer(1), 'delete non-existant customer')

    # =================================== OWNER TESTS ============================================
    def test_add_owner(self) -> None:
        o1 = Owner(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1), 'regular owner add')
        o2 = Owner(2, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_owner(o2), 'invalid owner name')
        o3 = Owner(1, 'b2')
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.add_owner(o3), 'duplicate owner add')
        o4 = Owner(1, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_owner(o4), 'duplicate owner and invalid name add')

    def test_get_owner(self) -> None:
        o1 = Owner(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1), 'regular owner add')
        self.assertEqual(o1, Solution.get_owner(1), 'get owner')
        self.assertEqual(Owner.bad_owner(), Solution.get_owner(2), 'get non-existant owner')

    def test_delete_owner(self) -> None:
        o1 = Owner(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1), 'regular owner add')
        self.assertEqual(ReturnValue.OK, Solution.delete_owner(1), 'delete owner')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.delete_owner(1), 'delete non-existant owner')

    # =================================== APARTMENT TESTS ==============================================
    def test_add_apartment(self) -> None:
        apt1 = Apartment(1, 'test', 'a', 'b', 3)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'normal apartment add')
        apt2 = Apartment(2, None, 'a', 'b', 3)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_apartment(apt2), 'apartment missing address add')
        apt3 = Apartment(3, 'test', None, 'b', 3)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_apartment(apt3), 'apartment missing city add')
        apt4 = Apartment(4, 'test', 'a', None, 3)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_apartment(apt4), 'apartment missing country add')
        apt5 = Apartment(5, 'test', 'a', 'b', None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_apartment(apt5), 'apartment missing size add')
        apt6 = Apartment(6, 'test', 'a', 'b', -3)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_apartment(apt6), 'apartment negative size add')
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.add_apartment(apt1), 'duplicate apartment add')
        apt7 = Apartment(1, None, None, None)
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.add_apartment(
            apt7), 'duplicate apartment with bad params add')
        apt8 = Apartment(8, 'test', 'a', 'b', 3)
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.add_apartment(apt8), 'same location apartment add')

    def test_get_apartment(self) -> None:
        apt1 = Apartment(1, 'test', 'a', 'b', 3)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'normal apartment add')
        self.assertEqual(apt1, Solution.get_apartment(1), 'get apartment')
        self.assertEqual(Apartment.bad_apartment(), Solution.get_apartment(2), 'get non-existant apartment')

    def test_delete_apartment(self) -> None:
        apt1 = Apartment(1, 'test', 'a', 'b', 3)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'normal apartment add')
        self.assertEqual(ReturnValue.OK, Solution.delete_apartment(1), 'delete apartment')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.delete_apartment(1), 'delete non-existant apartment')

    # =================================== RESERVATION TEST =================================================
    def test_add_reservation(self) -> None:
        c1 = Customer(1, 'one')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'add customer')
        c2 = Customer(2, 'two')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c2), 'add customer')
        apt1 = Apartment(1, 'test_addr', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')

        res2 = {'customer_id': 3, 'apartment_id': 1, 'start_date': date(
            2013, 10, 5), 'end_date': date(2014, 12, 3), 'total_price': 50}
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_made_reservation(
            **res2), 'missing customer add reservation')

        res3 = {'customer_id': 1, 'apartment_id': 2, 'start_date': date(
            2013, 10, 5), 'end_date': date(2014, 12, 3), 'total_price': 50}
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_made_reservation(
            **res3), 'missing apartment add reservation')

        res4 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(
            2013, 10, 5), 'end_date': date(2012, 12, 3), 'total_price': 50}
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_made_reservation(
            **res4), 'negative date add reservation')

        res5 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(
            2013, 10, 5), 'end_date': date(2014, 12, 3), 'total_price': -50}
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_made_reservation(
            **res5), 'negative price add reservation')

        res1 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(
            2013, 10, 5), 'end_date': date(2014, 12, 3), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res1), 'add reservation')

        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_made_reservation(
            **res1), 'duplicate reservation add')

        res11 = {'customer_id': 2, 'apartment_id': 1, 'start_date': date(
            2014, 10, 5), 'end_date': date(2015, 12, 3), 'total_price': 50}
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_made_reservation(
            **res11), 'date overlap reservation add')

    def test_cancel_reservation(self) -> None:
        c1 = Customer(1, 'one')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'add customer')
        apt1 = Apartment(1, 'test_addr', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')
        res1 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(
            2013, 10, 5), 'end_date': date(2014, 12, 3), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res1), 'add reservation')
        self.assertEqual(ReturnValue.OK, Solution.customer_cancelled_reservation(
            1, 1, date(2013, 10, 5)), 'cancel reservation')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_cancelled_reservation(
            1, 1, date(2013, 10, 5)), 'cancel non-existant reservation')

    # =================================== REVIEW TEST =================================================
    def test_add_review(self) -> None:
        c1 = Customer(1, 'one')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'add customer')
        apt1 = Apartment(1, 'test_addr', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')
        res1 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(
            2013, 10, 5), 'end_date': date(2014, 12, 3), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res1), 'add reservation')
        review1 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 5, 'review_text': 'good'}
        self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(**review1), 'add review')
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.customer_reviewed_apartment(
            **review1), 'duplicate review add')
        review2 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2011, 3, 3), 'rating': 6, 'review_text': 'good'}
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_reviewed_apartment(
            **review2), 'invalid date review add')
        review3 = {'customer_id': 1, 'apartment_id': 2, 'review_date': date(
            2015, 3, 3), 'rating': 6, 'review_text': 'good'}
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_reviewed_apartment(
            **review3), 'invalid apartment review add')
        review4 = {'customer_id': 2, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 6, 'review_text': 'good'}
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_reviewed_apartment(
            **review4), 'invalid customer review add')
        review5 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 15, 'review_text': None}
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_reviewed_apartment(
            **review5), 'invalid score review add')
        review6 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 0, 'review_text': None}
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_reviewed_apartment(
            **review6), 'invalid score review add')

    def test_change_review(self) -> None:
        c1 = Customer(1, 'one')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'add customer')
        apt1 = Apartment(1, 'test_addr', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')
        res1 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(
            2013, 10, 5), 'end_date': date(2014, 12, 3), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res1), 'add reservation')
        review1 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 5, 'review_text': 'good'}
        self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(**review1), 'add review')

        review2 = {'customer_id': 1, 'apartment_id': 1, 'update_date': date(
            2015, 4, 4), 'new_rating': 4, 'new_text': 'good'}
        self.assertEqual(ReturnValue.OK, Solution.customer_updated_review(**review2), 'change review')
        review3 = {'customer_id': 2, 'apartment_id': 1, 'update_date': date(
            2016, 3, 3), 'new_rating': 6, 'new_text': 'good'}
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_updated_review(
            **review3), 'change non-existant review')
        review4 = {'customer_id': 1, 'apartment_id': 1, 'update_date': date(
            2013, 3, 3), 'new_rating': 6, 'new_text': 'bad'}
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.customer_updated_review(
            **review4), 'change review to earlier date')
        review5 = {'customer_id': 1, 'apartment_id': 1, 'update_date': date(
            2016, 3, 3), 'new_rating': 15, 'new_text': 'good'}
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_updated_review(
            **review5), 'invalid score review change')
        review6 = {'customer_id': 1, 'apartment_id': 1, 'update_date': date(
            2016, 3, 3), 'new_rating': 0, 'new_text': 'good'}
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.customer_updated_review(
            **review6), 'invalid score review change')

    # ===================================== OWNERSHIP TESTS ============================================
    def test_owner_owns_apartment(self) -> None:
        o1 = Owner(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1), 'regular owner add')
        apt1 = Apartment(1, 'test_addr', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1, 1), 'add ownership')
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.owner_owns_apartment(1, 1), 'duplicate ownership add')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.owner_owns_apartment(
            1, 2), 'non-existing apartment ownership add')

        # this case will not be checked https://piazza.com/class/ln2fwf476uq1gd/post/61
        # self.assertEqual(ReturnValue.NOT_EXISTS, Solution.owner_owns_apartment(
        # 2, 1), 'non-existing owner ownership add')

    def test_owner_drops_apartment(self) -> None:
        o1 = Owner(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1), 'regular owner add')
        apt1 = Apartment(1, 'test_addr', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1, 1), 'add ownership')
        self.assertEqual(ReturnValue.OK, Solution.owner_drops_apartment(1, 1), 'drop ownership')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.owner_drops_apartment(1, 1), 'drop non-existant ownership')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.owner_drops_apartment(2, 1), 'drop non-existant ownership')
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.owner_drops_apartment(1, 2), 'drop non-existant ownership')

    def test_get_apt_owner(self) -> None:
        o1 = Owner(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1), 'regular owner add')
        apt1 = Apartment(1, 'test_addr', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1, 1), 'add ownership')

        self.assertEqual(o1, Solution.get_apartment_owner(1), 'get apartment owner')
        self.assertEqual(Owner.bad_owner(), Solution.get_apartment_owner(2), 'get non-existant apartment owner')

    def test_get_owner_apartments(self) -> None:
        o1 = Owner(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1), 'regular owner add')
        o2 = Owner(2, 'b2')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o2), 'regular owner add')
        apt1 = Apartment(1, 'test_addr1', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')
        apt2 = Apartment(2, 'test_addr2', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt2), 'add apartment')
        apt3 = Apartment(3, 'test_addr3', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt3), 'add apartment')
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1, 1), 'add ownership')
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1, 2), 'add ownership')
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2, 3), 'add ownership')

        self.assertEqual([apt1, apt2], Solution.get_owner_apartments(1), 'get owner apartments')


# =================================== API TESTS ============================================


    def test_apt_rating(self) -> None:
        c1 = Customer(1, 'one')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'add customer')
        c2 = Customer(2, 'two')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c2), 'add customer')
        c3 = Customer(3, 'three')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c3), 'add customer')

        apt1 = Apartment(1, 'test_addr', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')

        res1 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(
            2013, 1, 1), 'end_date': date(2013, 2, 2), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res1), 'add reservation')
        review1 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 5, 'review_text': 'good'}
        self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(**review1), 'add review')

        res2 = {'customer_id': 2, 'apartment_id': 1, 'start_date': date(
            2014, 1, 1), 'end_date': date(2014, 2, 2), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res2), 'add reservation')
        review2 = {'customer_id': 2, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 4, 'review_text': 'good'}
        self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(**review2), 'add review')

        res3 = {'customer_id': 3, 'apartment_id': 1, 'start_date': date(
            2015, 1, 1), 'end_date': date(2015, 2, 2), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res3), 'add reservation')
        review3 = {'customer_id': 3, 'apartment_id': 1, 'review_date': date(
            2016, 3, 3), 'rating': 3, 'review_text': 'good'}
        self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(**review3), 'add review')

        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.owner_owns_apartment(1, 1), 'add ownership')

        self.assertEqual(4, Solution.get_apartment_rating(1), 'get apartment rating')
        self.assertEqual(4, Solution.get_apartment_rating(1), 'get apartment rating')

    def test_get_owner_rating(self) -> None:
        o1 = Owner(1, 'a1')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(o1), 'regular owner add')

        c1 = Customer(1, 'one')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'add customer')
        c2 = Customer(2, 'two')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c2), 'add customer')

        apt1 = Apartment(1, 'test_addr_1', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment')
        apt2 = Apartment(2, 'test_addr_2', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt2), 'add apartment')
        apt3 = Apartment(3, 'test_addr_3', 'test_city', 'test_country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt3), 'add apartment')

        res1 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(
            2013, 1, 1), 'end_date': date(2013, 2, 2), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res1), 'add reservation')

        res2 = {'customer_id': 1, 'apartment_id': 2, 'start_date': date(
            2014, 1, 1), 'end_date': date(2014, 2, 2), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res2), 'add reservation')

        res3 = {'customer_id': 2, 'apartment_id': 3, 'start_date': date(
            2015, 1, 1), 'end_date': date(2015, 2, 2), 'total_price': 50}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res3), 'add reservation')

        review1 = {'customer_id': 1, 'apartment_id': 1, 'review_date': date(
            2015, 3, 3), 'rating': 5, 'review_text': 'good'}
        self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(**review1), 'add review')
        review2 = {'customer_id': 1, 'apartment_id': 2, 'review_date': date(
            2015, 3, 3), 'rating': 4, 'review_text': 'good'}
        self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(**review2), 'add review')
        review3 = {'customer_id': 2, 'apartment_id': 3, 'review_date': date(
            2016, 3, 3), 'rating': 3, 'review_text': 'good'}
        self.assertEqual(ReturnValue.OK, Solution.customer_reviewed_apartment(**review3), 'add review')

        self.assertEqual(ReturnValue.OK,Solution.owner_owns_apartment(1, 1),'add ownership appartment 1')
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1, 2), 'add ownership appartment 2')
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1, 3), 'add ownership appartment 3')
        self.assertEqual(4, Solution.get_owner_rating(1), 'get owner rating')

    def test_get_top_customer(self) -> None:
        c1 = Customer(1, 'Customer1')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c1), 'add customer 1')
        c2 = Customer(2, 'Customer2')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c2), 'add customer 2')
        c3 = Customer(3, 'Customer3')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(c3), 'add customer 3')

        apt1 = Apartment(1, 'Address1', 'City', 'Country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'add apartment 1')
        apt2 = Apartment(2, 'Address2', 'City', 'Country', 5)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt2), 'add apartment 2')

        res1 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(2023, 1, 1), 'end_date': date(2023, 1, 10),
                'total_price': 100}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res1), 'Customer 1 reservation 1')
        res2 = {'customer_id': 1, 'apartment_id': 2, 'start_date': date(2023, 2, 1), 'end_date': date(2023, 2, 10),
                'total_price': 200}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res2), 'Customer 1 reservation 2')

        res3 = {'customer_id': 2, 'apartment_id': 1, 'start_date': date(2023, 3, 1), 'end_date': date(2023, 3, 10),
                'total_price': 150}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res3), 'Customer 2 reservation')

        self.assertEqual(c1.get_customer_id(), Solution.get_top_customer().get_customer_id(),
                         'Verify top customer based on number of reservations')

    def test_reservations_per_owner(self) -> None:
        owner1 = Owner(1, 'Owner One')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(owner1), 'Add owner 1')
        owner2 = Owner(2, 'Owner Two')
        self.assertEqual(ReturnValue.OK, Solution.add_owner(owner2), 'Add owner 2')

        apt1 = Apartment(1, '123 Street', 'City', 'Country', 100)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt1), 'Add apartment 1')
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(1, 1), 'Owner 1 owns apartment 1')

        apt2 = Apartment(2, '456 Avenue', 'City', 'Country', 200)
        self.assertEqual(ReturnValue.OK, Solution.add_apartment(apt2), 'Add apartment 2')
        self.assertEqual(ReturnValue.OK, Solution.owner_owns_apartment(2, 2), 'Owner 2 owns apartment 2')

        customer1 = Customer(1, 'Customer One')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(customer1), 'Add customer 1')

        customer2 = Customer(2, 'Customer Two')
        self.assertEqual(ReturnValue.OK, Solution.add_customer(customer2), 'Add customer 2')

        res1 = {'customer_id': 1, 'apartment_id': 1, 'start_date': date(2023, 1, 1), 'end_date': date(2023, 1, 10), 'total_price': 1000}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res1), 'Customer 1 makes reservation for Apartment 1')

        res2 = {'customer_id': 2, 'apartment_id': 2, 'start_date': date(2023, 2, 1), 'end_date': date(2023, 2, 10), 'total_price': 2000}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res2), 'Customer 2 makes reservation for Apartment 2')

        res3 = {'customer_id': 2, 'apartment_id': 2, 'start_date': date(2024, 2, 1), 'end_date': date(2024, 2, 10),
                'total_price': 2000}
        self.assertEqual(ReturnValue.OK, Solution.customer_made_reservation(**res3),
                         'Customer 2 makes reservation for Apartment 2')
        expected_results = [(1, 1), (2, 2)] # Assuming each owner has one reservation
        actual_results = Solution.reservations_per_owner()

        self.assertListEqual(sorted(expected_results), sorted(actual_results), 'Verify reservation counts per owner')


# *** DO NOT RUN EACH TEST MANUALLY ***
if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
