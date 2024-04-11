from mimesis import Person
from mimesis import Datetime
from mimesis import Address
from mimesis.enums import Gender
from mimesis import locales
from mimesis.providers.numbers import Numbers
from mimesis.providers.text import Text
import random

Locales = locales
person = Person(Locales.EN_AU)
datetime = Datetime()
address = Address(Locales.EN_AU)
numbers = Numbers()
text = Text()


def mock_first_name(gender_value=None):
    if gender_value is None:
        return person.first_name()

    return person.first_name(gender=_convert_to_mimesis_gender_value(gender_value))


def mock_last_name(gender_value=None):
    if gender_value is None:
        return person.last_name()

    return person.last_name(gender=_convert_to_mimesis_gender_value(gender_value))


def mock_email():
    return person.email()


def mock_phone_number():
    return person.telephone()


def mock_contact_details(contact_type):
    if "email" in contact_type.lower():
        return mock_email()
    if "phone" in contact_type.lower():
        return mock_phone_number()


def mock_drivers_license():
    return F"{_get_random_alphabet()}{_get_random_alphabet()}{numbers.integer_number(start=1000, end=9999)}"


def _get_random_alphabet():
    return random.choice(text.alphabet())


def mock_date_of_birth():
    date = datetime.date(end=2005)
    return date.strftime("%Y-%m-%d %H:%M:%S")


def mock_street_number():
    return address.street_number()


def mock_street_name():
    return address.street_name()


def mock_street_suffix():
    return address.street_suffix()


def mock_city():
    return address.city()


def mock_state():
    return address.state()


def mock_zip_code():
    return address.zip_code()


def mock_account_number():
    return numbers.integer_number(start=1000000000100000, end=1200000000000000)


def _convert_to_mimesis_gender_value(gender_value):
    return Gender.FEMALE if gender_value == 'F' else Gender.MALE
