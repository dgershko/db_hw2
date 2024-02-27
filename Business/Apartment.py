class Apartment:
    def __init__(self, id: int=None, address: str=None, city: str=None, country: str=None, size: float=None) -> None:
        self.__id = id
        self.__address = address
        self.__city = city
        self.__country = country
        self.__size = size

    def get_id(self):
        return self.__id
        
    def set_id(self, id):
        self.__id = id

    def get_address(self):
        return self.__address
    
    def set_address(self, address):
        self.__address = address

    def get_city(self):
        return self.__city
    
    def set_city(self, city):
        self.__city = city

    def get_country(self):
        return self.__country
    
    def set_country(self, country):
        self.__country = country

    def get_size(self):
        return self.__size

    def set_size(self, size):
        self.__size = size

    @staticmethod
    def bad_apartment():
        return Apartment()

    def __eq__(self, __value: object) -> bool:
        if type(self) != type(__value): return False
        else: return self.__id == __value.__id and self.__address == __value.__address and self.__city == __value.__city and self.__country == __value.__country

    def __str__(self) -> str:
        return f'apartment_id={self.__id}, address={self.__address}, city={self.__city}, country={self.__country}'