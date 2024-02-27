class Customer:
    def __init__(self, customer_id: int=None, customer_name: str=None) -> None:
        self.__id = customer_id
        self.__name = customer_name

    def get_customer_id(self):
        return self.__id
    
    def set_customer_id(self, id):
        self.__id = id
    
    def get_customer_name(self):
        return self.__name
    
    def set_customer_name(self, name):
        self.__name = name

    @staticmethod
    def bad_customer():
        return Customer()

    def __eq__(self, __value: object) -> bool:
        if type(self) != type(__value): return False
        else: return self.__id == __value.__id and self.__name == __value.__name

    def __str__(self) -> str:
        return f'customer_id={self.__id}, customer_name={self.__name}'