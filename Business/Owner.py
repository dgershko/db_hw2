class Owner:
    def __init__(self, owner_id: int=None, owner_name: str=None) -> None:
        self.__id = owner_id
        self.__name = owner_name

    def get_owner_id(self):
        return self.__id
    
    def set_owner_id(self, id):
        self.__id = id
    
    def get_owner_name(self):
        return self.__name
    
    def set_owner_name(self, name):
        self.__name = name

    @staticmethod
    def bad_owner():
        return Owner()

    def __eq__(self, __value: object) -> bool:
        if type(self) != type(__value): return False
        else: return self.__id == __value.__id and self.__name == __value.__name

    def __str__(self) -> str:
        return f'owner_id={self.__id}, owner_name={self.__name}'