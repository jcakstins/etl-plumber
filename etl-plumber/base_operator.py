from abc import ABC, abstractmethod
     
class Operator(ABC):
    
    @abstractmethod
    def apply(self):
        pass
