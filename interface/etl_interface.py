from abc import ABC, abstractmethod

class ETLInterface(ABC):
    @abstractmethod
    def crawl(self):
       pass

    @abstractmethod
    def transform(self):
        pass 
    
    @abstractmethod
    def load(self):
        pass
