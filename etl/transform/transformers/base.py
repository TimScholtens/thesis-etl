from abc import ABC, abstractmethod


class Base(ABC):

    @abstractmethod
    def transform(self):
        pass
