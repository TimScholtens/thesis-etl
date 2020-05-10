from abc import ABC, abstractmethod


class Base(ABC):

    @abstractmethod
    def load(self, transform_directory):
        pass
