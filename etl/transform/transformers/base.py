from abc import ABC, abstractmethod


class Base(ABC):

    @abstractmethod
    def transform(self, extract_directory, transform_directory):
        pass
