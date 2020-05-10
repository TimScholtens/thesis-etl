from etl.transform.transformers.base import Base


class Dummy(Base):

    def transform(self, extract_directory, transform_directory):
        pass
