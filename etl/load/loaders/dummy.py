from etl.load.loaders.base import Base


class Dummy(Base):

    def load(self, transform_directory):
        pass
