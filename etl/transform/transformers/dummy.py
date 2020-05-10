from etl.transform.transformers.base import Base


class Dummy(Base):

    def transform(self):
        print('transforming from dummy class')



