from etl.transform.transformers.base import Base


class Township(Base):

    def transform(self):
        print('transforming from township class')
