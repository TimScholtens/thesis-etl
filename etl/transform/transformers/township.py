from etl.transform.transformers.base import Base
from pathlib import Path
from shutil import copy


class Township(Base):

    def transform(self, extract_directory, transform_directory):

        extract_directory_files = [file for file in Path(extract_directory).glob('*') if file.is_file()]

        for file in extract_directory:
            # seq_num = 0
            copy(file, Path(transform_directory) / file)




