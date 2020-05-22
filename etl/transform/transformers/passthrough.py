from etl.transform.transformers.base import Base
from pathlib import Path
from shutil import copy


class Passthrough(Base):

    def transform(self, extract_directory, transform_directory):
        """
            ETL lingo -> Passthrough: don't apply any transformation.
        """
        # Create transform_location if not exits
        if not Path(transform_directory).is_dir():
            Path.mkdir(transform_directory)

        extract_directory_files = [file.name for file in Path(extract_directory).glob('*') if file.is_file()]

        for file in extract_directory_files:
            # seq_num = 0
            copy(Path(extract_directory) / file, Path(transform_directory) / file)
