from etl.transform.transformers.base import Base
from pathlib import Path
from shutil import copy
from config import FINAL_TRANSFORMATION_ID


class Passthrough(Base):

    def transform(self, extract_directory, transform_directory):
        """
            ETL lingo -> Passthrough: don't apply any transformation.
        """

        # Create transform_location if not exits
        if not Path(transform_directory).is_dir():
            Path.mkdir(transform_directory, parents=True, exist_ok=True)

        # Note: only 1 extraction file because no transformation is needed.
        extract_directory_file = [file.name for file in Path(extract_directory).glob('*') if file.is_file()][0]

        file_name = extract_directory_file.split('.')[0]
        file_ext = extract_directory_file.split('.')[1]

        final_file_name = f'{file_name}_{FINAL_TRANSFORMATION_ID}.{file_ext}'

        copy(Path(extract_directory) / extract_directory_file, Path(transform_directory) / final_file_name)
