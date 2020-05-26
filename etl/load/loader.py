

def load(loader, transform_directory):
    loader.load(transform_directory=transform_directory)


def final_transformation_file(transform_directory):
    """
    Retrieves final transformation file from transformation directory.
    :param transform_directory: directory in which final transformation file can be found.
    :return: final transformation file.
    """
    from pathlib import Path
    from config import FINAL_TRANSFORMATION_ID

    transform_directory_file = [file.name for file in Path(transform_directory).glob(f'*{FINAL_TRANSFORMATION_ID}*')
                                if file.is_file()]

    return transform_directory_file
