from pathlib import Path


def load(loader, transform_directory):
    loader.load(transform_directory=transform_directory)


def final_transformation_files(transform_directory):
    """
    # transform_location = 'directory of to be serialized files'
    @ return = 'returns all distinct files with max sequence number'
    """
    transform_directory_files = [file.name for file in Path(transform_directory).glob('*') if file.is_file()]

    return transform_directory_files
