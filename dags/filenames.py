import os
from enum import Enum


def directory_up(path: str, n: int):
    for _ in range(n):
        path = directory_up(path.rpartition("/")[0], 0)
    return path


root_path = os.path.dirname(os.path.realpath(__file__))
# Change working directory to root of the project.
os.chdir(directory_up(root_path, 1))


class Filename(Enum):
    initial_log = 'log.txt'
    extracted_data = 'extracted_data.txt'
    transformed_data = 'transformed_data.txt'
    weblog = 'weblog.tar'


def get_absolute_path(file: Filename) -> str:
    return os.getcwd() + '/data/' + file.value
