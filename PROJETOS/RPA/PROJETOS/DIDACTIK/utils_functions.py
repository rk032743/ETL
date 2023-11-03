import os
import logging


def get_root_dir():

    return os.path.dirname(__file__)


def get_html(html):

    with open(f'{get_root_dir()}\index.html', 'w',  encoding='utf-8') as f:
        f.write(str(html))


def get_log():

    ROOT_DIR = get_root_dir()
    LOG_FILE = 'didactik_request.log'
    filename_log = os.path.join(ROOT_DIR, LOG_FILE)
    with open(filename_log, 'w'):
        pass
    logging.basicConfig(format='%(asctime)s - %(filename)s - %(levelname)s - %(process)d - %(message)s',
                        level=logging.INFO,
                        filename=filename_log)
