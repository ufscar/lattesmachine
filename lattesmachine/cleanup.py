import logging
import multiprocessing
from . import settings


logger = logging.getLogger(__name__)


def cleanup_cv(cv):
    pass


def cleanup(cvs):
    p = multiprocessing.Pool()
    return p.map(cleanup_cv, cvs)

