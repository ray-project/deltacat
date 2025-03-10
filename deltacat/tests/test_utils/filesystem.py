import shutil
import tempfile


from contextlib import contextmanager


@contextmanager
def temp_dir_autocleanup():
    tmpdir = tempfile.mkdtemp()
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)
