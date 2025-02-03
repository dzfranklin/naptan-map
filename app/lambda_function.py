import tempfile

from . import main


def lambda_handler(event, context):
    with tempfile.TemporaryDirectory() as scratch:
        main.main(scratch)
