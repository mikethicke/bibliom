"""
Unit tests for crossref.py
"""

# pylint: disable=no-member, missing-docstring, len-as-condition
import logging

import pytest

from bibliom.publication_objects import Paper
from bibliom.crossref import *

@pytest.mark.usefixtures('class_manager')
class TestCrossref():
    def test_cr_works(self):
        logging.getLogger('bibliom.pytest').debug('-->TestCrossref.test_cr_works')
        response = cr_works(ids='10.1016/j.ijhydene.2016.06.178')
        assert(
            response['message']['title'][0] ==
            'RETRACTED: Plasma equilibrium reconstruction for the nuclear ' +
            'fusion of magnetically confined hydrogen isotopes'
        )

    def test_parse_response(self):
        logging.getLogger('bibliom.pytsest').debug('-->TestCrossref.test_parse_response')
        response = cr_works(ids='10.1016/j.ijhydene.2016.06.178')
        paper = parse_response(response)
        assert isinstance(paper, Paper)
        assert(
            paper.title ==
            'RETRACTED: Plasma equilibrium reconstruction for the nuclear ' +
            'fusion of magnetically confined hydrogen isotopes'
        )
