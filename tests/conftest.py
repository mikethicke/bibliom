"""
Setup / fixtures for pytest.
"""

import os
from textwrap import dedent
import logging

import pytest

from bibliom import dbmanager
from bibliom import exceptions

logging.getLogger('bibliom.pytest').debug("### Beginning pytest session. ###")

DB_NAME = 'test_db'
DB_USER = 'test_user'
DB_PASSWORD = 'jfYf2NoJr4DMHrF,3b'

@pytest.fixture(scope="session")
def connected_manager():
    logging.getLogger('bibliom.pytest').debug('Pytest: connected_manager fixture')
    try:
        manager = dbmanager.DBManager(
            name=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD
        )
        manager.reset_database()
    except exceptions.UnknownDatabaseError:
        manager = dbmanager.DBManager(
            name=None,
            user=DB_USER,
            password=DB_PASSWORD
        )
        manager.create_database(DB_NAME)
    yield manager

    logging.getLogger('bibliom.pytest').debug('Pytest: closing connected_manager')
    if manager.db is not None and manager.db.open:
        manager.close()

@pytest.fixture(scope="class")
def class_manager(request, connected_manager):
    logging.getLogger('bibliom.pytest').debug('Pytest: class_manager fixture')
    request.cls.manager = connected_manager

@pytest.fixture(scope="module")
def import_small_database(connected_manager):
    logging.getLogger('bibliom.pytest').debug('Pytest: import_small_database fixture')
    connected_manager.reset_database()
    sql_file = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
        'test_data',
        'webofscience-small',
        'webofscience-small.sql')
    connected_manager._run_sql_file(sql_file)
    yield
    connected_manager.reset_database()

@pytest.fixture(scope='class')
def file_paths(request):
    logging.getLogger('bibliom.pytest').debug('Pytest: file_paths fixture')
    test_root = os.path.join(
        os.path.abspath(os.path.join(os.path.dirname(__file__), "..")),
        'test_data')
    request.cls.file_paths = {
        'junk': {
            'file': os.path.join(test_root, 'junk', 'utf_8_demo.txt'),
            'dir': os.path.join(test_root, 'junk')
        },
        'WOK': {
            'file': os.path.join(test_root, 'webofscience-small', 'records.txt'),
            'dir': os.path.join(test_root, 'webofscience-small'),
            'dirs': [os.path.join(test_root, 'webofscience-small'),
                     os.path.join(test_root, 'webofscience-small', 'more')]
        }
    }

@pytest.fixture(scope='class')
def test_data(request):
    logging.getLogger('bibliom.pytest').debug('Pytest: test_data fixture')
    request.cls.test_data = {
        'junk': "asfi82734aesfjklnadmf, [   24asfdsadfsarwer4\\\\\\\n\n\n\n\n\n\nasdfdasfd",
        'WOK': dedent(
            """
            FN Clarivate Analytics Web of Science
            VR 1.0
            PT J
            AU Sun, SB
                Sen, S
                Kim, NJ
                Magliery, TJ
                Schultz, PG
                Wang, F
            AF Sun, Sophie B.
                Sen, Shiladitya
                Kim, Nam-Jung
                Magliery, Thomas J.
                Schultz, Peter G.
                Wang, Feng
            TI RETRACTED: Mutational Analysis of 48G7 Reveals that Somatic
                Hypermutation Affects Both Antibody Stability and Binding Affinity
                (Retracted article. See vol. 140, pg. 1976, 2018)
            SO JOURNAL OF THE AMERICAN CHEMICAL SOCIETY
            LA English
            DT Article; Retracted Publication
            ID IMMUNOLOGICAL EVOLUTION; MATURATION; DIVERSITY
            AB The monoclonal antibody 48G7 differs from its germline precursor by 10 somatic mutations, a number of which appear to be functionally silent. We analyzed the effects of individual somatic mutations and combinations thereof on both antibody binding affinity and thermal stability. Individual somatic mutations that enhance binding affinity to hapten decrease the stability of the germline antibody; combining these binding mutations produced a mutant with high affinity for hapten but exceptionally low stability. Adding back each of the remaining somatic mutations restored thermal stability. These results, in conjunction with recently published studies, suggest an expanded role for somatic hypermutation in which both binding affinity and stability are optimized during clonal selection.
            C1 [Sun, Sophie B.; Kim, Nam-Jung; Schultz, Peter G.] Scripps Res Inst, Dept Chem, La Jolla, CA 92037 USA.
                [Sun, Sophie B.; Kim, Nam-Jung; Schultz, Peter G.] Scripps Res Inst, Skaggs Inst Chem Biol, La Jolla, CA 92037 USA.
                [Wang, Feng] Calif Inst Biomed Res, La Jolla, CA 92037 USA.
                [Sen, Shiladitya; Magliery, Thomas J.] Ohio State Univ, Dept Chem & Biochem, Columbus, OH 43210 USA.
            RP Magliery, TJ (reprint author), Ohio State Univ, Dept Chem & Biochem, 100 West 18th Ave, Columbus, OH 43210 USA.
            EM magliery.1@osu.edu; schultz@scripps.edu; fwang@calibr.org
            RI Magliery, Thomas/B-6050-2009
            OI Magliery, Thomas/0000-0003-0779-1477
            FU NIH [R01 GM062159, R01 GM083114]; Skaggs Institute of Chemical Biology
            FX We thank Peter Lee and the Ian Wilson lab for the use of their ForteBio
                Octet RED96 for binding affinity measurements, and we also thank
                Virginia Seely for assistance with manuscript preparation. This work was
                supported by NIH Grant R01 GM062159 (PGS), The Skaggs Institute of
                Chemical Biology (PGS), and NIH Grant R01 GM083114 (TJM). This is
                manuscript #23062 of The Scripps Research Institute.
            CR Center for Biological Sequence Analysis, NETNGLYC 1 0 SERV
                Chong LT, 1999, P NATL ACAD SCI USA, V96, P14330, DOI 10.1073/pnas.96.25.14330
                Di Nola JM, 2007, ANNU REV BIOCHEM, V76, P1, DOI 10.1146/annurev.biochem.76.061705.090740
                Ekiert DC, 2011, SCIENCE, V333, P843, DOI 10.1126/science.1204839
                FRENCH DL, 1989, SCIENCE, V244, P1152, DOI 10.1126/science.2658060
                JACOBS J, 1987, J AM CHEM SOC, V109, P2174, DOI 10.1021/ja00241a042
                KIM S, 1981, CELL, V27, P573, DOI 10.1016/0092-8674(81)90399-8
                King AC, 2011, PROTEIN SCI, V20, P1546, DOI 10.1002/pro.680
                Lavinder JJ, 2009, J AM CHEM SOC, V131, P3794, DOI 10.1021/ja8049063
                Patten PA, 1996, SCIENCE, V271, P1086, DOI 10.1126/science.271.5252.1086
                Schultz PG, 2002, ANGEW CHEM INT EDIT, V41, P4427, DOI 10.1002/1521-3773(20021202)41:23<4427::AID-ANIE4427>3.0.CO;2-K
                Wang F, 2013, P NATL ACAD SCI USA, V110, P4261, DOI 10.1073/pnas.1301810110
                Wedemayer GJ, 1997, J MOL BIOL, V268, P390, DOI 10.1006/jmbi.1997.0974
                Wedemayer GJ, 1997, SCIENCE, V276, P1665, DOI 10.1126/science.276.5319.1665
                Yang PL, 1999, J MOL BIOL, V294, P1191, DOI 10.1006/jmbi.1999.3197
                Yin J, 2003, J MOL BIOL, V330, P651, DOI 10.1016/S0022-2836(03)00631-4
                Yin J, 2001, BIOCHEMISTRY-US, V40, P10764, DOI 10.1021/bi010536c
            NR 17
            TC 17
            Z9 17
            U1 2
            U2 20
            PU AMER CHEMICAL SOC
            PI WASHINGTON
            PA 1155 16TH ST, NW, WASHINGTON, DC 20036 USA
            SN 0002-7863
            J9 J AM CHEM SOC
            JI J. Am. Chem. Soc.
            PD JUL 10
            PY 2013
            VL 135
            IS 27
            BP 9980
            EP 9983
            DI 10.1021/ja402927u
            PG 4
            WC Chemistry, Multidisciplinary
            SC Chemistry
            GA 183JG
            UT WOS:000321810400005
            PM 23795814
            OA Green Accepted
            DA 2018-12-27
            ER
            """)
    }
