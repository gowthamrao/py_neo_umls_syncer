import pytest
from unittest.mock import patch

# Mock settings before other imports
from .mocks import mock_settings
mock_settings()

from pyNeoUmlsSyncer.parser import UmlsParser, UmlsTerm

@pytest.fixture
def parser(tmp_path):
    # The parser requires a valid path in its constructor, even if not used
    # by the method under test. tmp_path provides a temporary directory.
    return UmlsParser(rrf_path=tmp_path)

def test_select_preferred_name_sab_priority(parser):
    """
    Tests that a term from a higher-priority SAB is chosen over a
    lower-priority one, even if the lower-priority one has a better
    UMLS rank.
    """
    # From mock_settings, SAB1 is higher priority than SAB2
    terms = [
        UmlsTerm(cui='C001', sab='SAB2', tty='PT', code='C2', name='Lower Prio SAB', ts='P', stt='PF', ispref='Y'),
        UmlsTerm(cui='C001', sab='SAB1', tty='SY', code='C1', name='Higher Prio SAB', ts='S', stt='VO', ispref='N'),
    ]
    preferred = parser.select_preferred_name(terms)
    assert preferred.name == 'Higher Prio SAB'

def test_select_preferred_name_umls_rank_tiebreaker(parser):
    """
    Tests that when terms are from the same priority SAB, the one with
    the better UMLS rank (TS, STT, ISPREF) is chosen.
    """
    terms = [
        # This one has a worse UMLS rank (ispref='N')
        UmlsTerm(cui='C007', sab='SAB1', tty='PT', code='C10', name='Term 7 Sab1 Pref N', ts='P', stt='PF', ispref='N'),
        # This one is better
        UmlsTerm(cui='C007', sab='SAB1', tty='PT', code='C9', name='Term 7 Sab1 Pref Y', ts='P', stt='PF', ispref='Y'),
    ]
    preferred = parser.select_preferred_name(terms)
    assert preferred.name == 'Term 7 Sab1 Pref Y'

def test_select_preferred_name_no_priority_sab(parser):
    """
    Tests that if no term is in a priority SAB, the choice falls back
    to the best UMLS rank.
    """
    terms = [
        UmlsTerm(cui='C002', sab='SAB_OTHER', tty='PT', code='C4', name='Term 2 Other', ts='P', stt='PF', ispref='N'),
        # SAB2 is in priority list, so it should be chosen
        UmlsTerm(cui='C002', sab='SAB2', tty='PT', code='C3', name='Term 2 Sab2 Pref', ts='P', stt='PF', ispref='Y'),
    ]

    # Temporarily modify the SAB_PRIORITY to exclude SAB2
    with patch('pyNeoUmlsSyncer.parser.settings.sab_priority', ['SAB1', 'SOME_OTHER_SAB']):
        # Now, neither SAB_OTHER nor SAB2 are in the priority list.
        # The choice should be based purely on UMLS rank.
        terms_no_prio = [
            UmlsTerm(cui='C002', sab='SAB_OTHER', tty='PT', code='C4', name='Term 2 Other P N', ts='P', stt='PF', ispref='N'),
            UmlsTerm(cui='C002', sab='SAB2', tty='PT', code='C3', name='Term 2 Sab2 S Y', ts='S', stt='VO', ispref='Y'),
        ]
        preferred = parser.select_preferred_name(terms_no_prio)
        # 'Term 2 Other P N' wins because TS='P' is better than TS='S'
        assert preferred.name == 'Term 2 Other P N'

def test_select_preferred_name_single_term(parser):
    """Tests that it works correctly with only one term."""
    terms = [
        UmlsTerm(cui='C001', sab='SAB1', tty='PT', code='C1', name='Single Term', ts='P', stt='PF', ispref='Y')
    ]
    preferred = parser.select_preferred_name(terms)
    assert preferred.name == 'Single Term'
