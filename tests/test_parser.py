import pytest
from pyNeoUmlsSyncer.config import Settings
from pyNeoUmlsSyncer.parser import UmlsParser

@pytest.mark.integration
def test_parser_mrconso(test_settings: Settings):
    """
    Tests that the UmlsParser can correctly parse the sample MRCONSO.RRF file.
    """
    parser = UmlsParser(test_settings)

    # Consume the generator to get all parsed records
    parsed_records = list(parser.parse_mrconso())

    # The sample MRCONSO.RRF has 4 lines, but one is suppressed ('O')
    assert len(parsed_records) == 3, "Should parse 3 non-suppressed records"

    # Find a specific record to check its content
    record_c2 = next((r for r in parsed_records if r['cui'] == 'C0000002'), None)

    assert record_c2 is not None, "Should find record for C0000002"
    assert record_c2['sab'] == 'SAB1'
    assert record_c2['str'] == 'Concept Two'
    assert record_c2['tty'] == 'PN'
