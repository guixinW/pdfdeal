from pdfdeal import Doc2X
from pdfdeal.file_tools import get_files
import logging

httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO)


def test_picocr():
    client = Doc2X()
    # Test single file OCR
    results, errors, has_error = client.picocr("tests/image/sample.png")
    assert not has_error
    assert len(results) == 1
    assert isinstance(results, list)
    assert isinstance(results[0], list)

    # Test multiple files OCR
    file_list, _ = get_files("tests/image", "img", None)
    results, errors, has_error = client.picocr(file_list)
    assert len(results) == len(file_list)
    for result in results:
        assert isinstance(result, (list, str))


def test_piclayout():
    client = Doc2X()
    # Test single file layout analysis
    results, errors, has_error = client.piclayout("tests/image/sample.png")
    assert not has_error
    assert len(results) == 1
    assert isinstance(results, list)
    assert isinstance(results[0], list)

    # Test invalid file type
    try:
        client.piclayout("tests/image/sample.txt")
        assert False, "Should raise ValueError for invalid file type"
    except ValueError as e:
        assert "Unsupported file type" in str(e)

    # Test non-existent file
    try:
        client.piclayout("tests/image/nonexistent.png")
        assert False, "Should raise ValueError for non-existent file"
    except ValueError as e:
        assert "File not found" in str(e)
