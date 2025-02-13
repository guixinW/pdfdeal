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
    assert isinstance(results, dict)
    first_result = next(iter(results.values()))
    assert isinstance(first_result, dict)
    assert "text" in first_result or "texts" in first_result

    # Test multiple files OCR
    file_list, _ = get_files("tests/image", "img", None)
    results, errors, has_error = client.picocr(file_list)
    assert len(results) == len(file_list)
    for result in results.values():
        assert isinstance(result, dict)
        assert "text" in result or "texts" in result


def test_piclayout():
    client = Doc2X()
    # Test single file layout analysis
    results, errors, has_error = client.piclayout("tests/image/sample.png")
    assert not has_error
    assert len(results) == 1
    assert isinstance(results, dict)
    first_result = next(iter(results.values()))
    assert isinstance(first_result, dict)
    assert "layout" in first_result or "layouts" in first_result

    # Test multiple files layout analysis
    file_list, _ = get_files("tests/image", "img", None)
    results, errors, has_error = client.piclayout(file_list)
    assert len(results) == len(file_list)
    for result in results.values():
        assert isinstance(result, dict)
        assert "layout" in result or "layouts" in result