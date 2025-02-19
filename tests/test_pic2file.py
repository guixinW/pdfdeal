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
    assert isinstance(results, list)  # 返回值应该是列表类型
    assert len(errors) == 1
    assert isinstance(errors[0], dict)
    assert "error" in errors[0]
    assert "path" in errors[0]

    # Test multiple files OCR
    file_list, _ = get_files("tests/image", "img", None)
    results, errors, has_error = client.picocr(file_list)
    assert len(errors) == len(file_list)
    assert isinstance(results, list)  # 返回值应该是列表类型
    for error in errors:
        assert isinstance(error, dict)
        assert "error" in error
        assert "path" in error


def test_piclayout():
    client = Doc2X()
    # Test single file layout analysis
    results, errors, has_error = client.piclayout("tests/image/sample.png")
    assert not has_error
    assert isinstance(results, list)  # 返回值应该是列表类型
    assert len(errors) == 1
    assert isinstance(errors[0], dict)
    assert "error" in errors[0]
    assert "path" in errors[0]
