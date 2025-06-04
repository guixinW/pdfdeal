from pdfdeal import Doc2X
from pdfdeal.file_tools import get_files
import logging

httpx_logger = logging.getLogger("httpx")
httpx_logger.setLevel(logging.WARNING)
logging.basicConfig(level=logging.INFO)


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
