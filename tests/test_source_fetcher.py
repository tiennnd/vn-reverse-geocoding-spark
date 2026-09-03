import hashlib
import pytest
from vn_geo_build.source_fetcher import ChecksumMismatch, fetch


class FakeResponse:
    def __init__(self, payload: bytes):
        self._payload = payload
        self.status_code = 200

    def raise_for_status(self) -> None:
        pass

    def iter_content(self, chunk_size: int = 8192):
        for i in range(0, len(self._payload), chunk_size):
            yield self._payload[i : i + chunk_size]

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


def test_fetch_ghi_file_khi_checksum_dung(tmp_path, monkeypatch):
    payload = b"noi dung gia lap"
    digest = hashlib.sha256(payload).hexdigest()
    monkeypatch.setattr(
        "vn_geo_build.source_fetcher.requests.get",
        lambda url, stream, timeout: FakeResponse(payload),
    )

    dest = tmp_path / "tai_ve.bin"
    result = fetch("https://vi-du/tep", dest, digest)

    assert result == dest
    assert dest.read_bytes() == payload


def test_fetch_bao_loi_va_xoa_file_khi_checksum_sai(tmp_path, monkeypatch):
    payload = b"noi dung gia lap"
    monkeypatch.setattr(
        "vn_geo_build.source_fetcher.requests.get",
        lambda url, stream, timeout: FakeResponse(payload),
    )

    dest = tmp_path / "tai_ve.bin"
    with pytest.raises(ChecksumMismatch):
        fetch("https://vi-du/tep", dest, "0" * 64)

    assert not dest.exists(), "file hong phai bi xoa, khong duoc de lai"


def test_fetch_dung_lai_file_da_co_dung_checksum(tmp_path, monkeypatch):
    payload = b"noi dung gia lap"
    digest = hashlib.sha256(payload).hexdigest()
    dest = tmp_path / "tai_ve.bin"
    dest.write_bytes(payload)

    def khong_duoc_goi(*args, **kwargs):
        raise AssertionError("da co file dung checksum thi khong duoc tai lai")

    monkeypatch.setattr("vn_geo_build.source_fetcher.requests.get", khong_duoc_goi)

    assert fetch("https://vi-du/tep", dest, digest) == dest
