# tests/test_roundtrip_local.py


from dataclasses import dataclass
from pathlib import Path

import pytest

from h5netcdf.tests.test_h5netcdf import (
    read_h5netcdf,
    read_legacy_netcdf,
    write_h5netcdf,
    write_legacy_netcdf,
)

# tests/test_roundtrip_local.py


# -------------------------------
# Helper: skip modules if missing
# -------------------------------
def require_modules(*modules: str):
    for mod in modules:
        pytest.importorskip(mod)


# -------------------------------
# Fixtures
# -------------------------------
@pytest.fixture
def tmp_local_netcdf(tmp_path):
    """Return a path for a temporary NetCDF file."""
    return tmp_path / "test.nc"


# -------------------------------
# Capability classes
# -------------------------------
@dataclass(frozen=True)
class Writer:
    name: str
    write: callable  # function: Path -> None
    requires: tuple[str, ...] = ()


@dataclass(frozen=True)
class Reader:
    name: str
    read: callable  # function: Path, write_module -> None
    requires: tuple[str, ...] = ()


# -------------------------------
# Writer implementations
# -------------------------------
def write_h5netcdf_local(path: Path):
    write_h5netcdf(path)
    return path


def write_legacy_local(path: Path):
    import h5netcdf.legacyapi as legacy

    write_legacy_netcdf(path, legacy)
    return path


def write_netcdf4_local(path: Path):
    import netCDF4

    write_legacy_netcdf(path, netCDF4)
    return path


WRITERS = {
    "h5netcdf": Writer("h5netcdf", write_h5netcdf_local, requires=("h5py",)),
    "legacy": Writer("h5netcdf.legacyapi", write_legacy_local, requires=("h5py",)),
    "netcdf4": Writer("netCDF4", write_netcdf4_local, requires=("netCDF4",)),
}


# -------------------------------
# Reader implementations
# -------------------------------
def read_h5netcdf_h5py(path: Path, write_module, decode_vlen=False):
    read_h5netcdf(
        tmp_netcdf=path,
        write_module=write_module,
        decode_vlen_strings={"decode_vlen_strings": decode_vlen},
        backend="h5py",
    )


def read_h5netcdf_pyfive(path: Path, write_module):
    read_h5netcdf(
        tmp_netcdf=path,
        write_module=write_module,
        decode_vlen_strings={},
        backend="pyfive",
    )


def read_legacy_h5py(path: Path, write_module):
    import h5netcdf.legacyapi as legacy

    read_legacy_netcdf(path, legacy, write_module, backend="h5py")


def read_legacy_pyfive(path: Path, write_module):
    import h5netcdf.legacyapi as legacy

    read_legacy_netcdf(path, legacy, write_module, backend="pyfive")


def read_netcdf4(path: Path, write_module):
    import netCDF4

    read_legacy_netcdf(path, netCDF4, write_module, backend=None)


READERS = {
    "h5netcdf-h5py": Reader(
        "h5netcdf-h5py",
        lambda p, wmod: read_h5netcdf_h5py(p, wmod, decode_vlen=False),
        requires=("h5py",),
    ),
    "h5netcdf-h5py-decode": Reader(
        "h5netcdf-h5py-decode",
        lambda p, wmod: read_h5netcdf_h5py(p, wmod, decode_vlen=True),
        requires=("h5py",),
    ),
    "h5netcdf-pyfive": Reader(
        "h5netcdf-pyfive", read_h5netcdf_pyfive, requires=("pyfive",)
    ),
    "legacy-h5py": Reader("legacy-h5py", read_legacy_h5py, requires=("h5py",)),
    "legacy-pyfive": Reader("legacy-pyfive", read_legacy_pyfive, requires=("pyfive",)),
    "netcdf4": Reader("netcdf4", read_netcdf4, requires=("netCDF4",)),
}

# -------------------------------
# Map writer_name to actual module
# -------------------------------
WRITER_MODULE_MAP = {
    "h5netcdf": __import__("h5netcdf"),
    "legacy": __import__("h5netcdf.legacyapi"),
    "netcdf4": __import__("netCDF4"),
}

# -------------------------------
# Roundtrip test
# -------------------------------
import itertools

writer_names = ["h5netcdf", "legacy", "netcdf4"]
reader_names = [
    "h5netcdf-h5py",
    "h5netcdf-h5py-decode",
    "h5netcdf-pyfive",
    "legacy-h5py",
    "legacy-pyfive",
    "netcdf4",
]
param_matrix = list(itertools.product(writer_names, reader_names))


@pytest.mark.parametrize("writer_name,reader_name", param_matrix)
def test_roundtrip_local_all(writer_name, reader_name, tmp_local_netcdf, monkeypatch):
    writer = WRITERS[writer_name]
    reader = READERS[reader_name]
    writer_module = WRITER_MODULE_MAP[writer_name]

    # Skip if dependencies are missing
    require_modules(*writer.requires, *reader.requires)

    # pyfive quirk
    if "pyfive" in reader.name:
        monkeypatch.setenv("PYFIVE_UNSUPPORTED_FEATURE", "warn")

    # Write → Read
    writer.write(str(tmp_local_netcdf))
    reader.read(str(tmp_local_netcdf), writer_module)
