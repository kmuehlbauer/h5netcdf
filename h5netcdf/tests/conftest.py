import itertools
import os
import random
import string
import tempfile
import time
from collections import namedtuple
from os import environ as env
from pathlib import Path
from shutil import rmtree

import pytest

from h5netcdf.tests import (
    has_h5py,
)
from h5netcdf.utils import h5dump as _h5dump

remote_h5 = ("http:", "hdf5:")
Backend = namedtuple("Backend", ["name", "mod"])


@pytest.fixture(scope="session")
def hsds_up():
    """Provide HDF Highly Scalable Data Service (HSDS) for h5pyd testing."""
    h5pyd = pytest.importorskip("h5pyd")
    pytest.importorskip("hsds")
    from hsds.hsds_app import HsdsApp

    root_dir = Path(tempfile.mkdtemp(prefix="tmp-hsds-root-"))
    bucket_name = "pytest"
    os.environ["BUCKET_NAME"] = bucket_name
    # need to create a directory for our bucket
    (root_dir / bucket_name).mkdir()

    kwargs = {
        "username": "h5netcdf-pytest",
        "password": "TestEarlyTestEverything",
        "root_dir": str(root_dir),
        "logfile": str(root_dir / "hsds.log"),
        "log_level": "DEBUG",
        "host": "localhost",
        "sn_port": 5101,
    }

    os.environ.update(
        {
            "BUCKET_NAME": bucket_name,
            "HS_USERNAME": kwargs["username"],
            "HS_PASSWORD": kwargs["password"],
            "HS_USE_HTTPS": "False",
        }
    )

    hsds_app = HsdsApp(**kwargs)

    try:
        hsds_app.run()
        timeout = time.time() + 60
        while not hsds_app.ready:
            if time.time() > timeout:
                raise TimeoutError("HSDS server did not become ready in time")
            time.sleep(1)

        os.environ["HS_ENDPOINT"] = hsds_app.endpoint
        # make folders expected by pytest
        h5pyd.Folder("/home/", mode="w")
        h5pyd.Folder("/home/h5netcdf-pytest/", mode="w")

        yield True

    except Exception as err:
        log_path = kwargs["logfile"]
        if os.path.exists(log_path):
            with open(log_path) as f:
                print("\n=== HSDS Log ===")
                print(f.read())
        else:
            print(f"HSDS log not found at: {log_path}")
        raise err

    finally:
        try:
            hsds_app.check_processes()
            hsds_app.stop()
        except Exception:
            pass

    rmtree(root_dir, ignore_errors=True)


@pytest.fixture
def tmp_local_netcdf(tmpdir):
    return str(tmpdir.join("testfile.nc"))


@pytest.fixture()
def setup_h5pyd_config(hsds_up):
    env["HS_ENDPOINT"] = "http://127.0.0.1:5101"
    env["HS_USERNAME"] = "h5netcdf-pytest"
    env["HS_PASSWORD"] = "TestEarlyTestEverything"
    env["HS_USE_HTTPS"] = "False"


# All possible backend combinations
ALL_COMBINATIONS = [
    ("local", "h5py", "h5py"),
    ("local", "h5py", "pyfive"),
    ("remote", "h5pyd", "h5pyd"),
]


def filtered_combinations(node):
    """Return all combinations, filtered according to @pytest.mark.local/remote."""
    marker_local = node.get_closest_marker("local")
    marker_remote = node.get_closest_marker("remote")

    filtered = []
    for mode, w, r in ALL_COMBINATIONS:
        # skip if marker exists and mode doesn't match
        if marker_local and not marker_remote and mode != "local":
            continue
        if marker_remote and not marker_local and mode != "remote":
            continue
        filtered.append((mode, w, r))

    if not filtered:
        pytest.skip("No valid backend+mode combination for this test")
    return filtered


# Fixture providing one combination per test
@pytest.fixture(
    params=ALL_COMBINATIONS,
    ids=lambda p: f"{p[0]}_w:{p[1]}-r:{p[2]}",
)
def file_and_backend(request, tmpdir, env=os.environ):
    mode, wname, rname = request.param  # supplied via params list

    # Skip if mode doesn't match markers
    marker_local = request.node.get_closest_marker("local")
    marker_remote = request.node.get_closest_marker("remote")
    if marker_local and not marker_remote and mode != "local":
        pytest.skip("Filtered out by local marker")
    if marker_remote and not marker_local and mode != "remote":
        pytest.skip("Filtered out by remote marker")

    # tmp_file
    if mode == "remote":
        try:
            hsds_up = request.getfixturevalue("hsds_up")
        except pytest.skip.Exception:
            pytest.skip("HSDS not available")

        if not hsds_up:
            pytest.skip("HSDS not available — skipping remote test")
        rnd = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=5))
        username = env.get("HS_USERNAME", "user")
        tmp_file = f"hdf5://home/{username}/testfile{rnd}.nc"
    else:
        tmp_file = str(tmpdir.join("testfile.nc"))

    # backends
    wmod = pytest.importorskip(wname)
    rmod = pytest.importorskip(rname)
    write_backend = Backend(wname, wmod)
    read_backend = Backend(rname, rmod)

    return tmp_file, write_backend, read_backend


@pytest.fixture
def file_and_backend2(request, tmpdir, env=os.environ):
    """
    Returns one combination: (tmp_file, write_backend, read_backend)
    Handles HSDS for remote only.
    """
    mode, wname, rname = request.param  # provided via indirect parametrize

    # tmp_file
    if mode == "remote":
        try:
            hsds_up = request.getfixturevalue("hsds_up")
        except pytest.skip.Exception:
            pytest.skip("HSDS not available")

        if not hsds_up:
            pytest.skip("HSDS not available — skipping remote test")
        rnd = "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=5))
        username = env.get("HS_USERNAME", "user")
        tmp_file = f"hdf5://home/{username}/testfile{rnd}.nc"
    else:
        tmp_file = str(tmpdir.join("testfile.nc"))

    # backends
    wmod = pytest.importorskip(wname)
    rmod = pytest.importorskip(rname)
    write_backend = Backend(wname, wmod)
    read_backend = Backend(rname, rmod)

    return tmp_file, write_backend, read_backend


@pytest.fixture
def tmp_netcdf(file_and_backend):
    return file_and_backend[0]


@pytest.fixture
def write_backend(file_and_backend):
    return file_and_backend[1]


@pytest.fixture
def read_backend(file_and_backend):
    return file_and_backend[2]


# @pytest.fixture(params=["testfile.nc", "hdf5://testfile"])
# def tmp_local_or_remote_netcdf(request, tmpdir):
#     param = request.param
#     if param.startswith(remote_h5):
#         try:
#             hsds_up = request.getfixturevalue("hsds_up")
#         except pytest.skip.Exception:
#             pytest.skip("HSDS not available")
#
#         if not hsds_up:
#             pytest.skip("HSDS fixture returned False (not running)")
#
#         rnd = "".join(random.choices(string.ascii_uppercase, k=5))
#         return f"hdf5://home/{env['HS_USERNAME']}/testfile{rnd}.nc"
#     else:
#         if not has_h5py:
#             pytest.skip("h5py not available")
#         return str(tmpdir.join(param))


@pytest.fixture()
def tmp_remote_netcdf(request, tmpdir):
    try:
        hsds_up = request.getfixturevalue("hsds_up")
    except pytest.skip.Exception:
        pytest.skip("HSDS not available")

    if not hsds_up:
        pytest.skip("HSDS fixture returned False (not running)")

    rnd = "".join(random.choices(string.ascii_uppercase, k=5))
    return f"hdf5://home/{env['HS_USERNAME']}/testfile{rnd}.nc"


@pytest.fixture(params=[True, False])
def decode_vlen_strings(request):
    return dict(decode_vlen_strings=request.param)


@pytest.fixture(params=["netCDF4", "h5netcdf.legacyapi"])
def netcdf_write_module(request):
    mod = request.param
    if mod == "h5netcdf.legacyapi" and not has_h5py:
        pytest.skip("No h5py")
    return pytest.importorskip(mod, reason=f"requires {mod}")


@pytest.fixture(params=["h5py", "h5pyd", "pyfive"])
def backend(request, monkeypatch):
    mod = request.param
    _ = pytest.importorskip(mod, reason=f"requires {mod}")
    if mod == "pyfive":
        monkeypatch.setenv("PYFIVE_UNSUPPORTED_FEATURE", "warn")

    return mod


@pytest.fixture(params=["h5py"], ids=lambda p: f"w:{p}")
def local_write_backend(request):
    name = request.param
    mod = pytest.importorskip(name, reason=f"requires {name}")
    return Backend(name, mod)


@pytest.fixture(params=["h5py", "pyfive"], ids=lambda p: f"r:{p}")
def local_read_backend(request, monkeypatch):
    name = request.param
    mod = pytest.importorskip(name, reason=f"requires {name}")
    if name == "pyfive":
        monkeypatch.setenv("PYFIVE_UNSUPPORTED_FEATURE", "warn")
    return Backend(name, mod)


def valid_backend_pairs():
    rw_matrix = {"h5py": ["h5py", "pyfive"], "h5pyd": ["h5pyd"], "pyfive": []}
    return [(w, r) for w in rw_matrix for r in rw_matrix[w]]


@pytest.fixture(params=valid_backend_pairs(), ids=lambda p: f"w:{p[0]}-r:{p[1]}")
def backend_pair(request):
    """Valid (write_backend, read_backend) pairs."""
    wback, rback = request.param
    wmod = pytest.importorskip(wback, reason=f"requires {wback}")
    rmod = pytest.importorskip(rback, reason=f"requires {rback}")
    return Backend(wback, wmod), Backend(rback, rmod)


# # @pytest.fixture
# # def write_backend(backend_pair):
# #     """Write backend from a valid pair."""
# #     return backend_pair[0]
# #
# #
# # @pytest.fixture
# # def read_backend(backend_pair, monkeypatch):
# #     """Read backend from a valid pair."""
# #     if backend_pair[1].name == "pyfive":
# #         monkeypatch.setenv("PYFIVE_UNSUPPORTED_FEATURE", "warn")
# #     return backend_pair[1]
#
#
# @pytest.fixture()
# def tmp_backend_netcdf(tmpdir, write_backend, request):
#     """Return test file path for the given write_backend."""
#     if write_backend.name == "h5pyd":
#         try:
#             hsds_up = request.getfixturevalue("hsds_up")
#         except pytest.skip.Exception:
#             pytest.skip("HSDS not available")
#
#         if not hsds_up:
#             pytest.skip("HSDS fixture returned False (not running)")
#
#         rnd = "".join(random.choices(string.ascii_uppercase, k=5))
#         return f"hdf5://home/{os.environ['HS_USERNAME']}/testfile{rnd}.nc"
#
#     return str(tmpdir.join("testfile.nc"))


def pytest_generate_tests(metafunc):
    # read/write matrix definition
    read_write_mod = ["netCDF4", "h5netcdf", "h5netcdf.legacyapi"]

    # backend modules
    backends = [b for b in ["h5py", "pyfive"]]

    rw_matrix = list(itertools.product(read_write_mod, read_write_mod))

    # generates test matrix for test_dimensions function
    if {"read_write_matrix", "backend_module"} <= set(metafunc.fixturenames):
        cases = []
        ids = []
        for wmod, rmod in rw_matrix:
            if rmod == "netCDF4":
                cases.append(((wmod, rmod), None))
                ids.append(f"{wmod}->{rmod}::no-backend")
            else:
                for backend in backends:
                    cases.append(((wmod, rmod), backend))
                    ids.append(f"{wmod}->{rmod}::{backend}")
        metafunc.parametrize("read_write_matrix, backend_module", cases, ids=ids)

    # generate test_roundtrip_local tests
    if {"tmp_local_netcdf", "wmod", "rmod", "bmod", "decode_vlen"} <= set(
        metafunc.fixturenames
    ):
        cases = []
        ids = []

        # build test matrix
        for wmod, rmod in rw_matrix:
            if rmod == "netCDF4":
                cases.append((wmod, rmod, None, False))
                ids.append(f"{wmod}->{rmod}::no-backend")
            else:
                for backend in backends:
                    # decode_vlen True/False only for h5netcdf reads, others False
                    decode_values = (
                        [True, False]
                        if rmod == "h5netcdf" and backend == "h5py"
                        else [False]
                    )
                    for dec in decode_values:
                        cases.append(
                            (
                                wmod,
                                rmod,
                                backend,
                                dict(decode_vlen_strings=dec),
                            )
                        )
                        ids.append(f"{wmod}->{rmod}::{backend}, dec-vl::{dec}")

        metafunc.parametrize("wmod, rmod, bmod, decode_vlen", cases, ids=ids)


@pytest.fixture
def h5dump():
    return _h5dump


@pytest.fixture(params=["NETCDF4", "NETCDF4_CLASSIC"])
def data_model(request):
    return dict(format=request.param)
