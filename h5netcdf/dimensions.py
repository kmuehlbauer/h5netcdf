from collections.abc import MutableMapping
from packaging.version import Version
import h5py
#import numpy as np


class Dimensions(MutableMapping):
    def __init__(self, group):
        self._group = group

    def __getitem__(self, key):
        #return self._group._dim_sizes[key]
        return self._group._dimensions[key]

    def __setitem__(self, key, value):
        self._group._create_dimension(key, value)

    def __delitem__(self, key):
        raise NotImplementedError("cannot yet delete dimensions")

    def __iter__(self):
        for key in self._group._dimensions:
        #for key in self._group._dim_sizes:
            yield key

    def __len__(self):
        #return len(self._group._dim_sizes)
        return len(self._group._dimensions)

    def __repr__(self):
        if self._group._root._closed:
            return "<Closed h5netcdf.Dimensions>"
        return "<h5netcdf.Dimensions: %s>" % ", ".join(
            "%s=%r" % (k, v) for k, v in self.items()
        )


def _join_h5paths(parent_path, child_path):
    return "/".join([parent_path.rstrip("/"), child_path.lstrip("/")])


NOT_A_VARIABLE = b"This is a netCDF dimension but not a netCDF variable."


class Dimension(object):
    def __init__(self, parent, name, size=None, phony=False, create=False):
        self._parent = parent
        self._phony = phony
        self._root = parent._root
        self._h5path = _join_h5paths(parent.name, name)
        #if size is None:
        #    size = 0
        #if not size:
        #    self._unlimited = True
        #else:
        #    self._unlimited = False

        if create:
            dimlen = bytes(f"{0:10}", "ascii")
            scale_name = (
                name
                if name in self._parent._variables
                else NOT_A_VARIABLE + dimlen
            )
            kwargs = {}
            if size is None or size == 0:
                kwargs["maxshape"] = (None,)
                kwargs["chunks"] = True
                size = 0
            print(name, size, kwargs)
            self._parent._h5group.create_dataset(
                name=name, shape=(size,), dtype=">f4", **kwargs
            )
            if not h5py.h5ds.is_scale(self._h5ds.id):
                if Version(h5py.__version__) < Version("2.10.0"):
                    self._h5ds.dims.create_scale(self._h5ds, scale_name)
                else:
                    self._h5ds.make_scale(scale_name)
        self._name = name
        self._size = size
        self._initialized = True

    @property
    def isphony(self):
        return self._phony

    @property
    def _h5ds(self):
        # Always refer to the root file and store not h5py object
        # subclasses:
        if self.isphony:
            return None
        return self._root._h5file[self._h5path]

    @property
    def name(self):
        if self.isphony:
            return self._name
        return self._h5ds.name

    def isunlimited(self):
        if self.isphony:
            return False
        return self._h5ds.maxshape == (None,)

    @property
    def dimid(self):
        if self.isphony:
            return False
        return self._h5ds.attrs.get("_Netcdf4Dimid")

    @property
    def size(self):
        return len(self)

    def resize(self, size):
        if not self.isunlimited():
            raise ValueError(
                "Dimension '%s' is not unlimited and thus "
                "cannot be resized." % self.name
            )
        self._h5ds.resize((size,))

    @property
    def maxsize(self):
        return self._h5ds.maxshape[0]

    def __len__(self):
        if self.isphony:
            return self._size
        refs = self._h5ds.attrs.get("REFERENCE_LIST", None)
        maxsize = 0
        if refs is not None:
            maxsize = max([self._root._h5file[ref].shape[dim] for ref, dim in refs])
        return maxsize

    _cls_name = "h5netcdf.Dimension"

    def __repr__(self):
        if not self.isphony and self._parent._root._closed:
            return "<Closed %s>" % self._cls_name
        special = ""
        if self.isphony:
            special += " (phony_dim)"
        if self.isunlimited():
            special += " (unlimited)"
        header = "<%s %r: size %s%s>" % (
            self._cls_name,
            self.name,
            self.size,
            special
        )
        return "\n".join(
            [header]
        )
