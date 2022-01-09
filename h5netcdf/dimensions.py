import weakref

from collections.abc import MutableMapping


class Dimensions(MutableMapping):
    def __init__(self, group):
        self._group = group

    def __getitem__(self, key):
        return self._group._dimensions[key]

    def __setitem__(self, key, value):
        if not self._group._root._writable:
            raise RuntimeError("H5NetCDF: Write to read only")
        self._group._create_dimension(key, value)

    def __delitem__(self, key):
        raise NotImplementedError("cannot yet delete dimensions")

    def __iter__(self):
        for key in self._group._dimensions:
            yield key

    def __len__(self):
        return len(self._group._dimensions)

    def __repr__(self):
        if self._group._root._closed:
            return "<Closed h5netcdf.Dimensions>"
        return "<h5netcdf.Dimensions: %s>" % ", ".join(
            "%s=%r" % (k, v) for k, v in self.items()
        )


def _join_h5paths(parent_path, child_path):
    return "/".join([parent_path.rstrip("/"), child_path.lstrip("/")])


class Dimension(object):
    def __init__(self, parent, name, size=None, phony=False):
        self._parent_ref = weakref.ref(parent)
        self._phony = phony
        self._root_ref = weakref.ref(parent._root)
        self._h5path = _join_h5paths(parent.name, name)
        self._name = name
        self._size = size
        self._dimid = self._root._max_dim_id
        self._initialized = True

    @property
    def _root(self):
        return self._root_ref()

    @property
    def _parent(self):
        return self._parent_ref()

    @property
    def isphony(self):
        return self._phony

    @property
    def _h5ds(self):
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
        return self._h5ds.attrs.get("_Netcdf4Dimid", self._dimid)


    @property
    def size(self):
        if self.isunlimited():
            return self._parent._determine_current_dimension_size(self._name, len(self))
        else:
            return len(self)

    @property
    def maxsize(self):
        return None if self.isunlimited() else self.size

    def __len__(self):
        if self.isphony:
            return self._size
        return self._h5ds.shape[0]

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
