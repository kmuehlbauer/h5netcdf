import h5py

from collections.abc import MutableMapping


class Dimensions(MutableMapping):
    def __init__(self, group):
        self._group = group

    def __getitem__(self, key):
        return self._group._dimensions[key]# self._group._dim_sizes[key]

    def __setitem__(self, key, value):
        self._group._create_dimension(key, value)

    def __delitem__(self, key):
        raise NotImplementedError("cannot yet delete dimensions")

    def __iter__(self):
        for key in self._group._dimensions:  # self._group._dim_sizes:
            yield key

    def __len__(self):
        return len(self._group._dimensions)#_dim_sizes)

    def __repr__(self):
        if self._group._root._closed:
            return "<Closed h5netcdf.Dimensions>"
        return "<h5netcdf.Dimensions: %s>" % ", ".join(
            "%s=%r" % (k, v) for k, v in self.items()
        )


def _join_h5paths(parent_path, child_path):
    return "/".join([parent_path.rstrip("/"), child_path.lstrip("/")])


class Dimension(object):
    def __init__(self, parent, name, size=None, phony=False, create=False):
        self._parent = parent
        self._phony = phony
        self._root = parent._root
        self._h5path = _join_h5paths(parent.name, name)
        if create:
            kwargs = {}
            if size is None:
                kwargs["maxshape"] = (None,)
            self._parent._h5group.create_dataset(
                name=name, shape=(size,), dtype=">f4", **kwargs
            )
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

    @property
    def maxsize(self):
        return 0 if self.isunlimited() else self.size

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
