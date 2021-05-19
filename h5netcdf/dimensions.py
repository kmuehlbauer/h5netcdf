import weakref
from collections import OrderedDict
from collections.abc import MutableMapping

import h5py


class Dimensions(MutableMapping):
    def __init__(self, group):
        self._group_ref = weakref.ref(group)
        self._objects = OrderedDict()

    @property
    def _group(self):
        return self._group_ref()

    def __getitem__(self, name):
        return self._objects[name]

    def __setitem__(self, name, size):
        phony = "phony_dim" in name
        if not self._group._root._writable and not phony:
            raise RuntimeError("H5NetCDF: Write to read only")
        if name in self._objects:
            raise ValueError("dimension %r already exists" % name)

        size = 0 if size is None else size
        self._objects[name] = Dimension(self._group, name, size, phony=phony)

        # do not create phony_dims
        if self._group._root._writable and not phony:
            self._group._create_dim_scale(name, size)

    def add(self, name):
        self._objects[name] = Dimension(self._group, name)

    def __delitem__(self, key):
        raise NotImplementedError("cannot yet delete dimensions")

    def __iter__(self):
        for key in self._objects:
            yield key

    def __len__(self):
        return len(self._objects)

    def __repr__(self):
        if self._group._root._closed:
            return "<Closed h5netcdf.Dimensions>"
        return "<h5netcdf.Dimensions: %s>" % ", ".join(
            "%s=%r" % (k, v) for k, v in self._objects.items()
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
        if phony:
            self._root._phony_dim_count += 1
        else:
            self._root._max_dim_id += 1
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
    def is_scale(self):
        return h5py.h5ds.is_scale(self._h5ds.id)

    @property
    def dimid(self):
        if self.isphony:
            return False
        return self._h5ds.attrs.get("_Netcdf4Dimid", self._dimid)

    @property
    def size(self):
        size = len(self)
        if self.isunlimited():
            # return actual dimensions sizes, this is in line with netcdf4-python
            # get sizes from all connected variables and calculate max
            # because netcdf unlimited dimensions can be any length
            # but connected variables dimensions can have a certain larger length.
            reflist = self._h5ds.attrs.get("REFERENCE_LIST", None)
            if reflist is not None:
                for ref, axis in reflist:
                    var = self._parent._h5group["/"][ref]
                    size = max(var.shape[axis], size)
        return size

    @property
    def scale_refs(self):
        """Return dimension scale references"""
        return list(self._h5ds.attrs.get("REFERENCE_LIST", []))

    def attach_scale(self, refs):
        """Attach dimension scale to references"""
        for var, dim in refs:
            self._parent._all_h5groups[var].dims[dim].attach_scale(self._h5ds)

    def detach_scale(self):
        """Detach dimension scale from all references"""
        refs = self.scale_refs
        if refs:
            for var, dim in refs:
                self._parent._all_h5groups[var].dims[dim].detach_scale(self._h5ds)

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
        header = "<%s %r: size %s%s>" % (self._cls_name, self.name, self.size, special)
        return "\n".join([header])
