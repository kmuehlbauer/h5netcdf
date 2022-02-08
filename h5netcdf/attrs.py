from collections.abc import MutableMapping

import numpy as np

_HIDDEN_ATTRS = frozenset(
    [
        "REFERENCE_LIST",
        "CLASS",
        "DIMENSION_LIST",
        "NAME",
        "_Netcdf4Dimid",
        "_Netcdf4Coordinates",
        "_nc3_strict",
        "_NCProperties",
    ]
)


class Attributes(MutableMapping):
    def __init__(self, h5attrs, check_dtype):
        self._h5attrs = h5attrs
        self._check_dtype = check_dtype

    def __getitem__(self, key):
        import h5py

        if key in _HIDDEN_ATTRS:
            raise KeyError(key)

        attr = self._h5attrs.get_id(key)

        # handle Empty
        if attr.get_space().get_simple_extent_type() == h5py.h5s.NULL:
            string_info = h5py.check_string_dtype(attr.dtype)
            if string_info and string_info.length == 1:
                return b""
            return h5py.Empty(attr.dtype)

        dtype = attr.dtype
        shape = attr.shape

        htype = h5py.h5t.py_create(dtype)

        if dtype.subdtype is not None:
            subdtype, subshape = dtype.subdtype
            shape = attr.shape + subshape   # (5, 3)
            dtype = subdtype                # 'f'

        arr = np.ndarray(shape, dtype=dtype, order='C')
        attr.read(arr, mtype=htype)

        # see https://github.com/h5netcdf/h5netcdf/issues/94 for details
        if isinstance(self._h5attrs[key], h5py.Empty):
            string_info = h5py.check_string_dtype(self._h5attrs[key].dtype)
            if string_info and string_info.length == 1:
                return b""

        #print(key, dtype)

        # see https://github.com/h5netcdf/h5netcdf/issues/116 for details
        # extract encoding decoding information
        string_info = h5py.check_string_dtype(dtype)

       # print(key, dtype, string_info)

        if string_info:
            if string_info.length is None:
                arr = np.array([
                    b.decode('utf-8', 'surrogateescape') for b in arr.flat
                ], dtype=dtype).reshape(arr.shape)
                arr = [b for b in arr.flat]
            else:
                #c = "S"# if string_info.encoding == "utf-8" else "S"
                arr = [
                    b.decode(string_info.encoding) for b in arr.flat
                ]
            if len(arr) == 1:  # in [(), (1,)]:
                arr = arr[0]
        else:
            if len(arr.shape) == 0:
                arr = arr[()]
        #arr = np.array(self._h5attrs[key])
        #string_info = h5py.check_string_dtype(arr.dtype)




        # if string_info and string_info.length is not None:
        #     try:
        #         arr = [att.decode(string_info.encoding) for att in arr]
        #     except UnicodeDecodeError:
        #         pass
        #
        # #print(key, arr.dtype, arr.shape)
        #
        # # see https://github.com/h5netcdf/h5netcdf/issues/116 for details
        # # check if 0-dim or one-element 1-dim and return extracted item()
        # if len(arr) == 1:
        #     arr = arr[0]

        return arr

    def __setitem__(self, key, value):
        if key in _HIDDEN_ATTRS:
            raise AttributeError("cannot write attribute with reserved name %r" % key)
        if hasattr(value, "dtype"):
            dtype = value.dtype
        else:
            dtype = np.asarray(value).dtype
        self._check_dtype(dtype)
        self._h5attrs[key] = value

    def __delitem__(self, key):
        del self._h5attrs[key]

    def __iter__(self):
        for key in self._h5attrs:
            if key not in _HIDDEN_ATTRS:
                yield key

    def __len__(self):
        hidden_count = sum(1 if attr in self._h5attrs else 0 for attr in _HIDDEN_ATTRS)
        return len(self._h5attrs) - hidden_count

    def __repr__(self):
        return "\n".join(
            ["%r" % type(self)] + ["%s: %r" % (k, v) for k, v in self.items()]
        )
