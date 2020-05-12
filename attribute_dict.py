#!/usr/bin/python3

import collections
import copy
from datetime import datetime
from functools import reduce
import os
import re
import json
import sys
import time



class JSONEncoder(json.JSONEncoder):
    """JSONEncoder to handle ``datetime`` and other problematic object values"""

    class EncodeError(Exception):
        """Raised when an error occurs during encoding"""

    def default(self, obj):
        try:
            if isinstance(obj, (datetime)):
                return obj.timestamp()
            elif isinstance(obj, bytes):
                return self.default(obj.decode("utf-8"))
            elif hasattr(obj, "toJSON"):
                return obj.toJSON()
            elif hasattr(obj, "jstr"):
                return obj.jstr()
            else:
                try:
                    encoded_obj = json.JSONEncoder.default(self, obj)
                except TypeError as err:
                    encoded_obj = repr(obj)
                return encoded_obj
        except Exception as err:
            raise JSONEncoder.EncodeError("JSON Encoder Error: {}".format(repr(err)))


### json object serializer
def json_safe(obj):
    """JSON dumper for objects not serializable by default json code"""
    return json.dumps(obj, cls=JSONEncoder, default=str, indent=4, separators=(",", ": "), sort_keys=True)


def to_dict(d):
    if isinstance(d, AD):
        td = {}
        for k, v in d.items():
            if k != "__dict__":
                td[k] = to_dict(v)
        return td
    else:
        return d


def to_ad(d):
    if isinstance(d, AD):
        return d
    if isinstance(d, bytes):
        return d.decode()
    if hasattr(d, "items"):
        td = AD()
        for k, v in d.items():
            if isinstance(k, bytes):
                k = k.decode()
            else:
                k = str(k)
            td[k] = to_ad(v)
        return td
    return d
d_to_ad = to_ad


########################################################################################################
# AD - Persistent Attribute Accessible Dict Class
########################################################################################################


class AttributeDictError(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message

class AD(dict):
    """
    Dictionary subclass enabling attribute lookup/assignment of keys/values.
    """

    cmeta = {}

    __getitem = dict.__getitem__
    __setitem = dict.__setitem__
    __delitem = dict.__delitem__

    __keys = dict.keys
    __items = dict.items

    def __init__(self, *args, **kwargs):
        dict.__init__(self)
        if len(args):
            for i in args:
                if not isinstance(i, str):
                    self.update(i)

        if len(kwargs):
            if "persistTGT" in kwargs:
                if "flush" in kwargs:
                    self.setpersist(kwargs["persistTGT"], flush=True)
                    del kwargs["flush"]
                else:
                    self.setpersist(kwargs["persistTGT"])
                del kwargs["persistTGT"]
            self.update(AD._to_ad(kwargs))

    def __cmp__(self, other):
        return id(self) - id(other)

    def __contains__(self, key):
        key = AD._prep_key(key)
        if "." not in key:
            return key in self.keys()
        else:
            return key in AD._deep_keys(self)
    has_key = __contains__

    def __delattr__(self, key):
        return self.__delitem__(key)

    def __delitem__(self, key):
        key = AD._prep_key(key)
        if self.__contains__(key):
            if "." in key:
                path, nkey = key.rsplit(".", 1)
                return AD.__delitem(AD.__getitem__(self, path), nkey)
            else:
                return AD.__delitem(self, key)

    def __deepcopy__(self):
        return AD(self)

    def __getattr__(self, key):
        return self.__getitem__(key)

    def __getitem__(self, key):
        try:
            key = AD._prep_key(key)
            if "." in key:
                return reduce(AD.__getitem, [k for k in key.split(".")], self)
            else:
                return AD.__getitem(self, key)
        except:
            raise KeyError(key)

    def __getstate__(self):
        return self.jstr()

    def __hash__(self):
        return id(self)

    def __iter__(self):
        for k in self.keys():
            yield k

    def __setattr__(self, key, value):
        return self.__setitem__(key, value)

    def __setitem__(self, key, value):
        key = AD._prep_key(key)
        if "." in key:
            path, nkey = key.split(".", 1)
            if isinstance(self.setdefault(path, AD()), (dict, AD)):
                return AD.__setitem__(AD.__getitem(self, path), nkey, AD._to_ad(value))
            else:
                AD.__setitem(self, path, AD())
                AD.__getitem(self, path)[nkey] = AD._to_ad(value)
                return self[key]
        else:
            return AD.__setitem(self, key, AD._to_ad(value))

    def __setstate__(self, state):
        if isinstance(state, str):
            self.update(json.loads(state))
            self.sync()
        elif isinstance(state, (dict, AD)):
            self.update(state)
            self.sync()
        else:
            raise AttributeDictError(f"Value is not a json_string or dict-like object: type -> {type(state)}")

    @staticmethod
    def _deep_items(d):
        """Recursive item iterator"""
        kvs = AD._deep_keys(d)
        _items = []
        for k in kvs:
            v = d[k]
            if isinstance(v, AD):
                _items.extend(AD._deep_items(v))
            elif isinstance(v, dict):
                _items.extend(
                    [(k + "." + str(_k), _v) for _k, _v in v.items() if _k]
                )
            else:
                _items.append((k, v))
        return iter(sorted(_items, key=lambda x: x[0]))

    @staticmethod
    def _deep_keys(d):
        """Recursive key iterator"""
        kvs = []
        for k, v in AD.__items(d):
            if "." in k or k == '__dict__':
                continue
            elif hasattr(v, "keys"):
                kvs.extend(
                    [k + "." +str(_k) for _k in AD._deep_keys(v) if _k]
                )
            else:
                kvs.append(k)
        return sorted(kvs)

    ### json object serializer
    @staticmethod
    def _json_safe(obj):
        """JSON dumper for objects not serializable by default json code"""
        return json.dumps(obj, cls=JSONEncoder, default=str, indent=4, separators=(",", ": "), sort_keys=True)

    @staticmethod
    def load(path):
        """Reads json in from a file"""
        if os.path.exists(path):
            with open(path, "r") as fh:
                return AD(json.load(fh))
        else:
            raise IOError(f"PAth does not exists {path}")

    @staticmethod
    def loads(jstr):
        """Reads parses str_in as json, and updates from results """
        return AD(json.loads(jstr))

    @staticmethod
    def _prep_key(key):
        """key formatter that insures keys are strings"""
        if isinstance(key, bytes):
            key = key.encode()
        else:
            key = str(key)
        key = key.replace("..", ".")
        if key[0] == ".":
            key = key[1:]
        if key[-1] == ".":
            key = key[:-1]
        return key

    @staticmethod
    def _to_ad(item):
        return AD._to_x(item, tgt=AD)

    @staticmethod
    def _to_dict(item):
        return AD._to_x(item)

    @staticmethod
    def _to_x(item, tgt=dict):
        if isinstance(item, (dict, AD)):
            td = tgt()
            for k, v in item.items():
                if k != "__dict__"  and isinstance(v, (dict, AD)):
                    td[AD._prep_key(k)] = AD._to_x(v, tgt=tgt)
                else:
                    td[AD._prep_key(k)] = v
            return td
        else:
            return item

    def clear(self):
        for key in self.keys():
            del self[key]

    def delete(self, key):
        AD.__delitem__(self, key)

    def deep_items(self):
        return AD._deep_items(self)
    deepItems = deep_items

    def deep_keys(self):
        return AD._deep_keys(self)
    deepKeys = deep_keys

    def dumps(self):
        return self.jstr()

    def get(self, key, default=None):
        try:
            if not key:
                return None
            key = AD._prep_key(key)
            if default:
                return self.setdefault(key, default=default)
            else:
                return self[key]
        except Exception:
            return None

    def getlike(self, partial_key, multi=False):
        kmatch = re.compile(partial_key)
        if multi:
            res = []
            for key in self:
                if kmatch.match(key):
                    res.append((key, self[key]))
            return dict(res)
        else:
            for key in self:
                if kmatch.match(key):
                    return self[key]

    def items(self):
        return [(k, v) for k, v in dict.items(self) if k != "__dict__"]

    def iteritems(self):
        for k, v in self.items():
            yield (k, v)

    def iterkeys(self):
        return self.__iter__()

    def itervalues(self):
        for _, v in self.items():
            yield v

    def jstr(self):
        return AD._json_safe(self)
    _for_json = jstr
    to_json = jstr

    def keys(self):
        return [k for k in AD.__keys(self) if k != "__dict__"]

    def pop(self, key):
        key = AD._prep_key(key)
        value = self[key]
        del self[key]
        return (key, value)

    def popitem(self):
        key = self.keys()[0]
        value = self[key]
        del self[key]
        return (key, value)

    def setdefault(self, key, default=None):
        key = AD._prep_key(key)
        if key not in self:
            self[key] = default
        return self[key]

    def setpersist(self, path, flush=False):
        """Sets path for persistence json store"""
        if not path:
            return self
        else:
            my = AD.cmeta[id(self)] = AD()
            my.path = os.path.abspath(path)
            my.dir = os.path.dirname(my.path)
            my.fname = os.path.basename(my.path)
            if not os.path.exists(my.path) or flush:
                os.system(f"mkdir -p {my.dir}")
                with open(my.path, 'w') as fh:
                    fh.writelines(["{}"])
            return self

    def sync(self):
        """Writes text rendering of self to a file"""
        if id(self) in AD.cmeta:
            my = AD.cmeta[id(self)]
            with open(my.path, "w") as me:
                me.write(self.jstr())

    def to_dict(self):
        return AD._to_dict(self)
    as_dict = to_dict

    def update(self, item):
        if isinstance(item, tuple) and len(item) == 2:
            self[AD._prep_key(item[0])] = AD._to_ad(item[1])
        elif (
            not isinstance(item, str)
            and isinstance(item, tuple)
            or isinstance(item, list)
            and all([len(i) == 2 for i in item])
            ):
            for _i in item:
                self.update(_i)
        elif hasattr(item, "items"):
            for k, v in item.items():
                k = AD._prep_key(k)
                if k in self and isinstance(self[k], AD):
                    self[k].update(AD._to_ad(v))
                else:
                    self[k] = AD._to_ad(v)

    def values(self):
        return list(self.itervalues())

##########################################################################################

def toCAD(d):
    if Consul_AD._ckvSig(d):
        return Consul_AD()(d)
    if hasattr(d, "items"):
        td = Consul_AD()
        for k, v in d.items():
            k = Consul_AD._prep_key(k)
            if k !="__dict__":
                td[k] = toCAD(v)
        return td
    else:
        try:
            if isinstance(d, bytes) and d[0] == b"[":
                return json.loads(d.decode("utf-8"))
            elif isinstance(d, bytes):
                try:
                    if d[0] == b'"' and len(d) > 4:
                        try:
                            return json.loads(d[1:-1].decode())
                        except UnicodeDecodeError:
                            return d[1:-1]
                    elif len(d) > 0:
                        return json.loads(d.decode())
                except json.scanner.JSONDecodeError:
                    return d.decode()
                except UnicodeDecodeError:
                    return d
            elif isinstance(d, str) and d.startswith('''b'"'''):
                try:
                    return json.loads(d[3:-3])
                except UnicodeDecodeError:
                    return d[3:-3]
            else:
                return d
        except Exception:
            return d


class Consul_AD(AD):
    __consul_value_sig = sorted(
        ["CreateIndex", "ModifyIndex", "LockIndex", "Flags", "Key", "Value", "Session"]
    )

    def __init__(self, *args, **kwargs):
        AD.__init__(self, *args, **kwargs)

    @staticmethod
    def _ckvSig(rec):
        cnt = 0
        if hasattr(rec, "keys"):
            for k in Consul_AD.__consul_value_sig:
                if k in rec:
                    cnt += 1
            if cnt >= len(Consul_AD.__consul_value_sig) - 2 and "Key" in rec and "Value" in rec:
                return True
        return False

    @staticmethod
    def _jvalue(value):
        try:
            if isinstance(value, bytes):
                _value = json.loads(value.decode())
            elif isinstance(value, str):
                _value = json.loads(value)
            else:
                _value = value
        except KeyError:
            print('Missing required key "Value"')
            _value = value
        except json.decoder.JSONDecodeError:
            _value = value
        return _value

    @staticmethod
    def _prep_key(key):
        """key formatter that insures keys are strings"""
        if isinstance(key, bytes):
            key = key.encode()
        else:
            key = str(key)
        key = key.replace("/", ".")
        key = key.replace("..", ".")
        if key[0] == ".":
            key = key[1:]
        if key[-1] == ".":
            key = key[:-1]
        return key

    def __call__(self, item):
        if isinstance(item, list) and all([isinstance(i, (dict, AD)) for i in item]):
            for i in item:
                self(i)
        elif isinstance(item, tuple) and len(item) == 2:
            self.update(item)
        elif Consul_AD._ckvSig(item):
            _key = Consul_AD._prep_key(item["Key"])
            _value = Consul_AD._jvalue(item["Value"])
            AD.__setitem__(self, _key, _value)
        elif hasattr(item, "keys"):
            self.update(item)
        else:
            print("Consul_AD - cannot process item: {}".format(item))

AD5 = AD
CAD = Consul_AD

