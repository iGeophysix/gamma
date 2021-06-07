import io
import unittest

import numpy as np
import redis

from components.database.RedisStorage import RedisStorage


class TestArrayInRedis(unittest.TestCase):

    def setUp(self):
        self._s = RedisStorage()
        self._s.flush_db()

    def test_store_compressed_array(self):
        a = np.array(((10, 20), (30, 40)))
        # print("INIT ARRAY", a)

        stream = io.BytesIO()
        np.save(stream, a, allow_pickle=True)

        # print("BYTES", stream.getvalue())

        self._s.connection().set("t", stream.getvalue())

        b = self._s.connection().get("t")

        # print("BYTES", b)

        b = np.load(io.BytesIO(b), allow_pickle=True)

        # print("RESULT ARRAY", b)

        self.assertEqual(a.all(), b.all())
        self._s.connection().delete("t")

    def test_store_txt_array(self):
        a = np.array(((10, 20), (30, 40)))
        # print("INIT ARRAY", a)

        stream = io.BytesIO()
        np.savetxt(stream, a, fmt='%s')

        self._s.connection().set("t", stream.getvalue())

        b = self._s.connection().get("t")

        b = np.loadtxt(io.BytesIO(b))


        self.assertEqual(a.all(), b.all())
        self._s.connection().delete("t")

    def test_store_map_of_arrays(self):
        a = np.array(((10, 20), (30, 40)))
        b = np.array(((50, 60), (70, 80)))

        m = {"a": a, "b": b}
        # print("INIT MAP", m)

        mb = {}
        for k, v in m.items():
            stream = io.BytesIO()
            np.save(stream, v, allow_pickle=True)
            mb[k] = stream.getvalue()  # bytes

        # print("BYTES MAP", mb)

        self._s.connection().hset("m", mapping=mb)

        mb2 = {k: self._s.connection().hget("m", k) for k in ["a", "b"]}

        # print("BYTES MAP", mb2)

        self.assertEqual(mb, mb2)

        m2 = {k: np.load(io.BytesIO(v), allow_pickle=True) for k, v in mb2.items()}

        # print("RESULT ARRAY", m2)

        self.assertEqual(m["a"].all(), m2["a"].all())
        self.assertEqual(m["b"].all(), m2["b"].all())
