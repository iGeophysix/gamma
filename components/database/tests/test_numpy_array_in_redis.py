import io
import unittest

import numpy as np
import redis

from settings import REDIS_HOST, REDIS_PORT, REDIS_DB, REDIS_PASSWORD


class TestArrayInRedis(unittest.TestCase):

    def setUp(self):
        self.conn = redis.Redis(host=REDIS_HOST,
                                port=REDIS_PORT,
                                db=REDIS_DB,
                                password=REDIS_PASSWORD)
        self.conn.flushdb()

    def test_store_compressed_array(self):
        a = np.array(((10, 20), (30, 40)))
        # print("INIT ARRAY", a)

        stream = io.BytesIO()
        np.save(stream, a, allow_pickle=True)

        # print("BYTES", stream.getvalue())

        self.conn.set("t", stream.getvalue())

        b = self.conn.get("t")

        # print("BYTES", b)

        b = np.load(io.BytesIO(b), allow_pickle=True)

        # print("RESULT ARRAY", b)

        self.assertEqual(a.all(), b.all())

    def test_store_txt_array(self):
        a = np.array(((10, 20), (30, 40)))
        # print("INIT ARRAY", a)

        stream = io.BytesIO()
        np.savetxt(stream, a, fmt='%s')

        self.conn.set("t", stream.getvalue())

        b = self.conn.get("t")

        b = np.loadtxt(io.BytesIO(b))


        self.assertEqual(a.all(), b.all())

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

        self.conn.hset("m", mapping=mb)

        mb2 = {k: self.conn.hget("m", k) for k in ["a", "b"]}

        # print("BYTES MAP", mb2)

        self.assertEqual(mb, mb2)

        m2 = {k: np.load(io.BytesIO(v), allow_pickle=True) for k, v in mb2.items()}

        # print("RESULT ARRAY", m2)

        self.assertEqual(m["a"].all(), m2["a"].all())
        self.assertEqual(m["b"].all(), m2["b"].all())
