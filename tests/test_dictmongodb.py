import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

import time
import asyncio
import logging

from DictMongodb import DictMongodb

logging.basicConfig(handlers=[logging.StreamHandler(sys.stdout)], level=logging.INFO)


async def test_init():
    dict_mongodb = DictMongodb("mongodb://localhost:27017", "dict", "words")
    assert dict_mongodb is not None

    print("Loading...")
    start = time.time()
    await dict_mongodb.load_from_7z(
        os.path.join(os.path.dirname(__file__), "..", "stardict.7z"), "stardict.csv"
    )
    print(f"Elapsed: {time.time() - start}")
    assert await dict_mongodb.count() > 0


async def test_find():
    dict_mongodb = DictMongodb("mongodb://localhost:27017", "dict", "words")
    assert dict_mongodb is not None

    # print(await dict_mongodb.find("hello"))
    # print(await dict_mongodb.find(1))
    # for doc in await dict_mongodb.fuzzy_find("hell"):
    #     print(doc)

    print(await dict_mongodb.fuzzy_find("deprecate"))


async def main():
    # await test_init()
    await test_find()


if __name__ == "__main__":
    asyncio.run(main())
