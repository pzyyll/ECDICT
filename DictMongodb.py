import motor.motor_asyncio
import py7zr
import asyncio
import tempfile
import contextlib
import logging

from stardict import DictCsv


class DictMongodb:
    def __init__(self, mongodb_uri, db_name, collection_name):
        self.client = motor.motor_asyncio.AsyncIOMotorClient(mongodb_uri)
        self.db = self.client[db_name]
        self.collection = self.db[collection_name]
        self.fields = [
            "id",
            "word",
            "sw",
            "phonetic",
            "definition",
            "translation",
            "pos",
            "collins",
            "oxford",
            "tag",
            "bnc",
            "frq",
            "exchange",
            "detail",
            "audio",
        ]

    async def _create_indexs(self):
        await self.collection.create_index("id", unique=True)
        await self.collection.create_index("word", unique=True)
        await self.collection.create_index("sw")
        await self.collection.create_index(
            [
                ("word", "text"),
                ("sw", "text"),
            ]
        )

    async def _run_with_asyncio(self, func, *args, **kwargs):
        return await asyncio.get_running_loop().run_in_executor(
            None, func, *args, **kwargs
        )

    async def load_from_7z(self, file, csv_file_name=""):
        await self.collection.drop()
        await self._create_indexs()

        def _extract_all_sync(tempdir):
            with py7zr.SevenZipFile(file, mode="r") as z:
                z.extractall(tempdir)
            return f"{tempdir}/{csv_file_name}"

        def _chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        @contextlib.asynccontextmanager
        async def _temp_dir():
            loop = asyncio.get_running_loop()
            try:
                temp_dir = await loop.run_in_executor(None, tempfile.TemporaryDirectory)
                yield temp_dir.name
            finally:
                if temp_dir:
                    await loop.run_in_executor(None, temp_dir.cleanup)

        def _check_data(data):
            for key in ("oxford", "collins"):
                x = data[key]
                if isinstance(x, (int, str)) and x in (0, "", "0"):
                    data[key] = None
            return data

        async def _task(dict_csv, words):
            return await self.insert_many(
                [_check_data(dict_csv[word]) for word in words]
            )

        async with _temp_dir() as tempdir:
            logging.info("Extracting file[%s] ... ", file)
            csv_file = await self._run_with_asyncio(_extract_all_sync, tempdir)

            logging.info("Loading DictCsv[%s] ...", csv_file)
            dict_csv: DictCsv = await self._run_with_asyncio(DictCsv, csv_file)

            logging.info("Create task...")
            tasks = []
            for words in _chunks(dict_csv.dumps(), 700000):
                task = asyncio.create_task(_task(dict_csv, words))

                tasks.append(task)
                cnt = len(tasks)
                logging.info("Tasks count start: %d", cnt)
            logging.info("Wait for all tasks to complete ...")
            await asyncio.gather(*tasks)
            logging.info("Tasks Done.")

    async def insert_many(self, items):
        return await self.collection.insert_many(items)

    async def update(self, key, items):
        # check key is string
        filter_opt = {"word": key} if isinstance(key, str) else {"id": key}
        return await self.collection.update_one(
            filter_opt, {"$set": items}, upsert=True
        )

    async def find(self, key):
        filter_opt = {"word": key} if isinstance(key, str) else {"id": key}
        return await self.collection.find_one(filter_opt)

    async def fuzzy_find(self, key, limit: int = None):
        return await self.collection.find(
            {
                "$text": {
                    "$search": key,
                    "$caseSensitive": False,
                    "$diacriticSensitive": False,
                }
            }
        ).to_list(limit)

    async def count(self):
        return await self.collection.count_documents({})
