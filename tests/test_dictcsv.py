import os, sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from stardict import DictCsv

def test_dictcsv():
    csv_file = os.path.join(os.path.dirname(__file__), "..", "ecdict.mini.csv")
    dict_csv = DictCsv(csv_file)
    print(dict_csv.query("nite"))
    print(dict_csv.match("why"))

test_dictcsv()
