from flask import Flask, Response, request
from collections import OrderedDict
from csv import DictReader
from StringIO import StringIO
import json

app = Flask(__name__)

def parse_file_content(f):
    try:
        f = f.strip()
        rows_parsed = []
        rows_parsed_dict = []
        reader = DictReader(StringIO(f))
        for row in reader:
            d = OrderedDict()
            d["Year"]            = row["Year"]
            d["Month"]           = row["Month"]
            d["Fund ID"]         = row["Fund ID"]
            d["Department ID"]   = row["Department ID"]
            d["Fund Name"]       = row["Fund Name"]
            d["Department Name"] = row["Department Name"]
            d["Amount"]          = row["Amount"]
            l = d.values()
            rows_parsed.append(l)
            rows_parsed_dict.append(d)
        return rows_parsed, rows_parsed_dict
    except Exception as e:
        print(str(e))
        return None, None

def key_not_found_in_dict(key, common_dict_key, d1, d2):
    return key not in d1[common_dict_key] or key not in d2[common_dict_key]

def aggregate(lDict):
    buckets = {}
    if len(lDict) == 0:
        return buckets

    for row in lDict:
        amount = round(float(row["Amount"]), 2)
        year = row["Year"]
        if year not in buckets:
            buckets[year] = { "Revenue":  {  "Funds": {},
                                             "Departments": {},
                                             "Total": 0.0
                              },
                              "Expenses": {  "Funds": {},
                                             "Departments": {},
                                             "Total": 0.0
                              }
                            }
        revenueBucket = buckets[year]["Revenue"]
        expensesBucket = buckets[year]["Expenses"]
        categoryBucket = revenueBucket if amount >= 0.0 else expensesBucket

        fundName = row["Fund Name"]
        departmentName = row["Department Name"]
        for categoryString, entityName in [("Funds", fundName), ("Departments", departmentName)]:
            if entityName not in categoryBucket[categoryString]:
                categoryBucket[categoryString][entityName] = 0.0

        categoryBucket["Total"] += amount
        categoryBucket["Departments"][departmentName] += amount
        categoryBucket["Funds"][fundName] += amount

    return buckets


@app.route("/scrub", methods=["POST"])
def scrub():
    file_content_stream = request.files.get("file")
    if file_content_stream is None:
        return "Please provide the input file", 500

    file_content = file_content_stream.read().replace("\r\n", "<NL>").replace("\r", "<NL>").replace("\n", "<NL>").replace("<NL>", "\n")

    lArr, lDict = parse_file_content(file_content)
    if lArr is None and lDict is None:
        return "Unable to parse the document", 404

    out = { "rows_parsed": lArr, "aggregations": aggregate(lDict) }
    return Response( json.dumps(out), mimetype="application/json" ), 200


if __name__ == "__main__":
    app.run(debug=True)
