import csv
import os


class Aggregator:

    election = "election"
    electorates = "electorates"
    parties = "parties"
    party_campaigns = "party_campaigns"

    def __init__(self, years, output_dir):
        self.years = years
        self.output_dir = output_dir
        self.id_map = {
            "election": {}
        }

    def open_csv(self, filename):
        with open(filename, newline='') as f:
            reader = csv.reader(f)
            for row in reader:
                yield row

    def write_csv(self, filename, data):
        with open(filename, "w", newline='') as f:
            writer = csv.writer(f)
            for row in data:
                writer.writerow(row)

    def elections(self):
        id = 1
        data = []
        first_file = True
        for year in self.years:
            self.id_map["election"][year] = id
            filename = os.path.join(
                self.output_dir, str(year), "summary total votes.csv")
            reader = self.open_csv(filename)
            header = next(reader)
            if first_file:
                first_file = False
                data.append(["id", "year"] + header)
            for row in reader:
                data.append([id, year] + row)
            id += 1
        output_filename = os.path.join(self.output_dir, "elections.csv")
        self.write_csv(output_filename, data)

    def electorates(self):
        id = 1

    def aggregate_all(self):
        self.elections()
