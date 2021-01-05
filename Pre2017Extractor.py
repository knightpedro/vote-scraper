import csv
import os
import logging


class Pre2017Extractor:
    summary = "e9_part1.csv"
    electorate_party_turnout = "e9_part9_1.csv"
    electorate_candidate_turnout = "e9_part9_2.csv"
    electorate_votes_for_registered_parties = "e9_part4.csv"

    def __init__(self, year, data_dir, output_dir):
        self.year = year
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.data_path = os.path.join(data_dir, str(year))

    def load_csv(self, filename):
        with open(filename) as csv_file:
            for row in csv.reader(csv_file):
                yield row

    def write_csv(self, name, data):
        filename = os.path.join(self.output_dir, str(self.year), f"{name}.csv")
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        with open(filename, "w", newline='') as csv_file:
            if isinstance(data, list):
                writer = csv.DictWriter(csv_file, data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            else:
                writer = csv.DictWriter(csv_file, data.keys())
                writer.writeheader()
                writer.writerow(data)

    def get_voting_place_candidate_filename(self, electorate_number):
        return os.path.join(self.data_path, f"e9_part8_cand_{electorate_number}.csv")

    def get_voting_place_party_filename(self, electorate_number):
        return os.path.join(self.data_path, f"e9_part8_party_{electorate_number}.csv")

    def get_results_summary(self):
        party_votes = []
        reading_parties = False
        registered = True
        logging.info("Loading election summary")
        filename = os.path.join(self.data_path, self.summary)
        reader = self.load_csv(filename)

        while reader and not reading_parties:
            row = next(reader)
            if "Registered" in row[0]:
                reading_parties = True

        while reader and reading_parties:
            row = next(reader)
            if "Unregistered" in row[0]:
                registered = False
            elif "Registered" in row[0]:
                pass
            elif "Independent" == row[0].strip():
                reading_parties = False
            else:
                party = {
                    "name": row[0],
                    "party seats": row[1],
                    "party votes": row[2],
                    "candidate seats": row[5],
                    "candidate votes": row[6],
                    "registered": registered
                }
                party_votes.append(party)

        next(reader)

        valid_votes_row = next(reader)
        valid_party_votes = valid_votes_row[3]
        valid_candidate_votes = valid_votes_row[-1]

        informal_votes_row = next(reader)
        informal_party_votes = informal_votes_row[3]
        informal_candidate_votes = informal_votes_row[-1]

        reader.close()

        total_votes = {
            "valid party votes": valid_party_votes,
            "valid candidate votes": valid_candidate_votes,
            "informal party votes": informal_party_votes,
            "informal candidate votes": informal_candidate_votes,
        }

        return total_votes, party_votes

    def process_electorate_turnout(self, reader):
        title_row = next(reader)
        vote_type = "party" if "Party" in title_row[0] else "candidate"

        for i in range(4):
            next(reader)

        electorates = []
        maori_electorate = False

        while reader:
            row = next(reader)
            if row[0] == "General Electorate Totals":
                maori_electorate = True
                continue
            if row[0] == "Maori Electorate Totals":
                break
            electorate = {
                "name": row[0],
                f"{vote_type} valid ordinary votes": row[1],
                f"{vote_type} valid special votes": row[2],
                f"{vote_type} informal ordinary votes": row[4],
                f"{vote_type} informal special votes": row[5],
                f"{vote_type} special votes disallowed": row[7],
                "enrolled": row[9],
                "population": row[10],
                "maori electorate": maori_electorate
            }
            electorates.append(electorate)

        return electorates

    def get_electorates(self):
        candidate_filename = os.path.join(
            self.data_path, self.electorate_candidate_turnout)
        party_filename = os.path.join(
            self.data_path, self.electorate_party_turnout)

        logging.info("loading electorate party voter turnout")
        party_reader = self.load_csv(party_filename)
        logging.info("loading electorate candidate voter turnout")
        candidate_reader = self.load_csv(candidate_filename)

        party_turnouts = self.process_electorate_turnout(party_reader)
        candidate_turnouts = self.process_electorate_turnout(candidate_reader)

        electorates = []
        for party, candidate in zip(party_turnouts, candidate_turnouts):
            electorates.append(candidate | party)
        return electorates

    def get_electorate_party_results(self):
        results = []
        filename = os.path.join(
            self.data_path, self.electorate_votes_for_registered_parties)
        logging.info("loading electorate party results")
        reader = self.load_csv(filename)
        next(reader)
        parties = next(reader)[1:-2]
        while reader:
            row = next(reader)
            name = row[0]
            if (name == "General Electorate Totals"):
                continue
            if (name == "Maori Electorate Totals"):
                break
            for index, party_vote in enumerate(row[1:-2]):
                results.append({
                    "electorate": name,
                    "party": parties[index],
                    "votes": party_vote
                })
        return results

    def get_booth_results(self, electorate_number, party=False):
        if (party):
            filename = self.get_voting_place_party_filename(electorate_number)
            logging.info(
                f"loading party results by booth for electorate {electorate_number}")
        else:
            filename = self.get_voting_place_candidate_filename(
                electorate_number)
            logging.info(
                f"loading candidate results by booth for electorate {electorate_number}")

        reader = self.load_csv(filename)

        next(reader)
        electorate_header = next(reader)
        electorate = ' '.join(electorate_header[0].split()[:-1])
        candidates = next(reader)[2:-2]

        suburb = ''
        booths = []
        while reader:
            row = next(reader)
            if not row:
                continue
            if "less than 6" in row[1]:
                break
            if row[0]:
                suburb = row[0]
            booth = row[1]
            for i, votes in enumerate(row[2:-2]):
                booth_result = {
                    "electorate": electorate,
                    "name": booth,
                    "suburb": suburb,
                    "votes": votes
                }
                if party:
                    booth_result["party"] = candidates[i]
                else:
                    booth_result["candidate"] = candidates[i]
                booths.append(booth_result)

        while reader:
            row = next(reader)
            if row and "Electorate" in row[0]:
                break

        electorate_results = []
        for row in reader:
            if not row:
                continue
            if party:
                result = {
                    "electorate": electorate,
                    "party": row[0],
                    "votes": row[1],
                    "percentage": row[2]
                }
            else:
                result = {
                    "electorate": electorate,
                    "candidate": row[0],
                    "party": row[1],
                    "votes": row[2],
                    "percentage": row[3]
                }
            electorate_results.append(result)
        return booths, electorate_results

    def get_booths(self, electorate_count):
        candidate_booths = []
        party_booths = []

        candidate_electorate_results = []
        party_electorate_results = []

        for i in range(1, electorate_count+1):
            candidate_booth_results, electorate_candidate_results = self.get_booth_results(
                i)
            candidate_booths += candidate_booth_results
            candidate_electorate_results += electorate_candidate_results
            party_booth_results, electorate_party_result = self.get_booth_results(
                i, True)
            party_booths += party_booth_results
            party_electorate_results += electorate_party_result

        return candidate_booths, candidate_electorate_results, party_booths, party_electorate_results

    def extract_all(self):
        logging.info(f"beginning extraction for {self.year}")
        summary_total_votes, summary_party_votes = self.get_results_summary()
        self.write_csv("summary party votes", summary_party_votes)
        self.write_csv("summary total votes", summary_total_votes)

        electorates = self.get_electorates()
        self.write_csv("electorates", electorates)

        candidate_booths, candidate_electorate_results, party_booths, party_electorate_results = self.get_booths(
            len(electorates))
        self.write_csv("booth candidate results", candidate_booths)
        self.write_csv("booth party results", party_booths)
        self.write_csv("electorate candidate results",
                       candidate_electorate_results)
        self.write_csv("electorate party results",
                       party_electorate_results)
