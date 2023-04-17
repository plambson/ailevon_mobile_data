import argparse
import csv
from collections import Counter, defaultdict


parser = argparse.ArgumentParser(description="Mobile Data Combiner",
                                 formatter_class=argparse.ArgumentDefaultsHelpFormatter)
parser.add_argument("-d", "--CEL_file", help="details file name")
parser.add_argument("-e", "--Estimate_file", help="estimates file")
parser.add_argument("-cbsa", "--CBSA_file", help="CBSA file")
parser.set_defaults(CEL_file='details.tsv',
                    Estimate_file='estimates.tsv',
                    CBSA_file='Zip to CBSA.csv')
args = parser.parse_args()
config = vars(args)


class MobileDataParser:
    def __init__(self, CEL_file, Estimate_file, CBSA_file):
        self.CEL_file = CEL_file
        self.Estimate_file = Estimate_file
        self.CBSA_file = CBSA_file
        self.crosswalk = {}
        self.estimate_dicts = dict()
        self.CEL = list()
        self.zip1 = defaultdict(list)
        self.zip1_summary = dict()
        self.zip2 = defaultdict(list)
        self.zip2_summary = dict()
        self.not_in_file_count = 0
        self.no_msa_count = 0
        self.observation_summary = {}
        self.out = False
        self.out_file_name = 'out.csv'
        self.zip1_file_name = 'zip1.csv'
        self.zip2_file_name = 'zip2.csv'

    def parse_crosswalk(self):
        """
        Read in the CBSA file, do some slight modifications to the
        ZIP field to add trailing zeros
        :return: self.crosswalk
        """
        with open(self.CBSA_file, encoding="utf8") as file:
            tsv_file = csv.DictReader(file, delimiter=",")

            for line in tsv_file:
                if len(line['ZIP']) < 5:
                    line['ZIP'] = (5 - len(line['ZIP'])) * '0' + line['ZIP']
                self.crosswalk[line['ZIP']] = line
        assert len(self.crosswalk) > 0, "CBSA File Not Loaded"
        print(f'CBSA File loaded with {len(self.crosswalk)} records')

    def parse_estimates(self):
        """
        Read in the estimates file arranged in a multiple level dictionary
        [polygon_id][local_date][estimated_visitors]
        :return: self.estimate_dicts
        """
        with open(self.Estimate_file, encoding="utf8") as file:
            tsv_file = csv.DictReader(file, delimiter="\t")
            try:
                for line in tsv_file:
                    if line['polygon_id'] in self.estimate_dicts:
                        self.estimate_dicts[line['polygon_id']][line['local_date']] = line['estimated_visitors']
                    else:
                        self.estimate_dicts[line['polygon_id']] = {line['local_date']: line['estimated_visitors']}
            except:
                raise ValueError('Estimates File is not as expected.')
        assert len(self.estimate_dicts) > 0, "Estimates File Not Loaded"
        print(f'Estimates file loaded with {len(self.estimate_dicts)} Polygons')

    def parse_CEL(self):
        '''
        Parse the full detailed file, apply MSA when possible and apply estimated visits when possible.
        Keeep track of how many times MSA or Estimates where not available
        :return: self.CEL
        '''
        with open(self.CEL_file, encoding="utf8") as file:
            reader = csv.DictReader(file, delimiter="\t")
            for dictionary in reader:
                try:
                    dictionary['CBSA'] = self.crosswalk[dictionary['Common Evening Postal1']]['Simplified CBSA']
                except:
                    dictionary['CBSA'] = 'NO MSA ASSIGNED'
                    self.no_msa_count += 1
                try:
                    dictionary['estimated_visitors'] = self.estimate_dicts[dictionary['Polygon Id']][
                        dictionary['Visit Date']]
                except:
                    self.not_in_file_count += 1
                self.CEL.append(dictionary)
        print(f'Details files loaded with {len(self.CEL)} records')

    def make_observation_summary(self):
        """
        Make a dict of count of observations in Polygons and Visit Dates in the CEL file.
        A multi-level dictionary with level of [Polygon Id][Visit Date]
        :return: self.observation_summary
        """
        for polygon in set([item['Polygon Id'] for item in self.CEL]):
            subcel = [observation for observation in self.CEL if observation['Polygon Id'] == polygon]
            self.observation_summary[polygon] = dict(Counter(item['Visit Date'] for item in subcel))

    def make_final_calcs(self):
        """
        Do calculations at an observation level, applying the logic at polygon visit date level.
        Add date-parts.
        :return:
        """
        for idx, observation in enumerate(self.CEL):
            observed_visits = self.observation_summary[observation['Polygon Id']][observation['Visit Date']]
            self.CEL[idx]['observed_visits'] = observed_visits
            try:
                near_estimated = int(observation['estimated_visitors']) / observation['observed_visits']
                self.CEL[idx]['near_estimated_visits'] = near_estimated
                self.zip1[observation['Common Evening Postal1']].append(self.CEL[idx]['near_estimated_visits'])
                self.zip2[observation['Common Evening Postal2']].append(self.CEL[idx]['near_estimated_visits'])
            except:
                pass
            date_parts = observation['Visit Date'].split('-')
            self.CEL[idx]['year'] = date_parts[0]
            self.CEL[idx]['month'] = date_parts[1]
            self.CEL[idx]['day'] = date_parts[2]
            if idx % 250000 == 0:
                print(f'Final Calcs Calculated {idx}')

    def summarize_zips(self):
        out = dict()
        for zip1 in self.zip1.keys():
            out[zip1] = sum(self.zip1[zip1])
        self.zip1_summary = out
        out = dict()
        for zip2 in self.zip2.keys():
            out[zip2] = sum(self.zip2[zip2])
        self.zip2_summary = out

    def print_report(self):
        print('Final Report')
        print("------------------------------------------------")
        print(f"    * Count of Detailed Records - {len(self.CEL)}")
        print(f"    * Count of No MSA - {self.no_msa_count}")
        print(f"    * Count of No Estimates - {self.not_in_file_count}")

    def write_out(self):
        print('Writing Output file...')
        with open(self.out_file_name, 'w') as file:
            fieldnames = list(self.CEL[0].keys())
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(self.CEL)
        self.out = True
        with open(self.zip1_file_name, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['zip1', 'near_estimated_visits'])
            for key, value in self.zip1_summary.items():
                writer.writerow([key, value])
        with open(self.zip2_file_name, 'w') as csv_file:
            writer = csv.writer(csv_file)
            writer.writerow(['zip2', 'near_estimated_visits'])
            for key, value in self.zip2_summary.items():
                writer.writerow([key, value])

    def start(self):
        print("Starting...")
        self.parse_crosswalk()
        self.parse_estimates()
        self.parse_CEL()
        self.make_observation_summary()
        self.make_final_calcs()
        self.summarize_zips()
        self.write_out()
        print("Completed!")
        self.print_report()

data_object = MobileDataParser(**config)
data_object.start()
del (data_object)
