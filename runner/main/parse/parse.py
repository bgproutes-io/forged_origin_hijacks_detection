import os
from os import listdir
from os.path import isfile, join
from datetime import datetime
import psycopg2
import pytricia
import pandas as pd


class Parser:
    def __init__(self, db_dir: str, date: str, store_results_in_db: bool, store_results_in_file: bool):
        self.db_dir = db_dir
        self.results_db_config = {
            'dbname': os.getenv('DFOH_DB_NAME'),
            'user': os.getenv('DFOH_DB_USER'),
            'password': os.getenv('DFOH_DB_PWD'),
            'host': 'localhost',
            'port': 5432
        } if store_results_in_db else None
        self.date = date
        self.write_cases_file = store_results_in_file

        # Load the details for every new edge case.        
        infile_prefixes = '{}/prefixes/{}.txt'.format(db_dir, date.strftime("%Y-%m-%d"))
        self.roas_v4 = pytricia.PyTricia()
        self.roas_v6 = pytricia.PyTricia(128)
        df_roas = pd.read_csv(infile_prefixes, dtype=str, sep=' ')
        for _, row in df_roas.iterrows():
            prefix = row["IP Prefix"]
            asn = row["ASN"]
            max_length = int(row["Max Length"])
            if ':' in prefix:  # IPv6 prefix
                if not self.roas_v6.has_key(prefix):
                    self.roas_v6[prefix] = {}
                self.roas_v6[prefix][asn] = max_length
            else:  # IPv4 prefix
                if not self.roas_v4.has_key(prefix):
                    self.roas_v4[prefix] = {}
                self.roas_v4[prefix][asn] = max_length

        self.dic_new_edges = {}
        self.dic_rec = {}
        self.new_edge_origin = {}

        if self.results_db_config:
            conn = psycopg2.connect(**self.results_db_config)
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT asn1, asn2, as_path, is_recurrent, prefix, peer_ip, peer_asn, is_recurrent
                    FROM new_link
                    WHERE DATE(observed_at) = %s
                """, (self.date.strftime("%Y-%m-%d"),))
                for row in cur.fetchall():
                    as1, as2, as_path, is_recurrent, prefix, peer_ip, peer_asn, is_recurrent = row

                    prefix = str(prefix)

                    # Make sure as1 is the lowest number.
                    if as1 > as2:
                        as1, as2 = as2, as1

                    as1 = str(as1)
                    as2 = str(as2)

                    if (as1, as2) not in self.dic_new_edges:
                        self.dic_new_edges[(as1, as2)] = set()
                        self.new_edge_origin[(as1, as2)] = set()

                    self.dic_rec[(as1, as2)] = is_recurrent
                    self.dic_new_edges[(as1, as2)].add((peer_ip, peer_asn))
                    self.new_edge_origin[(as1, as2)].add((as_path.split('|')[-1], prefix))
            conn.close()

        else:
            with open('{}/new_edge/{}.txt'.format(self.db_dir, self.date.strftime("%Y-%m-%d"))) as fd:
                for line in fd.readlines():
                    if line.startswith('#'):
                        continue

                    linetab = line.rstrip('\n').split(',')
                    as1 = linetab[0].split(' ')[0]
                    as2 = linetab[0].split(' ')[1]
                    asp = linetab[1].split(' ')
                    recurrent_case = linetab[3]

                    # Make sure as1 is the lowest number.
                    if int(as1) > int(as2):
                        as1, as2 = as2, as1

                    if (as1, as2) not in self.dic_new_edges:
                        self.dic_new_edges[(as1, as2)] = set()
                        self.new_edge_origin[(as1, as2)] = set()

                    if len(linetab) > 2:
                        info = linetab[2]
                        prefix = info.split('-')[1]
                        peerip = info.split('-')[2]
                        peerasn = info.split('-')[3]

                        self.dic_rec[(as1, as2)] = recurrent_case
                        self.dic_new_edges[(as1, as2)].add((peerip, peerasn))
                        self.new_edge_origin[(as1, as2)].add((asp[-1], prefix))

    def is_origin_valid(self, origin_asn, prefix):
        """
        Determine if a given origin is valid for a given prefix.

        Parameters:
        - origin_asn: ASN as a string (e.g., '64500')
        - prefix: prefix as a string (e.g., '203.0.113.0/24')
        - prefix_tree: a PyTricia tree containing the RPKI data

        Returns:
        - True if the origin ASN is valid for the prefix, False otherwise.
        """
        origin_asn = str(origin_asn)
        prefix = str(prefix)
        prefix_tree = self.roas_v6 if ':' in prefix else self.roas_v4

        # 1. Check for exact match
        if prefix_tree.has_key(prefix):
            node_data = prefix_tree[prefix]
            if origin_asn in node_data:
                return True
            return False

        # 2. Check for less-specific match
        covering_prefix = prefix_tree.get_key(prefix)
        while covering_prefix is not None:
            node_data = prefix_tree[covering_prefix]
            if origin_asn in node_data: # As long as the origin ASN is valid, we consider it valid
                    return True
            covering_prefix = prefix_tree.parent(covering_prefix)

        return False

    def parse(self):
        infile = "{}/cases/{}.tmp".format(self.db_dir, self.date.strftime("%Y-%m-%d"))

        dic_res = {}
        dic_tags = {}

        with open(infile, 'r') as fd:
            for line in fd.readlines():
                linetab = line.rstrip('\n').split(' ')
                as1 = linetab[0]
                as2 = linetab[1]

                # Make sure as1 is the lowest number.
                if int(as1) > int(as2):
                    as1, as2 = as2, as1

                asp = linetab[2].split('|')
                label = int(linetab[3])
                proba = linetab[4]
                sensitivity = linetab[5]

                if (as1, as2) not in dic_res:
                    dic_res[(as1, as2)] = {}
                if sensitivity not in dic_res[(as1, as2)]:
                    dic_res[(as1, as2)][sensitivity] = []
                dic_res[(as1, as2)][sensitivity].append(label)

                # Build the tags.
                if (as1, as2) not in dic_tags:
                    dic_tags[(as1, as2)] = {}

                # Tag Attacker/Victim.
                if 'attackers' not in dic_tags[(as1, as2)]:
                    dic_tags[(as1, as2)]['attackers'] = set()
                if 'victims' not in dic_tags[(as1, as2)]:
                    dic_tags[(as1, as2)]['victims'] = set()

                dic_tags[(as1, as2)]['victims'].add(asp[-1])

                if asp.index(as1) < asp.index(as2):
                    dic_tags[(as1, as2)]['attackers'].add(as1)
                else:
                    dic_tags[(as1, as2)]['attackers'].add(as2)

                # Type hijack type.
                hijack_type = len(asp)-min(asp.index(as1), asp.index(as2))-1
                if 'type' not in dic_tags[(as1, as2)]:
                    dic_tags[(as1, as2)]['type'] = set()
                dic_tags[(as1, as2)]['type'].add(hijack_type)

                # Tag local event.
                if 'peerasn' not in dic_tags[(as1, as2)]:
                    dic_tags[(as1, as2)]['peerasn'] = set()
                for peerinfo in self.dic_new_edges[(as1,as2)]:
                    dic_tags[(as1, as2)]['peerasn'].add(peerinfo[1])
            
                # Tag valid origin.
                valid_origin = True
                nb_processed = 0
                for origin, prefix in self.new_edge_origin[(as1, as2)]:
                    nb_processed += 1
                    if not self.is_origin_valid(origin, prefix):
                        valid_origin = False
                        break

                    # Only check 1000 cases to avoid looping for too long.. (and 1000 should be largely enough).
                    if nb_processed == 1000:
                        break

                if 'valid_origin' not in dic_tags[(as1, as2)]:
                    dic_tags[(as1, as2)]['valid_origin'] = set([valid_origin])

                # Tag recurrence.
                if (as1, as2) in self.dic_rec:
                    dic_tags[(as1, as2)]['recurrent'] = [self.dic_rec[(as1, as2)]]
                else:
                    print ('Problem parsing {} {}'.format(as1, as2))

            # Finishing with tag local events.
            for (as1, as2) in dic_tags:
                dic_tags[(as1, as2)]['local'] = [False]
                # If there is only one attacker and only VPs in one AS that saw the event.
                if len(dic_tags[(as1, as2)]['attackers']) == 1 and \
                    len(dic_tags[(as1, as2)]['peerasn']) == 1:

                    # If the VP that sees the event is in the attacker AS.
                    if len(dic_tags[(as1, as2)]['peerasn'].intersection(dic_tags[(as1, as2)]['attackers'])) == 1:
                        dic_tags[(as1, as2)]['local'] = [True]
                        

        # Prepare file and database connections outside the loop
        fd_out = None
        conn :psycopg2.extensions.connection = None
        cursor = None

        try:
            # Setup file writing if needed
            if self.write_cases_file:
                outfile = "{}/cases/{}".format(self.db_dir, self.date.strftime("%Y-%m-%d"))
                fd_out = open(outfile, 'w', 1)
            
            # Setup database connection if needed
            if self.results_db_config:
                conn = psycopg2.connect(**self.results_db_config)
                cursor = conn.cursor()
            
            # Single loop through dic_res
            for as1, as2 in dic_res:
                sus = 0
                leg = 0
                asp_count = 0
                confidence_level = [0]
                
                # Calculate statistics once per edge pair
                for sensitivity in dic_res[(as1, as2)]:
                    count_0 = dic_res[(as1, as2)][sensitivity].count(0)
                    count_1 = dic_res[(as1, as2)][sensitivity].count(1)

                    if count_1:
                        confidence_level.append(int(sensitivity))
                    
                    if count_0 > count_1:
                        leg += 1
                    else:
                        sus += 1
                    asp_count = count_0 + count_1
                
                # Write to file if enabled
                if fd_out:
                    # Build tags string
                    s = ''
                    for tags in dic_tags[(as1, as2)]:
                        if tags == 'peerasn':
                            continue
                        
                        stmp = ''
                        for e in dic_tags[(as1, as2)][tags]:
                            stmp += str(e) + ','
                        s += tags + ':' + stmp[:-1] + ';'
                    
                    # Write summary line
                    if sus == 0:
                        fd_out.write('!leg {} {} {} {} {} {}\n'.format(as1, as2, leg, sus, asp_count, s[:-1]))
                    else:
                        fd_out.write('!sus {} {} {} {} {} {}\n'.format(as1, as2, leg, sus, asp_count, s[:-1]))
                    
                    # Write inference result for every sensitivity
                    for sensitivity in dic_res[(as1, as2)]:
                        fd_out.write("{} {} {} {} {}\n".format(
                            as1,
                            as2,
                            sensitivity,
                            dic_res[(as1, as2)][sensitivity].count(0),
                            dic_res[(as1, as2)][sensitivity].count(1)
                        ))
                
                # Insert to database if enabled
                if cursor:
                    # Insert summary record
                    cursor.execute("""
                        INSERT INTO inference_summary (
                            asn1, asn2, classification, num_legit_inf, num_susp_inf, num_paths,
                            attackers, victims, hijack_types, is_origin_rpki_valid, is_recurrent, is_local, observed_at, confidence_level
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        RETURNING id
                    """, (
                        int(as1), int(as2), 'sus' if sus > 0 else 'leg', leg, sus, asp_count,
                        [int(asn) for asn in dic_tags[(as1, as2)]['attackers']],
                        [int(asn) for asn in dic_tags[(as1, as2)]['victims']],
                        [int(hijack_type) for hijack_type in dic_tags[(as1, as2)]['type']],
                        bool(list(dic_tags[(as1, as2)]['valid_origin'])[0]),
                        bool(list(dic_tags[(as1, as2)]['recurrent'])[0]),
                        bool(list(dic_tags[(as1, as2)]['local'])[0]),
                        self.date.strftime("%Y-%m-%d"),
                        max(confidence_level)
                    ))
                    
                    inference_id = cursor.fetchone()[0]
                    
                    # Insert detail records for each sensitivity
                    for sensitivity in dic_res[(as1, as2)]:
                        cursor.execute("""
                            INSERT INTO inference_details (inference_id, model_id, num_legit_paths, num_susp_paths)
                            VALUES (%s, %s, %s, %s)
                        """, (
                            inference_id, 
                            sensitivity, 
                            dic_res[(as1, as2)][sensitivity].count(0), 
                            dic_res[(as1, as2)][sensitivity].count(1)
                        ))
                    
                    # Update new_link table
                    cursor.execute("""
                        UPDATE new_link
                        SET inference_id = %s, confidence_level = %s
                        WHERE (
                            (asn1 = %s AND asn2 = %s) OR
                            (asn1 = %s AND asn2 = %s)
                        )
                        AND DATE(observed_at) = %s
                    """, (inference_id, max(confidence_level), int(as1), int(as2), int(as2), int(as1), self.date.strftime("%Y-%m-%d")))
            
            # Commit database changes
            if conn:
                conn.commit()

        except Exception as e:
            print(f"Error during processing: {e}")
            if conn:
                conn.rollback()
            raise

        finally:
            # Clean up resources
            if fd_out:
                fd_out.close()
            if conn:
                conn.close()

# def parse_dir(indir, outdir):
#     onlyfiles = [f for f in listdir(indir) if isfile(join(indir, f))]

#     for filename in onlyfiles:
#         parse('{}/{}'.format(indir, filename), '{}/{}'.format(outdir, filename))        


def launch_parser(\
    db_dir, \
    date, \
    store_results_in_db, \
    store_results_in_file):
    """
    This script parses the outfile provided by the broker.
    In case a directory is given as input, it parsed all the files in the directory.
    """

    p = Parser(db_dir, date, store_results_in_db, store_results_in_file)
    p.parse()
