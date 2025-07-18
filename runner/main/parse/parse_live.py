import time
from os import listdir
from os.path import isfile, join
from datetime import datetime

def parselive_prefix_to_asns(db_dir: str, date: str):
    # Load the details for every new edge case.        
    infile_prefixes = '{}/prefixes/{}.txt'.format(db_dir, date.replace(day=1).strftime("%Y-%m-%d"))
    prefix_to_asns = {}
    with open(infile_prefixes, 'r') as fd:
        for line in fd.readlines():
            linetab = line.rstrip('\n').split(' ')
            prefix = linetab[0]
            
            if prefix not in prefix_to_asns:
                prefix_to_asns[prefix] = set()
            
            for asn in linetab[1].split(','):
                prefix_to_asns[prefix].add(int(asn))

    return prefix_to_asns

def parselive(inference_str, timestamp, prefix_to_asns, newedge_info, is_recurrent, conn, outfile):
    dic_res = {}
    dic_tags = {}

    for line in inference_str.split('\n'):
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
            
            # Tag recurrence.
            dic_tags[(as1, as2)]['recurrent'] = is_recurrent


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
        for peer_ip, peer_asn, origin, prefix, ts, aspath in newedge_info:
            dic_tags[(as1, as2)]['peerasn'].add(peer_asn)

        # Tag valid origin.
        valid_origin = True
        nb_processed = 0
        for peer_ip, peer_asn, origin, prefix, ts, aspath in newedge_info:
            nb_processed += 1
            if prefix in prefix_to_asns and origin not in prefix_to_asns[prefix]:
                valid_origin = False
                break

            # Only check 1000 cases to avoid looping for too long.. (and 1000 should be largely enough).
            if nb_processed == 1000:
                break

        if 'invalid_origin' not in dic_tags[(as1, as2)]:
            dic_tags[(as1, as2)]['invalid_origin'] = False
        dic_tags[(as1, as2)]['invalid_origin'] = dic_tags[(as1, as2)]['invalid_origin'] or not valid_origin


    # Finishing with tag local events.
    for (as1, as2) in dic_tags:
        dic_tags[(as1, as2)]['local'] = False
        # If there is only one attacker and only VPs in one AS that saw the event.
        if len(dic_tags[(as1, as2)]['attackers']) == 1 and \
            len(dic_tags[(as1, as2)]['peerasn']) == 1:

            # If the VP that sees the event is in the attacker AS.
            if len(dic_tags[(as1, as2)]['peerasn'].intersection(dic_tags[(as1, as2)]['attackers'])) == 1:
                dic_tags[(as1, as2)]['local'] = True

    nb_sus = 0
    nb_leg = 0

    fd_out = open(outfile, 'a+', 1)
    for as1, as2 in dic_res:
        sus = 0
        leg = 0
        asp_count = 0
        
        for sensitivity in dic_res[(as1, as2)]:
            if dic_res[(as1, as2)][sensitivity].count(0) > dic_res[(as1, as2)][sensitivity].count(1):
                leg += 1
            else:
                sus += 1
            asp_count = dic_res[(as1, as2)][sensitivity].count(0) + dic_res[(as1, as2)][sensitivity].count(1)
        
        # Write tags:
        s = ''
        for tags in dic_tags[(as1, as2)]:
            if tags == 'peerasn':
                continue

            if tags in ['type']:
                stmp = ''
                for e in dic_tags[(as1, as2)][tags]:
                    stmp += str(e)+','
                s += tags+':'+stmp[:-1]+';'

            else:
                s += tags+':'+str(dic_tags[(as1, as2)][tags])+';'

        if sus == 0:
            if outfile is not None:
                fd_out.write('!leg {} {} {} {} {} {}\n'.format(as1, as2, leg, sus, asp_count, s[:-1]))

            nb_leg += 1
        else:
            if outfile is not None:
                fd_out.write('!sus {} {} {} {} {} {}\n'.format(as1, as2, leg, sus, asp_count, s[:-1]))
            nb_sus += 1

        # Add the case in the database.
        if conn is not None:
            sql = '''INSERT INTO cases(as1,as2,nb_leg,nb_sus,date,recurrent,type,invalid_origin,local,nbpaths,attackers,victims)
            VALUES({},{},{},{},{},{},{},{},{},{},{},{}) '''.format( \
            as1, \
            as2, \
            leg, \
            sus, \
            timestamp, \
            dic_tags[(as1, as2)]['recurrent'], \
            '\"{}\"'.format(' '.join(list(map(lambda x:str(x), list(dic_tags[(as1, as2)]['type']))))), \
            dic_tags[(as1, as2)]['invalid_origin'], \
            dic_tags[(as1, as2)]['local'], \
            len(newedge_info), \
            '\"{}\"'.format(' '.join(list(map(lambda x:str(x), list(dic_tags[(as1, as2)]['attackers']))))), \
            '\"{}\"'.format(' '.join(list(map(lambda x:str(x), list(dic_tags[(as1, as2)]['victims']))))), \
            )

            cur = conn.cursor()
            cur.execute(sql)
            case_id = cur.lastrowid
            conn.commit()
        else:
            case_id = None
            print ('No connection for database\n')

        # Write inference result for every sensitivity.
        for sensitivity in dic_res[(as1, as2)]:
            fd_out.write("{} {} {} {} {}\n".format( \
                as1, \
                as2, \
                sensitivity, \
                dic_res[(as1, as2)][sensitivity].count(0), \
                dic_res[(as1, as2)][sensitivity].count(1)))
            
    fd_out.close()
    return nb_leg, nb_sus, case_id