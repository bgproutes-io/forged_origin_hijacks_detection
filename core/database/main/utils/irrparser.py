import networkx as nx
import re 

from colorama import Fore
from colorama import Style
from colorama import init
init(autoreset=True)

class ASset:
    def __init__(self, text: str):
        self.text = text

        # Variable in the as-set objects.
        self.as_set_name = None
        self.mnt_by = set()
        self.source = None
        self.changed = None

        self.as_members = set()
        self.as_members_rec = set()
        self.as_set_rec = set()

    def __str__(self):
        s = "{} {} {} {}\n{}\n{}\n{}".format( \
            self.as_set_name, \
            self.mnt_by, \
            self.source, \
            self.changed, \
            self.as_members, \
            self.as_members_rec, \
            self.as_set_rec)

        return s

    def init_metadata(self):

        for line in self.text.split('\n'):
            # Tranform multiple consecutive spaces into one space
            line = ' '.join(line.split())

            # Get the maintainer.
            if line.startswith('mnt-by'):
                if ' ' in line:
                    self.mnt_by.add(line.rstrip('\n').split(' ')[1])
                else:
                    self.mnt_by.add(line.rstrip('\n').replace('mnt-by: ', ''))

            if line.startswith('as-set'):
                self.as_set_name = line.rstrip('\n').replace('as-set: ', '')
                # print (self.as_set_name)
            
            if line.startswith('source'):
                self.source= line.rstrip('\n').replace('source: ', '')

            if line.startswith('changed'):
                self.changed = line.rstrip('\n').replace('changed: ', '')

    def get_members(self):
        members_line = False

        for line in self.text.split('\n'):

            if line.startswith(' '):
                subline = True
            else: subline = False

            # Tranform multiple consecutive spaces into one space
            line = ' '.join(line.split()).rstrip('\n')

            if line.startswith('members:') or (subline and members_line):
                members_line = True

                # Get the direct AS members.
                for as_mem in re.findall('AS[0-9]+[:]*', line):
                    if ":" not in as_mem: # To avoid matching on cases like: AS13826:AS-CUSTOMERS
                        self.as_members.add(as_mem)

                # Get the reference to other AS set objects.
                for as_set in re.findall('AS[a-zA-Z0-9:_.-]+', line):
                    if as_set not in self.as_members:
                        self.as_set_rec.add(as_set)
            else:
                members_line = False

    def get_as_set_members_recursively(as_set_name: str, as_set_dic: dict, max_rec: int=10):
        # Initialisation of the recursion.
        members_rec = set()
        as_set_next_iter = as_set_dic[as_set_name].as_set_rec
        as_set_already_iter = as_set_dic[as_set_name].as_set_rec
        nb_iter = 0

        while nb_iter < 10 and len(as_set_next_iter) > 0:
            # Update the set of AS sets already processed, to avoid processing twice the same AS set.
            as_set_already_iter = as_set_already_iter.union(as_set_next_iter)

            new_as_set_next_iter = set()

            # For every AS set in the batch for the current iteration.
            for as_set in as_set_next_iter:

                if as_set in as_set_dic:
                    # Add the members of this AS set.
                    members_rec = members_rec.union(as_set_dic[as_set].as_members)

                    # Add the AS set of this AS set in the next batch.
                    for as_set_tmp in as_set_dic[as_set].as_set_rec:
                        if as_set_tmp not in as_set_already_iter:
                            new_as_set_next_iter.add(as_set_tmp)

            # Prepare the next iteration.
            as_set_next_iter = new_as_set_next_iter
            nb_iter += 1

        return members_rec


class AutNum:
    def __init__(self, text: str):
        self.text = text

        # Variables in the aut-num objects.
        self.aut_num = None
        self.as_name = None
        self.mnt_by = set()
        self.source = None
        self.changed = None

    def __str__(self):
        s = "{} {} {} {} {}".format( \
            self.aut_num, \
            self.as_name, \
            self.mnt_by, \
            self.source, \
            self.changed)

        return s

    def init_metadata(self):

        for line in self.text.split('\n'):
            # Tranform multiple consecutive spaces into one space
            line = ' '.join(line.split())

            # Get the maintainer.
            if line.startswith('mnt-by'):
                if ' ' in line:
                    self.mnt_by.add(line.rstrip('\n').split(' ')[1])
                else:
                    self.mnt_by.add(line.rstrip('\n').replace('mnt-by: ', ''))

            if line.startswith('aut-num'):
                self.aut_num = line.rstrip('\n').replace('aut-num: ', '')

            if line.startswith('as-name'):
                self.as_name = line.rstrip('\n').replace('as-name: ', '')
            
            if line.startswith('source'):
                self.source= line.rstrip('\n').replace('source: ', '')

            if line.startswith('changed'):
                self.changed = line.rstrip('\n').replace('changed: ', '')

    def get_links(self, as_set_dic):
        import_line = False
        export_line = False

        cur_as = self.aut_num.replace('AS', '')

        for line in self.text.split('\n'):

            if line.startswith(' '):
                subline = True
            else: subline = False

            # Tranform multiple consecutive spaces into one space
            line = ' '.join(line.split()).rstrip('\n')

            if line.startswith('import:') or line.startswith("mp-import") or (subline and import_line):
                import_line = True
                export_line = False

                # Find direct AS numbers.
                as_list = set(map(lambda x:x.replace(' ', ''), re.findall(' AS[0-9]+ ', line)))
                for asn in as_list:
                    yield (cur_as, int(asn.replace('AS', '')), 0)

                # Recusively find ases in as-set.
                as_object_list = re.findall('to AS[a-zA-Z0-9:_.-]+ ', line)
                for obj in as_object_list:
                    # Remove direct AS labels.
                    obj_name = obj.replace('to ', '').replace(' ', '')

                    if obj_name not in as_list and obj_name != 'AS-ANY':
                        # Return all the links infered from the AS set object.
                        if obj_name in as_set_dic:
                            for as_member in as_set_dic[obj_name].as_members:
                                yield (cur_as, int(as_member.replace('AS', '')), 1)
                            
                            for as_member in as_set_dic[obj_name].as_members_rec:
                                yield (cur_as, int(as_member.replace('AS', '')), 1)

            elif line.startswith('export:') or line.startswith("mp-export") or (subline and export_line):
                export_line = True
                import_line = False

                # Find direct AS numbers.
                as_list = set(map(lambda x:x.replace(' ', ''), re.findall(' AS[0-9]+ ', line)))
                for asn in as_list:
                    yield (cur_as, int(asn.replace('AS', '')), 0)

                # Recusively find ases in as-set.
                as_object_list = re.findall('to AS[a-zA-Z0-9:_.-]+ ', line)
                for obj in as_object_list:
                    # Remove direct AS labels.
                    obj_name = obj.replace('to ', '').replace(' ', '')

                    if obj_name not in as_list and obj_name != 'AS-ANY':
                        # Return all the links infered from the AS set object.
                        if obj_name in as_set_dic:
                            for as_member in as_set_dic[obj_name].as_members:
                                yield (cur_as, int(as_member.replace('AS', '')), 1)
                            
                            for as_member in as_set_dic[obj_name].as_members_rec:
                                yield (cur_as, int(as_member.replace('AS', '')), 1)

            else:
                import_line = False
                export_line = False


def print_prefix():
    return Fore.MAGENTA+Style.BRIGHT+"[irrparser.py]: "+Style.NORMAL


def parse_irr_snapshot(infile_list: list, outfile: str):
    # First, process the as-set objects and infer their members, recursively.
    as_set_dic = {}

    print (print_prefix()+"Reading the as-set objects.")
    for infile in infile_list:
        print (print_prefix()+infile)
        with open(infile, 'r', encoding='utf-8', errors='ignore') as fd:
            for obj in fd.read().split('\n\n'):
                if 'as-set:' in obj:
                    as_set = ASset(obj)
                    as_set.init_metadata()
                    as_set.get_members()
                    as_set_dic[as_set.as_set_name] = as_set

    print (print_prefix()+'Build as-set objects recursively.')
    # Run the recursive ASset membership inference.
    for as_set_name, as_set in as_set_dic.items():
        as_set.as_members_rec = ASset.get_as_set_members_recursively(as_set_name, as_set_dic, max_rec=10)

    # Second, process the aut-num objects and infer the AS topologly.
    aut_num_list = []
    topo = nx.DiGraph()

    print (print_prefix()+"Constructing the topology.")    
    for infile in infile_list:
        print (print_prefix()+infile)
        with open(infile, 'r', encoding='utf-8', errors='ignore') as fd:
            for obj in fd.read().split('\n\n'):
                if obj.startswith('aut-num:'):
                    autnum = AutNum(obj)
                    autnum.init_metadata()
                    for (as1, as2, t) in autnum.get_links(as_set_dic):
                        topo.add_edge(as1, as2)

    print (print_prefix()+"IRR topo size {} nodes and {} edges".format(topo.number_of_nodes(), topo.number_of_edges()))

    # Write the resulting graph in the output file.
    with open(outfile, 'w') as fd:
        for as1, as2 in topo.edges():
            fd.write('{} {}\n'.format(as1, as2))



if __name__ == "__main__":
    # parse_irr_snapshot("../raw/db/irr/2021-05-15_irr_radb.txt")
    irr_list = ['level3', 'radb', 'wcgdb', 'afrinic', 'altdb', 'apnic', 'arin', 'jpirr', 'lacnic', 'nttcom', 'reach', 'ripe', 'tc']
    irr_list = list(map(lambda x:"../raw/db/irr/2022-08-05_irr_"+str(x)+".txt", irr_list))
    print (irr_list)
        # parse_irr_snapshot(["../raw/db/irr/2021-05-15_irr_level3.txt", "../raw/db/irr/2021-05-15_irr_radb.txt"])
    parse_irr_snapshot(irr_list)
