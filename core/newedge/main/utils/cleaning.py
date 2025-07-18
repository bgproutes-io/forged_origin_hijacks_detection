# Remove 
# - as PATH prepending
# - wrong ASN numbers (privates ones)
# - IXP AS numbers

# ixps = set([1200, 4635, 5507, 6695, 7606, \
#     8714, 9355, 9439, 9560, 9722, \
#     9989, 11670, 15645, 17819, 18398, \
#     21371, 24029, 24115, 24990, 35054, \
#     40633, 42476, 43100, 47886, 48850, \
#     50384, 55818, 57463])

# This function reads the list of IXP that is
# supposed to be in the database already.


def remove_asprepending(aspath, ixps):
    while (len(aspath) > 0 and ((aspath[0] > 64496 and aspath[0] < 131071) or aspath[0] > 4200000000 or aspath[0] in ixps)):
        aspath = aspath[1:]
    
    if len(aspath) > 0:
        tmp = [aspath[0]]

        for asn in aspath[1:]:
            if asn != tmp[-1] \
                and not ((asn > 64496 and asn < 131071) or asn > 4200000000 or asn in ixps):
                tmp.append(asn)

        return tmp
    else:
        return aspath

if __name__ == "__main__":
    aspath = [24029, 50384, 1,2,3,4,5,5,5,70000,70000,5,24115, 10,11]
    print (remove_asprepending(aspath))
    aspath = [24029, 50384]
    print (remove_asprepending(aspath))