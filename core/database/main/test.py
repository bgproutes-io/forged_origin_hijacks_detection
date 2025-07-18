import bgpkit

parser = bgpkit.Parser(url="https://data.ris.ripe.net/rrc24/2022.11/updates.20221105.1045.gz",
                       filters={"peer_ips": "185.1.8.65, 2001:7f8:73:0:3:fa4:0:1"})
elems = parser.parse_all()

print (elems)