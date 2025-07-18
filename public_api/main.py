from fastapi import FastAPI, HTTPException
from typing import List
from pydantic import BaseModel
from datetime import datetime
import psycopg
import os
import ipaddress

app = FastAPI()

MAX_LIST_LENGTH = 10  # Maximum length for lists in queries

# === PostgreSQL connection details ===
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "db_name"),
    "user": os.getenv("DB_USER", "db_user"),
    "password": os.getenv("DB_PASSWORD", "db_pwd"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": 5432
}

class NewLink(BaseModel):
    observed_at: str
    prefix: str
    as_path: str
    vantage_point: str

class Inference(BaseModel):
    date: str
    link: str
    num_paths: int
    classification: str
    confidence: int
    new_links: List[NewLink]

@app.get("/forged_origin_hijacks", response_model=List[Inference])
def get_hijacks(
    asn: str = None, 
    attackers: str = None,
    victims: str = None,
    classification: str = None,
    hijack_type: str = None,
    is_origin_rpki_valid: str = None,
    is_local: str = None,
    is_recurrent: str = None,
    prefixes: str = None,
    prefix_match_type: str = "exact",
    peer_ips: str = None,
    peer_asns: str = None,
    before_datetime: str = None,
    after_datetime: str = None,
    path_contains: str = None
):
    query = """
        SELECT 
            i.id,
            DATE(n.observed_at) as date,
            CONCAT(i.asn1, ' ', i.asn2) as link,
            i.num_paths,
            i.classification,
            i.num_susp_inf,
            json_agg(json_build_object(
                'observed_at', to_char(n.observed_at, 'YYYY-MM-DD HH24:MI:SS'),
                'prefix', n.prefix,
                'as_path', n.as_path,
                'vantage_point', CONCAT(n.peer_asn, ' ', n.peer_ip)
            )) as new_links
        FROM inference_summary i
        JOIN new_link n ON n.inference_id = i.id
    """

    query_list = []
    params = []
    if asn:
        asn_list = asn.split(",")
        # Validate ASN
        if len(asn_list) > MAX_LIST_LENGTH:
            raise HTTPException(status_code=400, detail=f"asn list cannot exceed {MAX_LIST_LENGTH} items")
        if not all(asn.isdigit() and 0 <= int(asn) < 4294967296 for asn in asn_list):
            raise HTTPException(status_code=400, detail="asn must be a comma-separated list of valid AS numbers (0-4294967295)")
        
        # Adjust query to handle ASNs
        query_list.append("(i.asn1 = ANY(%s) OR i.asn2 = ANY(%s))")
        params.extend([asn_list, asn_list])

    if attackers:
        attackers = attackers.split(",")

        # Validate attackers
        if len(attackers) > MAX_LIST_LENGTH:
            raise HTTPException(status_code=400, detail=f"attackers list cannot exceed {MAX_LIST_LENGTH} items")
        if not all(attacker.isdigit() and 0 <= int(attacker) < 4294967296 for attacker in attackers):
            raise HTTPException(status_code=400, detail="attackers must be a comma-separated list of valid AS numbers (0-4294967295)")
        
        # Adjust query to handle attackers
        query_list.append("EXISTS (SELECT 1 FROM inference_details d WHERE d.inference_id = i.id AND d.attacker = ANY(%s))")
        params.append(attackers)

    if victims:
        victims = victims.split(",")
        
        # Validate victims
        if len(victims) > MAX_LIST_LENGTH:
            raise ValueError(f"victims list cannot exceed {MAX_LIST_LENGTH} items")
        if not all(victim.isdigit() and 0 <= int(victim) < 4294967296 for victim in victims):
            raise HTTPException(status_code=400, detail="victims must be a comma-separated list of valid AS numbers (0-4294967295)")
        
        # Adjust query to handle victims
        query_list.append("EXISTS (SELECT 1 FROM inference_details d WHERE d.inference_id = i.id AND d.victim = ANY(%s))")
        params.append(victims)

    if classification:
        # Validate classification
        if classification not in ['leg', 'sus']:
            raise HTTPException(status_code=400, detail="classification must be 'leg' or 'sus'")
        
        # Adjust query to handle classification
        query_list.append("i.classification = %s")
        params.append(classification)

    if hijack_type:
        # Validate hijack_type
        if not hijack_type.isdigit():
            raise HTTPException(status_code=400, detail="hijack_type must be a valid integer")
        
        # Adjust query to handle hijack_type
        query_list.append("i.hijack_type = %s")
        params.append(int(hijack_type))

    if is_origin_rpki_valid:
        # Validate exclude_valid
        if is_origin_rpki_valid.lower() == "true":
            is_origin_rpki_valid = True
        elif is_origin_rpki_valid.lower() == "false":
            is_origin_rpki_valid = False
        else:
            raise HTTPException(status_code=400, detail="exclude_valid must be 'true' or 'false'")
        
        # Adjust query to exclude valid origins
        if is_origin_rpki_valid:
            query_list.append("i.is_origin_rpki_valid")
        else:
            query_list.append("NOT i.is_origin_rpki_valid")

    if is_local:
        # Validate exclude_local
        if is_local.lower() == "true":
            is_local = True
        elif is_local.lower() == "false":
            is_local = False
        else:
            raise HTTPException(status_code=400, detail="exclude_local must be 'true' or 'false'")
        
        # Adjust query to exclude local origins
        if is_local:
            query_list.append("i.is_local")
        else:
            query_list.append("NOT i.is_local")

    if is_recurrent:
        # Validate exclude_recurrent
        if is_recurrent.lower() == "true":
            is_recurrent = True
        elif is_recurrent.lower() == "false":
            is_recurrent = False
        else:
            raise HTTPException(status_code=400, detail="exclude_recurrent must be 'true' or 'false'")
        
        # Adjust query to exclude recurrent origins
        if is_recurrent:
            query_list.append("i.is_recurrent")
        else:
            query_list.append("NOT i.is_recurrent")

    if prefixes:
        prefixes = prefixes.split(",")

        # Validate prefixes
        if len(prefixes) > MAX_LIST_LENGTH:
            raise HTTPException(status_code=400, detail=f"prefixes list cannot exceed {MAX_LIST_LENGTH} items")
        try:
            [ipaddress.ip_network(prefix) for prefix in prefixes]
        except ValueError:
            raise HTTPException(status_code=400, detail="prefixes must be a comma-separated list of valid CIDR prefixes")
        if prefix_match_type not in ["exact", "more_specific", "less_specific"]:
            raise HTTPException(status_code=400, detail="prefix_match_type must be one of 'exact', 'more_specific', or 'less_specific'")
        
        # Adjust query based on prefix match type
        if prefix_match_type == "exact":
            query_list.append("n.prefix = ANY(%s)")
            params.append(prefixes)
        elif prefix_match_type == "more_specific":
            query_list.append("n.prefix::inet <<= ANY(%s)")
            params.append(prefixes)
        elif prefix_match_type == "less_specific":
            query_list.append("n.prefix::inet >>= ANY(%s)")
            params.append(prefixes)

    if peer_ips:
        peer_ips = peer_ips.split(",")

        # Validate peer IPs
        if len(peer_ips) > MAX_LIST_LENGTH:
            raise HTTPException(status_code=400, detail=f"peer_ips list cannot exceed {MAX_LIST_LENGTH} items")
        try:
            [ipaddress.ip_address(ip) for ip in peer_ips]
        except ValueError:
            raise HTTPException(status_code=400, detail="peer_ips must be a comma-separated list of valid IP addresses")
        
        # Adjust query to handle peer IPs
        query_list.append("n.peer_ip = ANY(%s)")
        params.append(peer_ips)

    if peer_asns:
        peer_asns = peer_asns.split(",")

        # Validate peer ASNs
        if len(peer_asns) > MAX_LIST_LENGTH:
            raise HTTPException(status_code=400, detail=f"peer_asns list cannot exceed {MAX_LIST_LENGTH} items")
        if not all(asn.isdigit() and 0 <= int(asn) < 4294967296 for asn in peer_asns):
            raise HTTPException(status_code=400, detail="peer_asns must be a comma-separated list of valid AS numbers (0-4294967295)")
        
        # Adjust query to handle peer ASNs
        query_list.append("n.peer_asn = ANY(%s)")
        params.append(peer_asns)

    if before_datetime:
        # Validate before_datetime
        try:
            before_datetime = datetime.strptime(before_datetime, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="before_datetime must be in 'YYYY-MM-DD' format")
        
        # Adjust query to handle before_datetime
        query_list.append("n.observed_at < %s")
        params.append(before_datetime)

    if after_datetime:
        try:
            after_datetime = datetime.strptime(after_datetime, "%Y-%m-%d")
        except ValueError:
            raise HTTPException(status_code=400, detail="after_datetime must be in 'YYYY-MM-DD' format")
        query_list.append("n.observed_at > %s")
        params.append(after_datetime)

    if path_contains:
        path_asns = path_contains.split(",")
        if len(path_asns) > MAX_LIST_LENGTH:
            raise HTTPException(status_code=400, detail=f"path_contains list cannot exceed {MAX_LIST_LENGTH} items")
        if not all(asn.isdigit() and 0 <= int(asn) < 4294967296 for asn in path_asns):
            raise HTTPException(status_code=400, detail="path_contains must be a comma-separated list of valid AS numbers (0-4294967295)")
        for asn in path_asns:
            query_list.append("n.as_path LIKE %s")
            params.append(f"% {asn} %")

    if query_list:
        query += " WHERE " + " AND ".join(query_list)
    else:
        query += " WHERE TRUE"

    query += " GROUP BY i.id, date, link, i.num_paths, i.classification, i.num_susp_inf"

    # print(f"Executing query: {query} with params: {params}")

    with psycopg.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute(query, params)
            rows = cur.fetchall()

    results = []
    for row in rows:
        results.append(Inference(
            date=row[1].strftime("%Y-%m-%d"),
            link=str(row[2]),
            num_paths=int(row[3]),
            classification=str(row[4]),
            confidence=int(row[5]),
            new_links=row[6]
        ))

    return results
