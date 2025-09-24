from psycopg2.extensions import connection




def boolean_to_str_legitimacy(val :bool):
    if val == "leg":
        return "legitimate"
    
    elif val == "sus":
        return "suspicious"
    
    else:
        return "unknown"
    



def execute_read_query(conn :connection, query):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        result = cursor.fetchall()

        return True, result
    
    except Exception as e:
        return False, str(e)



def _build_match_list(column, values):
    if len(values) == 1:
        return "{} = {}".format(column, values[0])

    else:
        return "{} = ANY(ARRAY{})".format(column, values)



def _build_conditions(asn :list[int]=None,
                    attackers :list[int]=None,
                    victims :list[int]=None,
                    inference_result :bool=None,
                    min_confidence_level :int=None,
                    nb_max_aspaths :int=None,
                    nb_min_aspaths :int=None,
                    start_ts :int=None,
                    stop_ts :int=None,
                    new_link_ids :list[int]=None):
    
    conditions = list()

    if asn:
        conditions.append("({} OR {})".format(_build_match_list("asn1", asn), _build_match_list("asn2", asn)))

    if attackers:
        conditions.append("attackers && ARRAY{}::BIGINT[]".format(attackers))

    if victims:
        conditions.append("victims && ARRAY{}::BIGINT[]".format(victims))

    if inference_result:
        conditions.append("classification = {}".format(inference_result))

    if min_confidence_level:
        conditions.append("confidence_level >= {}".format(min_confidence_level))

    if nb_max_aspaths:
        conditions.append("num_paths <= {}".format(nb_max_aspaths))

    if nb_min_aspaths:
        conditions.append("nb_aspaths >= {}".format(nb_min_aspaths))

    if start_ts:
        conditions.append("observed_at >= {}".format(start_ts))

    if stop_ts:
        conditions.append("observed_at >= {}".format(stop_ts))

    if new_link_ids:
        conditions.append(_build_match_list("id", new_link_ids))

    return conditions



def get_new_links(pg_helper :connection,
                  asn :list[int],
                  attackers :list[int],
                  victims :list[int],
                  inference_result :bool,
                  min_confidence_level :int,
                  nb_max_aspaths :int,
                  nb_min_aspaths :int,
                  start_date :int,
                  stop_date :int,
                  new_link_ids :list[int]):


    query = "SELECT id, observed_at, asn1, asn2, attackers, victims, classification, confidence_level, num_paths, is_reccurent FROM inference_summary"

    conditions = _build_conditions(asn=asn,
                                   attackers=attackers,
                                   victims=victims,
                                   inference_result=inference_result,
                                   min_confidence_level=min_confidence_level,
                                   nb_max_aspaths=nb_max_aspaths,
                                   nb_min_aspaths=nb_min_aspaths,
                                   start_ts=start_date,
                                   stop_ts=stop_date,
                                   new_link_ids=new_link_ids)
    
    ### --- Add all filters to the query --- ###
    if len(conditions):
        query += "WHERE {};".format(" AND ".join(conditions))
    else:
        query += ";"

    ok, res = execute_read_query(pg_helper, query)
    if not ok:
        return {"code": 404, "detail": "Unable to execute the query. Please retry later."}
    

    results :list[dict[str, int | str | list[int]]] = dict()

    for id, observed_at, asn1, asn2, attackers, victims, classification, confidence_level, num_paths, is_reccurent in res:
        case_ = dict()
    
        case_["id"] = id
        case_["date"] = observed_at
        case_["as1"] = asn1
        case_["as2"] = asn2
        case_["presumed_attacker"] = attackers
        case_["presumed_victims"] = victims
        case_["inference_result"] = boolean_to_str_legitimacy(classification)
        case_["confidence_level"] = confidence_level
        case_["nb_aspaths_observed"] = num_paths
        case_["is_reccurent"] = is_reccurent

        results.append(case_)

    return {"code": 200, "results": results}




def get_inference_details(pg_helper :connection,
                          new_link_ids :list[int]):
    
    ### --- Build the query to get the inference details --- ###
    query = "SELECT observed_at, asn1, asn2, inference_id, peer_asn, peer_ip, as_path, prefix FROM inferences WHERE inference_id = ANY(ARRAY{});".format(new_link_ids)

    ### --- Retreive results --- ###
    ok, res = execute_read_query(pg_helper, query)
    if not ok:
        return {"code": 404, "detail": "Unable to execute the query. Please retry later."}
    

    ids = set()

    for _, _, _, id_, _, _, _, _ in res:
        ids.add(id_)

    values = dict()

    for id_ in ids:
        query = "SELECT is_origin_rpki_valid, is_local, classification, confidence_level FROM inference_summary WHERE id = {};".format(id_)

        ok, tmp_res = execute_read_query(pg_helper, query)
        if not ok:
            return {"code": 404, "detail": "Unable to execute the query. Please retry later."}
        
        pfxs_tags = list()
        for is_origin_rpki_valid, is_local, classification, confidence_level in tmp_res:
            if is_origin_rpki_valid:
                pfxs_tags.append("RPKI valid")
            else:
                pfxs_tags.append("RPKI invalid")

            if not is_local:
                pfxs_tags.append("Origin invalid")

            values[id_] = [boolean_to_str_legitimacy(classification), confidence_level, [], pfxs_tags]

    
    results :dict[int, list[tuple[str, int, int, int, str, str, str, str, int, list[str], list[str]]]] = dict()

    for observed_at, asn1, asn2, inference_id, peer_asn, peer_ip, as_path, prefix in res:
        is_legit, confidence_level, asp_tags, pfx_tags = values[inference_id]

        case_ = (observed_at, 
                 asn1, 
                 asn2, 
                 peer_asn, 
                 peer_ip, 
                 as_path, 
                 prefix, 
                 is_legit, 
                 confidence_level, 
                 asp_tags, 
                 pfx_tags)

        if inference_id not in results:
            results[inference_id] = list()

        results[inference_id].append(case_)

    return {"code": 200, "results": results}

