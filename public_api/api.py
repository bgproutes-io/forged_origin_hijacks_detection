from public_api.read import get_new_links, get_inference_details
import os
from fastapi import FastAPI, Query, HTTPException
import psycopg2
import uvicorn
import time
import datetime
import sys




DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "db_name"),
    "user": os.getenv("DB_USER", "db_user"),
    "password": os.getenv("DB_PASSWORD", "db_pwd"),
    "host": os.getenv("DB_HOST", "localhost"),
    "port": 5432
}


DEFAULT_EXTERNAL_API_HOST = "127.0.0.1"
DEFAULT_EXTERNAL_API_PORT = 5555


INTERVAL = 3600 * 6


class DFOHExternalAPI:
    def __init__(self, debug_file=None, api_verbose_file=None, api_host=None, api_port=None):
        self.debug   = open(debug_file, "w") if debug_file else sys.stderr
        self.verbose = open(api_verbose_file, "w") if api_verbose_file else sys.stderr

        self.pg_helper = psycopg2.connect(**DB_CONFIG)
        
        self.api_host = api_host if api_host else DEFAULT_EXTERNAL_API_HOST
        self.api_port = api_port if api_port else DEFAULT_EXTERNAL_API_PORT
        
        self.app = FastAPI()
        self.app.get("/new_links")(self._get_new_link)
        self.app.get("/inference_details")(self._get_inference_details)



    def _get_new_link(self, 
                      asn :str=Query(None, description="Filter on the ASNs ivolved in the new links. Must be a list of integers, comma separated."),
                      attackers :str=Query(None, description="Filter on the ASNs that are a potential attacker. Must be a list of integers, comma separated."),
                      victims :str=Query(None, description="Filter on the ASNs that are a potential victim. Must be a list of integers, comma separated."),
                      inference_result :str=Query(None, description="Filter on the inferred legitimacy of the new links. Values are either 'legitimate' or 'suspicious'."),
                      min_confidence_level :int=Query(None, description="Filter all new links with a confidence level below the passed value."),
                      nb_max_aspaths :int=Query(None, description="Filter new links observed with more AS-path than the passed value."),
                      nb_min_aspaths :int=Query(None, description="Filter new links observed less more AS-path than the passed value."),
                      start_time :str=Query(None, description="Min time at which the new links must have started. YYYY-MM-DDTHH:MM:SS."),
                      stop_time :str=Query(None, description="Max time at which the new links must have started. YYYY-MM-DDTHH:MM:SS."),
                      new_link_ids :str=Query(None, description="List of new link ids that we want to get. Must be a list of integers, comma separated.")):
        
        asn_val                  = None
        attackers_val            = None
        victims_val              = None
        inference_result_val     = None
        min_confidence_level_val = None
        nb_max_aspaths_val       = None
        nb_min_aspaths_val       = None
        start_time_val           = None
        stop_time_val            = None
        new_link_ids_val         = None

        ## -- Check ASN parameter consistency -- ##
        if asn:
            asn_val = list()

            if not all(val.isdigit() for val in asn.split(",")):
                raise HTTPException(status_code=404, detail="Parameter 'asn' must be a list of integers, comma separated. Value '{}' is not valid.".format(asn))

            for val in asn.split(","):
                asn_val.append(int(val))


        ## -- Check Attacker parameter consistency
        if attackers:
            attackers_val = list()

            if not all(val.isdigit() for val in attackers.split(",")):
                raise HTTPException(status_code=404, detail="Parameter 'attackers' must be a list of integers, comma separated. Value '{}' is not valid.".format(attackers))

            for val in attackers.split(","):
                attackers_val.append(int(val))

        ## -- Check Victims parameter consistency
        if victims:
            victims_val = list()

            if not all(val.isdigit() for val in victims.split(",")):
                raise HTTPException(status_code=404, detail="Parameter 'victims' must be a list of integers, comma separated. Value '{}' is not valid.".format(victims))

            for val in victims.split(","):
                victims_val.append(int(val))


        ## -- Check inference result parameter -- ##
        if inference_result:
            if inference_result == "suspicious":
                inference_result_val = False
            
            elif inference_result == "legitimate":
                inference_result_val = True
            
            else:
                raise HTTPException(status_code=404, detail="Parameter 'inference_result' must be a either 'legitimate' or 'suspicious'. Value '{}' is not valid.".format(inference_result))
            

        ## -- Check minimum confidence level parameter -- ##
        if min_confidence_level:
            if int(min_confidence_level) < 0 or int(min_confidence_level) > 5:
                raise HTTPException(status_code=404, detail="Parameter 'min_confidence_level' must be an integer between 0 and 5. Value '{}' is not valid.".format(min_confidence_level))
            
            min_confidence_level_val = int(min_confidence_level)


        ## -- Check Max number of collected AS-paths parameter -- ##
        if nb_max_aspaths:
            if int(nb_max_aspaths) < 0:
                raise HTTPException(status_code=404, detail="Parameter 'nb_max_aspaths' must be a positive integer. Value '{}' is not valid.".format(nb_max_aspaths))
            
            nb_max_aspaths_val = int(nb_max_aspaths)

        ## -- Check Min number of collected AS-paths parameter -- ##
        if nb_min_aspaths:
            if int(nb_min_aspaths) < 0:
                raise HTTPException(status_code=404, detail="Parameter 'nb_min_aspaths' must be a positive integer. Value '{}' is not valid.".format(nb_min_aspaths))
            
            nb_min_aspaths_val = int(nb_min_aspaths)


        ## -- Check the starting date parameter -- ##
        if start_time:
            start_time_val = start_time.split("T")[0]

            if start_time_val == -1:
                raise HTTPException(status_code=404, detail="Parameter 'start_time' must be a date in ISO format (YYYY-MM-DDTHH:MM:SS). Value '{}' is not valid.".format(start_time))
        else:
            start_time_val = datetime.datetime.fromtimestamp(time.time()).strftime("%Y-%m-%d")

        ## -- Check the ending date parameter -- ##
        if stop_time:
            stop_time_val = stop_time.split("T")[0]

            if stop_time_val == -1:
                raise HTTPException(status_code=404, detail="Parameter 'stop_time' must be a date in ISO format (YYYY-MM-DDTHH:MM:SS). Value '{}' is not valid.".format(stop_time))
        else:
            stop_time_val = start_time_val

        
        ## -- Check new link IDs parameter -- ##
        if new_link_ids:
            new_link_ids_val = list()

            if not all(val.isdigit() for val in new_link_ids.split(",")):
                raise HTTPException(status_code=404, detail="Parameter 'new_link_ids' must be a list of integers, comma separated. Value '{}' is not valid.".format(new_link_ids))
            
            for val in new_link_ids.split(","):
                new_link_ids_val.append(int(val))


        result = get_new_links(self.pg_helper,
                               asn_val,
                               attackers_val,
                               victims_val,
                               inference_result_val,
                               min_confidence_level_val,
                               nb_max_aspaths_val,
                               nb_min_aspaths_val,
                               start_time_val,
                               stop_time_val,
                               new_link_ids_val)
        
        if result["code"] != 200:
            raise HTTPException(status_code=result["code"], detail=result["detail"])
        
        return result
    


    def _get_inference_details(self,
                               new_link_ids :str=Query(..., description="List of the new link IDs from which we want to get the detail. must be a list of integers, comma separated. The list length must not exceed 100.")):
        
        new_link_ids_val = list()

        if not all(val.isdigit() for val in new_link_ids.split(",")):
            raise HTTPException(status_code=404, detail="Parameter 'new_link_ids' must be a list of integers, comma separated. Value '{}' is not valid.".format(new_link_ids))
        
        if len(new_link_ids.split(",")) > 100:
            raise HTTPException(status_code=404, detail="Parameter 'new_link_ids' must be a list of size 100 maximum. There are {} elements in the list.".format(len(new_link_ids.split(","))))
        
        for val in new_link_ids.split(","):
            new_link_ids_val.append(int(val))

        result = get_inference_details(self.pg_helper, new_link_ids_val)

        if result["code"] != 200:
            raise HTTPException(status_code=result["code"], detail=result["detail"])
        
        return result
    


    def start_api(self):
        if self.verbose != sys.stderr:
            sys.stderr = self.verbose
            sys.stdout = self.verbose

        uvicorn.run(self.app, host=self.api_host, port=self.api_port)


if __name__ == "__main__":
    api_worker = DFOHExternalAPI()
    api_worker.start_api()

