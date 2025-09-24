from external_api.read import get_new_links, get_inference_details
from utils.debug import Debug
from utils.config import Config
from utils.pg_helper import PGClient
from utils.utils import is_as_number, iso_to_unix, create_directory_from_file
import os
from fastapi import FastAPI, Query, HTTPException
import uvicorn
import time
import sys



INTERVAL = 3600 * 6



config = Config()

class DFOHExternalAPI:
    def __init__(self, debug_file=None, pg_debug_file=None, api_verbose_file=None, api_host=None, api_port=None):
        self.debug_file    = debug_file if debug_file else config.EXTERNAL_API_DEBUG_FILE
        self.pg_debug_file = pg_debug_file if pg_debug_file else config.EXTERNAL_API_POSTGRES_FILE
        self.verbose_file  = api_verbose_file if api_verbose_file else config.EXTERNAL_API_VERBOSE_FILE

        self.debug = Debug(outfile=self.debug_file)
        self.pg_helper = PGClient(config.DFOH_PG_PATH, 
                                  config.DFOH_PG_PORT, 
                                  config.DFOH_PG_DB, 
                                  config.DFOH_PG_USER, 
                                  os.environ.get("PG_PASS"), 
                                  debug_file=self.pg_debug_file)
        
        self.api_host = api_host if api_host else config.DFOH_EXTERNAL_API_HOST
        self.api_port = api_port if api_port else config.DFOH_EXTERNAL_API_PORT
        
        self.app = FastAPI()
        self.app.get("/new_links")(self._get_new_link)
        self.app.get("/inference_details")(self._get_inference_details)



    def _get_new_link(self, 
                      asn :str=Query(None, description="Filter on the ASNs ivolved in the new links. Must be a list of integers, comma separated."),
                      attackers :str=Query(None, description="Filter on the ASNs that are a potential attacker. Must be a list of integers, comma separated."),
                      victims :str=Query(None, description="Filter on the ASNs that are a potential victim. Must be a list of integers, comma separated."),
                      inference_result :str=Query(None, description="Filter on the inferred legitimacy of the new links. Values are either 'legitimate' or 'suspicious'."),
                      min_confidence_level :int=Query(None, description="Filter all new links with a confidence level below the passed value."),
                      nb_max_prefixes :int=Query(None, description="Filter new links affecting more prefixes than the passed value"),
                      nb_min_prefixes :int=Query(None, description="Filter new links affecting less prefixes than the passed value"),
                      nb_max_visible_vps :int=Query(None, description="Filter new links seen by more VPs that the passed value."),
                      nb_min_visible_vps :int=Query(None, description="Filter new links seen by less VPs that the passed value."),
                      nb_max_aspaths :int=Query(None, description="Filter new links observed with more AS-path than the passed value."),
                      nb_min_aspaths :int=Query(None, description="Filter new links observed less more AS-path than the passed value."),
                      start_time :str=Query(None, description="Min time at which the new links must have started. YYYY-MM-DDTHH:MM:SS."),
                      stop_time :str=Query(None, description="Max time at which the new links must have started. YYYY-MM-DDTHH:MM:SS."),
                      new_link_ids :str=Query(None, description="List of new link ids that we want to get. Must be a list of integers, comma separated."),
                      tags :str=Query(None, description="List of tags associated with the new links that we want to retreive. Must be a list comma separated.")):
        
        asn_val                  = None
        attackers_val            = None
        victims_val              = None
        inference_result_val     = None
        min_confidence_level_val = None
        nb_max_prefixes_val      = None
        nb_min_prefixes_val      = None
        nb_max_visible_vps_val   = None
        nb_min_visible_vps_val   = None
        nb_max_aspaths_val       = None
        nb_min_aspaths_val       = None
        start_time_val           = None
        stop_time_val            = None
        new_link_ids_val         = None
        tags_val                 = None

        ## -- Check ASN parameter consistency -- ##
        if asn:
            asn_val = list()

            if not all(is_as_number(val) for val in asn.split(",")):
                raise HTTPException(status_code=404, detail="Parameter 'asn' must be a list of integers, comma separated. Value '{}' is not valid.".format(asn))

            for val in asn.split(","):
                asn_val.append(int(val))


        ## -- Check Attacker parameter consistency
        if attackers:
            attackers_val = list()

            if not all(is_as_number(val) for val in attackers.split(",")):
                raise HTTPException(status_code=404, detail="Parameter 'attackers' must be a list of integers, comma separated. Value '{}' is not valid.".format(attackers))

            for val in attackers.split(","):
                attackers_val.append(int(val))

        ## -- Check Victims parameter consistency
        if victims:
            victims_val = list()

            if not all(is_as_number(val) for val in victims.split(",")):
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
            if not is_as_number(min_confidence_level) or int(min_confidence_level) < 0 or int(min_confidence_level) > 5:
                raise HTTPException(status_code=404, detail="Parameter 'min_confidence_level' must be an integer between 0 and 5. Value '{}' is not valid.".format(min_confidence_level))
            
            min_confidence_level_val = int(min_confidence_level)


        ## -- Check Max number of prefixes parameter -- ##
        if nb_max_prefixes:
            if not is_as_number(nb_max_prefixes) or int (nb_max_prefixes) < 0:
                raise HTTPException(status_code=404, detail="Parameter 'nb_max_prefixes' must be a positive integer. Value '{}' is not valid.".format(nb_max_prefixes))
            
            nb_max_prefixes_val = int(nb_max_prefixes)

        ## -- Check Min number of prefixes parameter -- ##
        if nb_min_prefixes:
            if not is_as_number(nb_min_prefixes) or int (nb_min_prefixes) < 0:
                raise HTTPException(status_code=404, detail="Parameter 'nb_min_prefixes' must be a positive integer. Value '{}' is not valid.".format(nb_min_prefixes))
            
            nb_min_prefixes_val = int(nb_min_prefixes)


        ## -- Check Max number of visible VPs parameter -- ##
        if nb_max_visible_vps:
            if not is_as_number(nb_max_visible_vps) or int(nb_max_visible_vps) < 0:
                raise HTTPException(status_code=404, detail="Parameter 'nb_max_visible_vps' must be a positive integer. Value '{}' is not valid.".format(nb_max_visible_vps))
            
            nb_max_visible_vps_val = int(nb_max_visible_vps)

        ## -- Check Min number of visible VPs parameter -- ##
        if nb_min_visible_vps:
            if not is_as_number(nb_min_visible_vps) or int(nb_min_visible_vps) < 0:
                raise HTTPException(status_code=404, detail="Parameter 'nb_min_visible_vps' must be a positive integer. Value '{}' is not valid.".format(nb_min_visible_vps))
            
            nb_min_visible_vps_val = int(nb_min_visible_vps)


        ## -- Check Max number of collected AS-paths parameter -- ##
        if nb_max_aspaths:
            if not is_as_number(nb_max_aspaths) or int(nb_max_aspaths) < 0:
                raise HTTPException(status_code=404, detail="Parameter 'nb_max_aspaths' must be a positive integer. Value '{}' is not valid.".format(nb_max_aspaths))
            
            nb_max_aspaths_val = int(nb_max_aspaths)

        ## -- Check Min number of collected AS-paths parameter -- ##
        if nb_min_aspaths:
            if not is_as_number(nb_min_aspaths) or int(nb_min_aspaths) < 0:
                raise HTTPException(status_code=404, detail="Parameter 'nb_min_aspaths' must be a positive integer. Value '{}' is not valid.".format(nb_min_aspaths))
            
            nb_min_aspaths_val = int(nb_min_aspaths)


        ## -- Check the starting date parameter -- ##
        if start_time:
            start_time_val = iso_to_unix(start_time)

            if start_time_val == -1:
                raise HTTPException(status_code=404, detail="Parameter 'start_time' must be a date in ISO format (YYYY-MM-DDTHH:MM:SS). Value '{}' is not valid.".format(start_time))
        else:
            start_time_val = int(time.time()) - 3600 * 2

        ## -- Check the ending date parameter -- ##
        if stop_time:
            stop_time_val = iso_to_unix(stop_time)

            if stop_time_val == -1:
                raise HTTPException(status_code=404, detail="Parameter 'stop_time' must be a date in ISO format (YYYY-MM-DDTHH:MM:SS). Value '{}' is not valid.".format(stop_time))
        else:
            stop_time_val = start_time_val - INTERVAL

        
        ## -- Check new link IDs parameter -- ##
        if new_link_ids:
            new_link_ids_val = list()

            if not all(is_as_number(val) for val in new_link_ids.split(",")):
                raise HTTPException(status_code=404, detail="Parameter 'new_link_ids' must be a list of integers, comma separated. Value '{}' is not valid.".format(new_link_ids))
            
            for val in new_link_ids.split(","):
                new_link_ids_val.append(int(val))

        ## -- Parse list of tags -- ##
        if tags:
            tags_val = list()

            for val in tags.split(","):
                tags_val.append(val.replace("_", " "))

        result = get_new_links(self.pg_helper,
                               asn_val,
                               attackers_val,
                               victims_val,
                               inference_result_val,
                               min_confidence_level_val,
                               nb_max_prefixes_val,
                               nb_min_prefixes_val,
                               nb_max_visible_vps_val,
                               nb_min_visible_vps_val,
                               nb_max_aspaths_val,
                               nb_min_aspaths_val,
                               start_time_val,
                               stop_time_val,
                               new_link_ids_val,
                               tags_val)
        
        if result["code"] != 200:
            raise HTTPException(status_code=result["code"], detail=result["detail"])
        
        return result
    


    def _get_inference_details(self,
                               new_link_ids :str=Query(..., description="List of the new link IDs from which we want to get the detail. must be a list of integers, comma separated. The list length must not exceed 100.")):
        
        new_link_ids_val = list()

        if not all(is_as_number(val) for val in new_link_ids.split(",")):
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
        if isinstance(self.verbose_file, str):
            create_directory_from_file(self.verbose_file)

            sys.stderr = open(self.verbose_file, "a")
            sys.stdout = open(self.verbose_file, "a")


        uvicorn.run(self.app, host=self.api_host, port=self.api_port)




