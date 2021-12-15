from os import walk, path
import json
import datetime
import csv
import subprocess
import re
import requests
import time
class refresh:
    columns_list = [  'OBJECTID', 'FEATURE_ID', 'CODE', 'NAME', 'ADDRESS', 'XCOORD', 'YCOORD', 'LONGITUDE', 'LATITUDE'
                    , 'ADDRESS_NUM', 'ON_PREFIX', 'ON_STNAME', 'ON_STYPE', 'ON_SUFFIX', 'CROSS_PREFIX', 'CROSS_STNAME'
                    , 'CROSS_STYPE', 'CROSS_SUFFIX', 'GEOMETRY_X', 'GEOMETRY_Y']
    unmapped_columns = []
    skipped_files   = 0
    reformat_failed = 0
    date_seperator = f"{datetime.datetime.now().year}/{datetime.datetime.now().month:0>2d}/{datetime.datetime.now().day:0>2d}"
    output_path = './output_data/processed_data'
    date_ts = datetime.datetime.now().strftime('%Y%m%d%H%M%S%f')
    notify_failure = False
    # Module to execute file system commands
    def execute_system_command(self,pCommandList,pShell=False, pPrintMessage=True):
        try:
            if isinstance(pCommandList, list) or isinstance(pCommandList, tuple):
                print('Executing command [{:s}]'.format(' '.join(pCommandList))) 
                gdiv_sp = subprocess.Popen(pCommandList,stdout=subprocess.PIPE,shell=pShell)
                rtrn_out = gdiv_sp.communicate()
                return rtrn_out
        except:
            raise
    # Module to reformat timestamp values
    def convert_unixts_to_datetime(self,pSeconds,pMicroSeconds=0):
        try:
            return datetime.datetime(year=1970,month=1,day=1) + datetime.timedelta(seconds=pSeconds, microseconds=pMicroSeconds)
        except:
            raise
    # Module to list file system objects
    def http_request(self,pParameters,pReqType='get'):
        try:
            if isinstance(pParameters, dict):
                if 'URL' in pParameters:
                    req_params = {}
                    if 'BASICAUTH' in pParameters:
                        req_params['auth'] = (pParameters['BASICAUTH'][0], pParameters['BASICAUTH'][1])
                    if 'HEADER' in pParameters:
                        req_params['headers'] = pParameters['HEADER']
                    if 'DATA' in pParameters:
                        req_params['data'] = pParameters['DATA']
                    if 'JSON' in pParameters:
                        req_params['json'] = pParameters['JSON']
                    if 'PARAMS' in pParameters:
                        req_params['params'] = pParameters['PARAMS']
                    if 'TIMEOUT' in pParameters:
                        req_params['timeout'] = pParameters['TIMEOUT']
                    else:
                        req_params['timeout'] = 10
                    if pReqType == 'get':
                        return requests.get(pParameters['URL'], **req_params)
                    elif pReqType == 'post':
                        return requests.post(pParameters['URL'], **req_params)
                    else:
                        raise Exception("Module is not configured to accept Request type other than GET/POST")
                else:
                    raise Exception(','.join(list(pParameters.keys()) + " Parameter don't have URL key !"))
            else:
                Message =   (
                                "The http_request excepts a dictionary object as parameter !\n"
                                "Expected below keys \n"
                                "URL[Mandatory]: ThreadName, Dynamically name will be generated using Function Name and Counter\n"
                                "HEADER[Optional]: Requests HEADER PARAMETER\n"
                                "TIMEOUT[Optional]: DEFAULTED to 10 Seconds, can be overridden\n"
                                "BASICAUTH[Optional]: no default\n"
                                "DATA[Optional]: no default\n"
                                "PARAMS[Optional]: no default\n"
                                "JSON[Optional]: no default\n"
                            )
                raise Exception(Message)
        except:
            raise
    # Module to reformat API Response
    def reformat_api_response(self, pJSON):
        self.execute_system_command(['mkdir','-p',self.output_path])
        pOutfile = f"{self.output_path}/transit_api_data.csv"
        outfile_ref = open(pOutfile, 'w')
        csv_write_obj = csv.writer(outfile_ref, quotechar='"')
        required_objects = pJSON["features"]
        for iL0 in required_objects:
            record = {}
            for iDK0, iDV0 in iL0.items():
                if iDK0 == 'attributes':
                    for iDK1, iDV1 in iDV0.items():
                        if iDK1 in self.columns_list:
                            record[iDK1] = iDV1
                        else:
                            if iDK1 not in self.unmapped_columns:
                                self.unmapped_columns.append(iDK1)
                elif iDK0 == 'geometry':
                    for iDK1, iDV1 in iDV0.items():
                        if f"GEOMETRY_{iDK1.upper()}" in self.columns_list:
                            record[f"GEOMETRY_{iDK1.upper()}"] = iDV1
                        else:
                            if f"GEOMETRY_{iDK1.upper()}" not in self.unmapped_columns:
                                self.unmapped_columns.append(f"GEOMETRY_{iDK1.upper()}")
            csv_write_obj.writerow([record[iL1] if iL1 in record else "" for iL1 in self.columns_list] + [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
        outfile_ref.close()
    def initiate(self):
        try:
            retry_limit = 5
            retry_interval = 60
            try_count = 0
            while True:
                req = self.http_request({"URL" : "https://gisdata.kingcounty.gov/arcgis/rest/services/OpenDataPortal/transportation___base/MapServer/395/query?where=1%3D1&outFields=*&outSR=4326&f=json"})
                if req.status_code == 200:
                    break
                else:
                    try_count += 1
                    if try_count > 4:
                        raise Exception(f"ERROR: API Call failed with Status Code[{req.status_code}], Retry attempts [{try_count}] @ Interval [{retry_interval}]")
                    else:
                        time.sleep(retry_interval)
            api_response = json.loads(req.text)
            self.reformat_api_response(api_response)
            #Below steps are for notifying reformat step execution summary
            if len(self.unmapped_columns) > 0:
                print(f"INFO: Total UnMapped Attributes [{len(self.unmapped_columns)}]")
                for iL0 in self.unmapped_columns:
                    print(f"WARNING: [{iL0}] Attribute is not mapped in the script !")
        except:
            self.notify_failure = True
if __name__ == "__main__":
    obj = refresh()
    obj.initiate()

