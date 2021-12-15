from os import walk, path
import json
import datetime
import csv
import subprocess
import re
class refresh:
    columns_list = ['id', 'date', 'price', 'bedrooms', 'bathrooms', 'sqft_living', 'sqft_lot', 
                    'floors', 'waterfront', 'view', 'condition', 'grade', 'sqft_above', 
                    'sqft_basement', 'yr_built', 'yr_renovated', 'zipcode', 'lat', 'long', 
                    'sqft_living15', 'sqft_lot15']
    date_reformat_columns = {"date" : "%Y%m%dT%H%M%S"}
    exponent_reformat_columns = ["price"]
    skipped_files = 0
    reformat_failed = 0
    list_of_files_failed = []
    list_of_output_files = {}
    date_seperator = f"{datetime.datetime.now().year}/{datetime.datetime.now().month:0>2d}/{datetime.datetime.now().day:0>2d}"
    output_path = f'./output_data/processed_data/kaggle/{date_seperator}'
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
    def get_list_of_files(self, pLocalFilePath,rSubDirs=False):
        try:
            for Dpath, Sdirs, Fnames in walk(pLocalFilePath, followlinks=False):
                if rSubDirs:
                    for sdName in Sdirs:
                        yield '{:s}/{:s}'.format(Dpath, sdName)
                else:
                    for fName in Fnames:
                        if Dpath.split('/').pop() == '':
                            yield '{:s}{:s}'.format(Dpath, fName)
                        else:
                            yield '{:s}/{:s}'.format(Dpath, fName)
        except:
            raise
    # Module to get listed already processed files to exclude from the current process
    def get_processed_files(self):
        self.processed_files = {}
        if not path.isfile("./output_data/processed_files.json"):
            return
        with open("./output_data/processed_files.json", "r") as infile_ref:
            self.processed_files = json.loads(infile_ref.read())
    # Module to store the processed files object
    def set_processed_files(self):
        with open("./output_data/processed_files.json", "w") as infile_ref:
            infile_ref.write(json.dumps(self.processed_files))
    # Module to reformat file content
    def reformat_file(self, pFName):
        header_ = {}
        self.execute_system_command(['mkdir','-p',self.output_path])
        filename = pFName.split('/').pop()
        filename = filename.split('.')
        filename.pop()
        pOutfile = f"{self.output_path}/{'.'.join(filename)}_{self.date_ts}.csv"
        check_issue = False
        with open(pFName, 'r') as infile_ref, \
             open(pOutfile, 'w') as outfile_ref:
            csv_write_obj = csv.writer(outfile_ref, quotechar='"')
            try:
                rCount = 0
                for iRow in csv.reader(infile_ref):
                    if header_:
                        out_cols = [iRow[header_[iL0]] for iL0 in self.columns_list]
                        for iDK0, iDV0 in self.date_reformat_columns.items():# Reformating Timestamp values to ISO Timestamp Format
                            out_cols[header_[iDK0]] = datetime.datetime.strptime(out_cols[header_[iDK0]], iDV0).strftime('%Y-%m-%d %H:%M:%S')
                        for iL0 in self.exponent_reformat_columns:# Reformating numeric values with exponent text
                            tmp_value = out_cols[header_[iL0]].split('e+')
                            if len(tmp_value) > 1:
                                out_cols[header_[iL0]] = str(float(tmp_value[0]) * (10 ** int(tmp_value[1])))
                        rCount += 1
                        csv_write_obj.writerow(out_cols + [datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
                    else:
                        header_ = {iRow[iR0]:iR0 for iR0 in range(len(iRow))}
                        if len(header_) != len(self.columns_list):# Validating file layout
                            self.list_of_files_failed.append(pFName)
                            check_issue = True
                            break
            except:
                check_issue = True
                raise
        if not check_issue: # If there is no issue, File will be used for Syncing with Database table
            self.list_of_output_files[pOutfile] = rCount
    def initiate(self):
        self.get_processed_files()
        files_to_be_processed = {}
        for iFile in self.get_list_of_files("./source_data/kaggle/"):
            if iFile in self.processed_files:
                # Validating the files timestamp, if the value is different from last execution, the job reprocess the files, so the file is updated
                if self.convert_unixts_to_datetime(int(path.getmtime(iFile))) > datetime.datetime.strptime(self.processed_files[iFile], '%Y-%m-%d %H:%M:%S'):
                    files_to_be_processed[iFile] = self.convert_unixts_to_datetime(int(path.getmtime(iRow))).strftime('%Y-%m-%d %H:%M:%S')
                    self.reformat_file(iFile)
                else:
                    # self.reformat_file(iFile) # Test
                    self.skipped_files += 1
            else:
                files_to_be_processed[iFile] = self.convert_unixts_to_datetime(int(path.getmtime(iFile))).strftime('%Y-%m-%d %H:%M:%S')
                self.reformat_file(iFile)
        self.processed_files.update(files_to_be_processed) # Updating list of processed file, so in next execution the files will be skipped gto avoid reprocessing same files
        self.set_processed_files()
        # Below steps are for notifying reformat step execution summary
        print(f"INFO: Total files processed [{len(files_to_be_processed)}]")
        print(f"INFO: Total files skipped [{self.skipped_files}]")
        for iDK0, iDV0 in self.list_of_output_files.items():
            print(f"INFO: Total Records Processed [{iDV0}] for file [{iDK0}] .")
        if len(self.list_of_files_failed) > 0:
            self.notify_failure = True
            for iL0 in self.list_of_files_failed:
                print(f"ERROR: Reformat failed for file [{iL0}]")
if __name__ == "__main__":
    obj = refresh()
    obj.initiate()

