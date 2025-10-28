#---------------------------------------------------------------------
# $Id: run_stage_load.py 1418 2019-12-17 17:28:34Z PCLC0\ttran $
##################
# SVN Properties #
##################
# $Rev: 1418 $
# $Author: PCLC0\ttran $
# $Date: 2019-12-17 12:28:34 -0500 (Tue, 17 Dec 2019) $
##################
#---------------------------------------------------------------------
# Change History
#---------------------------------------------------------------------
#  Author     Date         Change
# --------   ------       -----------------------------------------------
#  C.Palmer  2022-06-19    Rework for Testing Environment
#  C.Palmer  2023-01-18    Remove aws keys
#  Y.Tawk    2023-01-30    UPdate variable user to meta_user
#  J.Rankin  2024-04-18    TSGCR-5585 - Modified print statements to call function logMessage
#  J.Rankin  2024-05-31    TSGCR-5551 - In call of getStageTableTempLoad, added on_error argument.
#  Y. Tawk   2024-08-06    TSGCR-5534 - retrieve bucket_name from landing_directory instead of config file
#---------------------------------------------------------------------
###########################################  START HEADER
#Import needed Utilities
import argparse
from sys import exit

#Import ETL Framework Libraries
#   ETL DB
import getSqlConfig
#   Load DB
import getLoadConfig
#   ETL Meta Libraries
import opsLookupUtil as lkp
import opsInsertUtil as ins
import opsUpdateUtil as upd
#   Load Libraries
import opsLoadUtil as ld
#import opsFileUtil as f  # CAP - comment out
import s3Tools as s3t  # CAP - Add
import base64

successCode = 0
errorCode = 1
InvalidArgsCode = 2
failedStr = 'FAILED'
completedStr = 'COMPLETED'
stage_table_temp_name = ''

###########################################  END HEADER


###########################################  START ARGUMENT VALIDATION
#Parse Variables
parser = argparse.ArgumentParser()
parser.add_argument('-filename','--filename', help='Description', required=True)
parser.add_argument('-snowflake_stage_landing_directory','--snowflake_stage_landing_directory', help='Description', required=False)
args = parser.parse_args()

filename = args.filename
snowflake_stage_landing_directory = args.snowflake_stage_landing_directory

#Validate Required Fields
if filename is None:
    print('filename argument is required')
    exit(InvalidArgsCode)

if snowflake_stage_landing_directory is None:
    snowflake_stage_landing_directory = getLoadConfig.getEnv('snowflake_stage_triggered_directory')

MetaEngine = getSqlConfig.MetaEngine
LoadEngine = getLoadConfig.LoadEngine

bucket_inbound = getLoadConfig.getEnv('bucket_inbound')
bucket_archive = getLoadConfig.getEnv('bucket_archive')
# snowflake_stage_landing_directory = getLoadConfig.getEnv('snowflake_stage_triggered_directory')
###########################################  END ARGUMENT VALIDATION




###############################################################################################  START MAIN

print('||-------------run_stage_load.py--------------||')
print('||-----')

process_name = 'run_stage_load.py'

###########################################  START STANDARD BEGIN PROCESS LOG BLOCK
print('|| 0. Begin Process Log')
print('||-----')
print('||     Process Name: ' + str(process_name)) 
print('||-----')

if process_name is None:
    print('|| Result: Failure')
    print('|| process_name argument is required - exiting')
    exit(InvalidArgsCode)

dbUser = getSqlConfig.getEnv('meta_user')
print('|| connecting to database with user account: ' + dbUser)

process_id = lkp.getProcessIdByProcessName(MetaEngine, process_name)

print('||-----')
if process_id:
    print('||     process_id: ' + str(process_id))

if process_id is None or process_id == 'None':
    process_id = ins.insertProcess(MetaEngine, process_name, None, None, None, None, None, None, None)

if process_id is None:
    print('||     process_id is not set')
    exit(errorCode)

process_log_id = ins.insertProcessLog(MetaEngine, process_id, None, None, None, None, None, None, None, None, None, None, 'RUNNING', None, None)

print('||     process_log_id: ' + str(process_log_id))
print('||-----')

###########################################  END STANDARD BEGIN PROCESS LOG BLOCK

#Get attributes from Metadata needed for process
print('|| 1. Get Attributes of the Source')
print('||-----')

#------------file Attribs
fileAttr = lkp.getDataFileRecordByFilename(MetaEngine, filename)
if fileAttr.shape[0] == 0:
    errorMSG = 'No Records Found in DATA_FILE! Exiting...'
    print('|| ' + errorMSG)
    upd.updateProcessLog(MetaEngine, process_log_id,None,None,None,None,None,None,None,None,None,None,None,failedStr,errorMSG,None)
    exit(errorCode)

file_id = fileAttr.file_id[0]
object_id = fileAttr.object_id[0]
source_id = fileAttr.object_id[0]
batch_id = fileAttr.batch_id[0]

#------------All Source-related Attribs
sourceAttr = lkp.getSourceAttributesById(MetaEngine, str(source_id))
if sourceAttr.shape[0] == 0:
    errorMSG = 'No Records Found in SOURCE_FEED! Exiting...'
    print('|| ' + errorMSG)
    upd.updateProcessLog(MetaEngine, process_log_id, None, None, None, None, None, None, None, None, None, None, None,
                         failedStr, errorMSG, batch_id)
    exit(errorCode)

file_format = sourceAttr.file_format[0][:5]
schema_name = sourceAttr.schema_name[0]
landing_directory = sourceAttr.landing_directory[0]  # this value should be the s3 landing directory
bucket_name = landing_directory.split('/')[2]
header_row_count = sourceAttr.header_row_count[0]
target_id = sourceAttr.target_object_id[0]
add_record_id_ind = sourceAttr.add_record_id_ind[0]
split_filename_pattern = sourceAttr.split_filename_pattern[0]
stage_name = sourceAttr.stage_name[0]
compression_str = sourceAttr.compression_str[0]
column_delimiter = sourceAttr.column_delimiter[0]
text_qualifier = sourceAttr.text_qualifier[0]
error_on_column_count_mismatch = sourceAttr.error_on_column_count_mismatch[0]
on_error = sourceAttr.on_error[0]

date_format = sourceAttr.date_format[0]
time_format = sourceAttr.time_format[0]
timestamp_format = sourceAttr.timestamp_format[0]
escape_char = sourceAttr.escape_char[0]
escape_unenclosed_field = sourceAttr.escape_unenclosed_field[0]
trim_space = sourceAttr.trim_space[0]
null_if = sourceAttr.null_if[0]
file_encoding = sourceAttr.file_encoding[0]
strip_outer_element = sourceAttr.strip_outer_element[0]   

#------------Handle Nulls and Blanks and Logic
if compression_str is None:
    compression_str = 'NONE'
if compression_str.lower() == 'gzip' and filename.lower()[-3:] != '.gz':
    stage_filename = filename + '.gz'
else:
    stage_filename = filename

file_format = file_format.strip()
if file_format is None or file_format == '':
    file_format = 'DELIM'
else:
    file_format = file_format.upper()

if schema_name is None or schema_name == '':
    schema_name = 'STG'

if (landing_directory[::-1])[:1] == '/':
    landing_directory = landing_directory[:len(landing_directory)-1]
elif landing_directory is None or landing_directory == '':
    landing_directory = bucket_inbound

if snowflake_stage_landing_directory is None or snowflake_stage_landing_directory == '':
    snowflake_stage_landing_directory = '@stg.inbound'

if header_row_count is None or header_row_count == '':
    header_row_count = 0

if add_record_id_ind is None:
    add_record_id_ind = 0

if error_on_column_count_mismatch is None:
    error_on_column_count_mismatch = 'FALSE'

if on_error is None or on_error == '':
    on_error = 'continue'

if split_filename_pattern is not None:
    if split_filename_pattern.lower() == 'none' or split_filename_pattern.lower() == '.gz':
        splitFilenamePattern = None

if date_format is None:
    date_format = 'AUTO'
if time_format is None:
    time_format = 'AUTO'
if timestamp_format is None:
    timestamp_format = 'AUTO'
ld.clean_none_value(escape_char)
ld.clean_none_value(escape_unenclosed_field)

if ld.is_empty(null_if):
    null_if = None

if ld.is_empty(trim_space):
    trim_space = None

if ld.is_empty(file_encoding):
    file_encoding = None

#------------Print
print('||     file_id = ' + str(file_id))
print('||     object_id = ' + str(object_id))
print('||     source_id = ' + str(source_id))
print('||     batch_id = ' + str(batch_id))
print('||     bucket_name = ' + str(bucket_name))
print('||     landing_directory = ' + str(landing_directory))
print('||     snowflake_stage_landing_directory = ' + str(snowflake_stage_landing_directory))
print('||     schema_name = ' + str(schema_name))
print('||     file_format = ' + str(file_format))
print('||     header_row_count = ' + str(header_row_count))
print('||     target_id = ' + str(target_id))
print('||     add_record_id_ind = ' + str(add_record_id_ind))
print('||     split_filename_pattern = ' + str(split_filename_pattern))
print('||     stage_name = ' + str(stage_name))
print('||     compression_str = ' + str(compression_str))
print('||     stage_filename = ' + str(stage_filename))
print('||     column_delimiter = ' + str(column_delimiter))
print('||     text_qualifier = ' + str(text_qualifier))
print('||     error_on_column_count_mismatch = ' + str(error_on_column_count_mismatch))
print('||     on_error = ' + str(on_error))
print('||     date_format = ' + str(date_format))
print('||     time_format = ' + str(time_format))
print('||     timestamp_format = ' + str(timestamp_format))
print('||     escape_char = ' + str(escape_char))
print('||     escape_unenclosed_field = ' + str(escape_unenclosed_field))
print('||     null_if = ' + str(null_if))
print('||     file_encoding = ' + str(file_encoding))
#print('||     strip_outer_element = ' + str(strip_outer_element))  # CAP

#------------Metadata Check
print('||-----')
print('|| 1b. Check Metadata before proceeding...')
print('||-----')
if file_id is None:
    print('|| Result: Failure')
    errorMSG = 'filename does not exist in DATA_FILE.'
    print('|| ' + errorMSG)
    upd.updateProcessLog(MetaEngine, process_log_id, None, None, None, None, None, None, None, None, None, None, None,
                         failedStr, errorMSG, batch_id)
    exit(InvalidArgsCode)
elif object_id is None:
    print('|| Result: Failure')
    errorMSG = 'There is no Source in DATA_OBJECT that matches the filename_pattern.'
    print('|| ' + errorMSG)
    upd.updateProcessLog(MetaEngine, process_log_id, None, None, None, None, None, None, None, None, None, None, None,
                         failedStr, errorMSG, batch_id)
    exit(InvalidArgsCode)
elif batch_id is None:
    print('|| Result: Failure')
    errorMSG = 'This file is not registered in an active Batch.'
    print('|| ' + errorMSG)
    upd.updateProcessLog(MetaEngine,process_log_id,None,None,None,None,None,None,None,None,None,None,None,failedStr,errorMSG,batch_id)
    exit(InvalidArgsCode)
elif target_id is None:
    print('|| Result: Failure')
    errorMSG = 'This Source has a target defined in target_object_name that has no TARGET record.'
    print('|| ' + errorMSG)
    upd.updateProcessLog(MetaEngine,process_log_id,None,None,None,None,None,None,None,None,None,None,None,failedStr,errorMSG,batch_id)
    exit(InvalidArgsCode)
elif (
        stage_name is None or
        stage_name == ''
    ):
    print('|| Result: Failure')
    errorMSG = 'This Source does not have a stage_name populated.'
    print('|| ' + errorMSG)
    upd.updateProcessLog(MetaEngine,process_log_id,None,None,None,None,None,None,None,None,None,None,None,failedStr,errorMSG,batch_id)
    exit(InvalidArgsCode)
print('|| Result: Success')
print('||-----')

#------------
print('|| 2a. Get Stage Table Create Statement')
print('||-----')

tableName = lkp.getStageTableName(MetaEngine, file_id)
stage_table_name = tableName.table_name[0]

#Returns a string
sqlCreateTable = ld.getStageTableCreate(MetaEngine,source_id,schema_name,stage_table_name,add_record_id_ind)

print('||     stage_table_name = ' + str(stage_table_name))
print('||-------------------')
print('|| Create SQL = ' + str(sqlCreateTable))
print('||-------------------')
print('||-----')

#------------
print('|| 2b. Execute Stage Table Create Statement')
print('||-----')

#Returns a string
result = ld.execLoadDBQuery(LoadEngine, sqlCreateTable)
print('|| Result: ' + ld.logMessage(result)[0])
print('||-----')

#------------
print('|| 3a. Check for Fixed-Width File')
print('||-----')
if file_format == 'FIXED':
    print('|| This file is Fixed-width and will require extra steps.')
else:
    print('|| This file is not Fixed-width and will move to the next step.')

if file_format == 'FIXED':
    print('||-----')

    #------------
    print('|| 3b. Get Temp Stage Table Create Statement')
    print('||-----')

    #Returns a string
    sqlCreateTempTable = ld.getStageTableTempCreate(schema_name, stage_table_name)

    print('||-------------------')
    print('|| Create Temp SQL = ' + str(sqlCreateTempTable))
    print('||-------------------')
    print('||-----')

    #------------
    print('|| 3c. Execute Temp Stage Table Create Statement')
    print('||-----')

    #Returns a string
    result = ld.execLoadDBQuery(LoadEngine,sqlCreateTempTable)
    print('|| Result: ' + ld.logMessage(result)[0])
    print('||-----')

    #------------
    print('|| 3d. Get Temp Stage Table Load Statement')
    print('||-----')

    #Returns a string
    sqlLoadTempTable = ld.getStageTableTempLoad(schema_name, stage_table_name, snowflake_stage_landing_directory, stage_filename, split_filename_pattern, header_row_count, null_if, compression_str, file_encoding, on_error)

    print('||-------------------')
    print('|| Create Temp SQL = ' + str(sqlLoadTempTable))
    print('||-------------------')
    print('||-----')

    #------------
    print('|| 3e. Execute Temp Stage Table Load Statement')
    print('||-----')

    #Returns a string
    result = ld.execLoadDBQuery(LoadEngine,sqlLoadTempTable)
    print('|| Result: ' + ld.logMessage(result)[0])
print('||-----')

#------------
print('|| 4a. Get Stage Load Statement')
print('||-----')

#Returns a string
sqlLoadTable = ld.getStageTableLoad(MetaEngine,source_id,file_format,schema_name,stage_table_name,split_filename_pattern,stage_filename,snowflake_stage_landing_directory,text_qualifier,column_delimiter,header_row_count,error_on_column_count_mismatch,on_error,date_format, time_format, timestamp_format, escape_char, escape_unenclosed_field, null_if, trim_space, compression_str, file_encoding, strip_outer_element)
print('||-------------------')
print('|| Load SQL = ' + str(sqlLoadTable))
print('||-------------------')
print('||-----')

#------------
print('|| 4b. Execute Stage Load Statement')
print('||-----')

#Returns a string
result = ld.execLoadDBQuery(LoadEngine,sqlLoadTable)
print('|| Result: ' + ld.logMessage(result)[0])
print('||-----')

#------------
print('|| 4c. Grab Bad Records and Store (partial loads only!)')
print('||-----')
if file_format == 'FIXED':
    error_row_count = None
    result = 'File is Fixed-width and does not capture bad records.'
else:
    #Returns a string
    error_row_count = ld.getBadRecords(LoadEngine, stage_table_name)
    result = 'The system has captured ' + str(error_row_count) + ' bad records!'
print('|| Result: ' + str(result))
print('||-----')

#------------
print('|| 4d. Collect Load Statistics')
print('||-----')
if file_format == 'FIXED':
    stage_table_temp_name = stage_table_name + '_TMP'
    stg = ld.getLoadHistory(LoadEngine, stage_table_temp_name)
else:
    stg = ld.getLoadHistory(LoadEngine, stage_table_name)

if stg.shape[0] == 0:
    errorMSG = 'No Records Found in Load History! Exiting...'
    print('|| ' + errorMSG)
    upd.updateProcessLog(MetaEngine,process_log_id,None,None,None,None,None,None,None,None,None,None,None,failedStr,errorMSG,batch_id)
    exit(errorCode)

inserted_rows = stg.inserted_rows[0]
file_rows = stg.file_rows[0]
error_row_count = stg.error_row_count[0]
load_status = stg.load_status[0]
file_status = stg.file_status[0].upper()
process_status = stg.process_status[0].upper()
process_message = load_status + ': ' + process_name + ' for ' + filename

print('||     Inserted_Rows: ' + str(inserted_rows)) 
print('||     File Rows: ' + str(file_rows))
print('||     Error Rows: ' + str(error_row_count))
print('||     Load Status: ' + str(load_status))
print('||     File Status: ' + str(file_status))
print('||     Process Status: ' + str(process_status))
print('||     Process Message: ' + str(process_message))
print('||-----')

#------------
print('|| 4e. Update Data File Status')
print('||-----')

result = upd.updateDataFile(MetaEngine, file_id, None, None, None, file_status, None, None, None, None, None, file_rows, error_row_count, inserted_rows, None)
print('|| Result: ' + str(result))
print('||-----')

#------------
print('|| 5a. Archive Files')
print('||-----')
if split_filename_pattern is None:
    filename_prefix = filename
else:
    filename_prefix = split_filename_pattern

# CAP - Changed
#file_old_path = landing_directory.split(bucket_name)[1]
#file_old_path = file_old_path[1:]
s3_file_path = landing_directory.split(bucket_name)[1]
s3_file_path = s3_file_path[1:]
processed_filename = bucket_archive + '/' + stage_filename # CAP add

print('s3_file_path:' + str(s3_file_path))
print('processed_filename:' + str(processed_filename))

result = s3t.rename_blob(bucket_name, s3_file_path + '/' + stage_filename, new_name=processed_filename)
#moveFile = f.moveProcessedFiles(bucket_name, filename_prefix=filename_prefix, old_path=file_old_path, new_path=bucket_archive)
#print('|| Result: ' + str(moveFile))
print('|| Result: ' + str(result['msg']))


if file_format == 'FIXED':
    print('|| 5b. Drop Temp Table')
    print('||-----')
    #Returns a string
    dropTable = ld.execTableDrop(LoadEngine, schema_name, stage_table_temp_name)
    print('|| Result: ' + str(dropTable))

###########################################  START STANDARD END PROCESS LOG BLOCK
print('|| 99. End Process Log')
print('||-----')
print('||     Process Name: ' + str(process_name)) 
print('||     process_log_id: ' + str(process_log_id))
print('||-----')

#Returns a string
updateCount = upd.updateProcessLog(MetaEngine, process_log_id, None, source_id, target_id, file_id, 
    None, None, None, inserted_rows, None, None, error_row_count, process_status,
    process_message, batch_id)

if updateCount != 1:
    print('|| Result: Failure')
    print('|| No process log was updated.')
    exit(errorCode)
else:
    print('|| Result: ' + str(updateCount))

if process_status.upper() == 'FAILED':
    print('|| Result: Failure')
    print('|| ' + process_message)
    exit(errorCode)

###########################################  END STANDARD END PROCESS LOG BLOCK

print('||-----')
print('||----------------SUCCESS----------------||')
print('||-----')

###############################################################################################  END MAIN

