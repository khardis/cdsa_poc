#Import needed Utilities
import argparse
from sys import exit
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
args = parser.parse_args()

filename = args.filename

#Validate Required Fields
if filename is None:
    print('filename argument is required')
    exit(InvalidArgsCode)
###########################################  END ARGUMENT VALIDATION

def getDataFileRecordByFilename(MetaEngine, filename):
    try:
        if MetaEngine is None:
            print("MetaEngine argument is required")
            exit(InvalidArgsCode)
        if filename is None:
            print("filename argument is required")
            exit(InvalidArgsCode)

        open_batch_status = 'STARTED'

        filename = str(filename) if filename is not None else None

        sqlQuery = """
         SELECT df.*
        FROM DATA_FILE df 
        LEFT JOIN BATCH b 
        ON (b.batch_id = df.batch_id AND b.batch_status = :batch_status)
        WHERE df.filename = LTRIM(RTRIM(:my_var)) 
        ORDER BY file_id;
        """
        sqlQuery = text(sqlQuery)
        resultSet= pd.read_sql(sqlQuery, MetaEngine, params={'my_var': filename, 'batch_status': open_batch_status})
        return resultSet

    except Exception as e:
        errorStr = 'ERROR (getDataFileRecordByFilename): ' + str(e)
        print(errorStr)
        raise

def getSourceAttributesById(MetaEngine, source_id):
    try:
        
        if MetaEngine is None:
            print("MetaEngine argument is required")
            exit(InvalidArgsCode)
        if source_id is None:
            print("source_id argument is required")
            exit(InvalidArgsCode)
        source_id = str(source_id) if source_id is not None else None

        sqlQuery = """
        
        SELECT *, sf.source_id as source_feed_id, t.target_id as target_object_id
        FROM SOURCE s
        JOIN DATA_OBJECT do
        on s.source_id = do.object_id
        LEFT JOIN SOURCE_FEED sf
        on s.source_id = sf.source_id
        LEFT JOIN TARGET t
        on sf.target_object_name = t.target_name
        WHERE s.source_id = :my_var
        
        """
        sqlQuery = text(sqlQuery)
        resultSet= pd.read_sql(sqlQuery, MetaEngine, params={'my_var': source_id})
        
        return resultSet

    except Exception as e:
        errorStr = 'ERROR (getSourceAttributesById): ' + str(e)
        print(errorStr)
        raise

def clean_none_value(field_name):
    try:
        if field_name is not None:
            if field_name.upper() == 'NONE':
                field_name = 'NONE'

        return field_name
    except Exception as e:
        errorStr = 'ERROR (clean_none_value)' + str(e)
        print(errorStr)
        raise

def is_empty(string):
    """is_empty checks a string and returns True if the string having value of either 'none', 'n/a', 'na', '', or None
    This is used to account for errors in manual DLF meta data input."""
    if string is None:
        return True
    elif string.strip() == '' or string.strip().lower() == 'none' or string.strip().lower() == 'n/a' or string.strip().lower() == 'na':
        return True
    else:
        return False

# Generates the Stage Table name for a file_id
def getStageTableName(MetaEngine, file_id):
    try:

        if MetaEngine is None:
            print('MetaEngine argument is required')
            exit(InvalidArgsCode)
        if file_id is None:
            print('file_id argument is required')
            exit(InvalidArgsCode)

        file_id = str(file_id) if file_id is not None else None

        sqlQuery = """
        SELECT  UPPER(MAX(LOWER(c.stage_name) || '_' || 
                RIGHT('0000000000000' || CAST(a.file_id AS VARCHAR), 7))) as table_name 
        FROM    DATA_FILE a
		JOIN	SOURCE c
		  ON	a.object_id = c.source_id
        WHERE   a.file_id = :my_var
        """
        sqlQuery = text(sqlQuery)
        resultSet = pd.read_sql(sqlQuery, MetaEngine, params={'my_var': int(file_id)})

        return resultSet

    except Exception as e:
        errorStr = 'ERROR (getStageTableName)' + str(e)
        print(errorStr)
        raise

# Returns Source Feed Columns data
def getSourceFeedColumnsById(MetaEngine, source_id):
    try:

        if MetaEngine is None:
            print('MetaEngine argument is required')
            exit(InvalidArgsCode)
        if source_id is None:
            print('source_id argument is required')
            exit(InvalidArgsCode)

        source_id = str(source_id) if source_id is not None else None

        sqlQuery = """

        SELECT  column_id,
                ordinal_position,
                column_name, 
                data_type,
                max_length,
                scale,
                fixed_width_start_position,
                fixed_width_length, 
                column_property
        FROM    source_feed_column
        WHERE   source_id = :my_var
        ORDER BY ordinal_position, column_id        

        """

        sqlQuery = text(sqlQuery)
        feed_columns = pd.read_sql(sqlQuery, MetaEngine, params={'my_var': source_id})

        return feed_columns

    except Exception as e:
        errorStr = 'ERROR (getSourceFeedColumnsById)' + str(e)
        print(errorStr)
        raise

def getStageTableCreate(MetaEngine, source_id, schema_name, stage_table_name, add_record_id_ind):
    """getStageTableCreate generates a SQL stage table create statement by looking up DLF Meta data table SOURCE_FEED_COLUMN and add_record_id_ind in SOURCE_FEED."""
    try:

        if MetaEngine is None:
            print('MetaEngine argument is required')
            exit(InvalidArgsCode)
        if source_id is None:
            print('source_id argument is required')
            exit(InvalidArgsCode)
        if schema_name is None:
            print('schema_name argument is required')
            exit(InvalidArgsCode)
        if stage_table_name is None:
            print('stage_table_name argument is required')
            exit(InvalidArgsCode)
        if add_record_id_ind is None:
            print('add_record_id_ind argument is required')
            exit(InvalidArgsCode)

        feed_columns = getSourceFeedColumnsById(MetaEngine, source_id)

        sqlColumnList = ''

        for index, x in feed_columns.iterrows():
            ordinal_position = feed_columns.ordinal_position[index]
            column_name = feed_columns.column_name[index].lower()
            data_type = feed_columns.data_type[index].lower()
            max_length = feed_columns.max_length[index]
            scale = feed_columns.scale[index]

            if ordinal_position > 1:
                sqlColumnList = sqlColumnList + ', '

            sqlColumnList = sqlColumnList + ' ' + str(column_name) + ' ' + str(data_type)
            if 'char' in data_type:
                sqlColumnList = sqlColumnList + '(' + str(int(max_length)) + ')'
            if data_type == 'decimal':
                sqlColumnList = sqlColumnList + '(' + str(int(max_length)) + ',' + str(int(scale)) + ')'

        if add_record_id_ind == 'Y':
            sqlColumnList = sqlColumnList + ', RECORD_ID number autoincrement start 1 increment 1 '

        sqlCreateTable = 'CREATE OR REPLACE TRANSIENT TABLE ' + schema_name + '.' + stage_table_name + ' (' + sqlColumnList + ')'

        return sqlCreateTable

    except Exception as e:
        errorStr = 'ERROR (getStageTableCreate)' + str(e)
        print(errorStr)
        raise

def getSourceFeedColumnsById(MetaEngine, source_id):
    try:

        if MetaEngine is None:
            print('MetaEngine argument is required')
            exit(InvalidArgsCode)
        if source_id is None:
            print('source_id argument is required')
            exit(InvalidArgsCode)

        source_id = str(source_id) if source_id is not None else None

        sqlQuery = """

        SELECT  column_id,
                ordinal_position,
                column_name, 
                data_type,
                max_length,
                scale,
                fixed_width_start_position,
                fixed_width_length, 
                column_property
        FROM    source_feed_column
        WHERE   source_id = :my_var
        ORDER BY ordinal_position, column_id        

        """

        sqlQuery = text(sqlQuery)
        feed_columns = pd.read_sql(sqlQuery, MetaEngine, params={'my_var': source_id})

        return feed_columns

    except Exception as e:
        errorStr = 'ERROR (getSourceFeedColumnsById)' + str(e)
        print(errorStr)
        raise

def getStageTableLoad(MetaEngine, source_id, file_format, schema_name, stage_table_name, split_filename_pattern,
                      stage_filename, landing_directory, text_qualifier, column_delimiter, header_row_count,
                      error_on_column_count_mismatch, on_error, date_format, time_format, timestamp_format, escape_char,
                      escape_unenclosed_field, null_if, trim_space, compression_str, file_encoding, strip_outer_element):
    """getStageTableLoad generates a COPY command to load stage table using the following DLF meta data:
    - SOURCE: source_id
    - DATA_OBJECT: file_format, landing_directory, text_qualifier, column_delimiter, header_row_count, file_encoding
    - SOURCE_FEED: split_filename_pattern, error_on_column_count_mismatch, on_error, date_format, time_format, timestamp_format, escape_char,
                    escape_unenclosed_field, null_if, trim_space, compression_str
    """
    try:

        if MetaEngine is None:
            print('MetaEngine argument is required')
            exit(InvalidArgsCode)
        if source_id is None:
            print('source_id argument is required')
            exit(InvalidArgsCode)
        if file_format is None:
            print('file_format argument is required')
            exit(InvalidArgsCode)
        if schema_name is None:
            print('schema_name argument is required')
            exit(InvalidArgsCode)
        if stage_table_name is None:
            print('stage_table_name argument is required')
            exit(InvalidArgsCode)
        if stage_filename is None:
            print('stage_filename argument is required')
            exit(InvalidArgsCode)
        if landing_directory is None:
            print('landing_directory argument is required')
            exit(InvalidArgsCode)
        if header_row_count is None:
            print('header_row_count argument is required')
            exit(InvalidArgsCode)

        mySqlColumnList = ''
        mySqlColumnList2 = ''
        mySqlLine1 = ''
        mySqlLine2 = ''

        feed_columns = getSourceFeedColumnsById(MetaEngine, source_id)

        for index,x in feed_columns.iterrows():
            ordinal_position = feed_columns.ordinal_position[index]
            column_name = feed_columns.column_name[index].lower()
            fixed_width_start_position = feed_columns.fixed_width_start_position[index]
            fixed_width_length = feed_columns.fixed_width_length[index]
            data_type = feed_columns.data_type[index].lower()

            if ordinal_position > 1:
                #mySqlLine1 = mySqlLine1 + ', '
                mySqlColumnList = mySqlColumnList +  ', '
                mySqlColumnList2 = mySqlColumnList2 +  ', '

            #mySqlLine1 = mySqlLine1 + column_name

            mySqlColumnList = mySqlColumnList + ' $' + str(ordinal_position)

            mySqlColumnList2 = mySqlColumnList2 + " " + column_name

        mySqlLine1 = 'COPY INTO ' + schema_name + '.' + stage_table_name + ' '

        if split_filename_pattern:
            tmp_filename = """ PATTERN = '""" + split_filename_pattern + """' """
        else:
            tmp_filename = stage_filename

        mySqlLine3 = """FILE_FORMAT = (TYPE = 'CSV' """
            
        mySqlLine3 = mySqlLine3 + """COMPRESSION='""" + compression_str + """' """

        if not is_empty(text_qualifier):
            mySqlLine3 = mySqlLine3 + """FIELD_OPTIONALLY_ENCLOSED_BY='""" + text_qualifier + """' """

        if not is_empty(column_delimiter):
            mySqlLine3 = mySqlLine3 + """FIELD_DELIMITER='""" + str(column_delimiter) + """' """
        else:
            mySqlLine3 = mySqlLine3 + 'FIELD_DELIMITER=NONE '

        if header_row_count > 0:
            mySqlLine3 = mySqlLine3 + "SKIP_HEADER=" + str(header_row_count) + " "

        if null_if is None:
            mySqlLine3 = mySqlLine3 + "NULL_IF=('\"\"','',' ') "
        else:
            mySqlLine3 = mySqlLine3 + "NULL_IF=(" + null_if + ") "

        if escape_char:
            mySqlLine3 = mySqlLine3 + "ESCAPE='" + escape_char + "' "

        if escape_unenclosed_field:
            mySqlLine3 = mySqlLine3 + "ESCAPE_UNENCLOSED_FIELD ='" + escape_unenclosed_field + "' "

        if trim_space:
            mySqlLine3 = mySqlLine3 + "TRIM_SPACE=" + trim_space + " "

        if file_encoding:
            mySqlLine3 = mySqlLine3 + "ENCODING='" + file_encoding + "' "

        mySqlLine3 = mySqlLine3 + "DATE_FORMAT='" + date_format + "' "
        mySqlLine3 = mySqlLine3 + "TIME_FORMAT='" + time_format + "' "
        mySqlLine3 = mySqlLine3 + "TIMESTAMP_FORMAT='" + timestamp_format + "' "
        mySqlLine3 = mySqlLine3 + "ERROR_ON_COLUMN_COUNT_MISMATCH=" + error_on_column_count_mismatch + " ) ON_ERROR= '" + on_error + "'"

        mySqlFull = mySqlLine1 + ' ' + mySqlLine2 + ' ' + mySqlLine3

        return mySqlFull

    except Exception as e:
        errorStr = 'ERROR (getStageTableLoad)' + str(e)
        print(errorStr)
        raise

def getLoadHistory(LoadEngine, stage_table_name):
    """getLoadHistory gathers the Load History statistics for a table in Snowflake"""
    try:

        if LoadEngine is None:
            print('LoadEngine argument is required')
            exit(InvalidArgsCode)
        if stage_table_name is None:
            print('stage_table_name argument is required')
            exit(InvalidArgsCode)

        sqlQuery = """
        SELECT SUM(ROW_COUNT) as inserted_rows,  
               SUM(ROW_PARSED) as file_rows,
               SUM(ROW_PARSED)-SUM(ROW_COUNT) as error_row_count,
               MAX(STATUS) as load_status,
               CASE WHEN MAX(STATUS) = 'LOAD_FAILED' THEN 'Error' ELSE 'Staged' END as file_status,
               CASE WHEN MAX(STATUS) = 'LOAD_FAILED' THEN 'Failed' ELSE 'Completed' END as process_status
        FROM INFORMATION_SCHEMA.LOAD_HISTORY
        WHERE TABLE_NAME = :stage_table_name
        GROUP BY TABLE_NAME
        """

        sqlQuery = text(sqlQuery)
        result = pd.read_sql(sqlQuery, LoadEngine, params={'stage_table_name': stage_table_name})

        return result

    except Exception as e:
        errorStr = 'ERROR (getLoadHistory)' + str(e)
        print(errorStr)
        raise

def updateDataFile(MetaEngine, file_id, object_id=None, file_pattern=None, file_received_date=None, file_status=None, 
    file_byte_size=None, filename=None, expected_row_count=None, row_count=None, 
    fm_file_id=None, fm_good_record_count=None, fm_error_record_count=None, stg_good_record_count=None, file_path=None):
    try:

        if MetaEngine is None:
            print("MetaEngine argument is required")
            exit(InvalidArgsCode)
        if file_id is None:
            print("file_id argument is required")
            exit(InvalidArgsCode)

        file_id = int(file_id)
        object_id = int(object_id) if object_id is not None else None
        file_received_date = str(file_received_date) if file_received_date is not None else None
        file_status = str(file_status) if file_status is not None else None
        file_byte_size = str(file_byte_size) if file_byte_size is not None else None
        filename = str(filename) if filename is not None else None
        expected_row_count = int(expected_row_count) if expected_row_count is not None else None
        row_count = int(row_count) if row_count is not None else None
        fm_file_id = int(fm_file_id) if fm_file_id is not None else None
        fm_good_record_count = int(fm_good_record_count) if fm_good_record_count is not None else None
        fm_error_record_count = int(fm_error_record_count) if fm_error_record_count is not None else None
        stg_good_record_count = int(stg_good_record_count) if stg_good_record_count is not None else None
        file_path = str(file_path) if file_path is not None else None

        sqlQuery = """
            UPDATE DATA_FILE SET
            
                object_id = COALESCE(:object_id, object_id), 
                file_pattern = COALESCE(:file_pattern, file_pattern), 
                file_received_date = COALESCE(:file_received_date, file_received_date), 
                file_status = COALESCE(:file_status, file_status), 
                file_byte_size = COALESCE(:file_byte_size, file_byte_size), 
                filename = COALESCE(:filename, filename), 
                expected_row_count = COALESCE(:expected_row_count, expected_row_count), 
                row_count = COALESCE(:row_count, row_count), 
                fm_file_id = COALESCE(:fm_file_id, fm_file_id), 
                fm_good_record_count = COALESCE(:fm_good_record_count, fm_good_record_count), 
                fm_error_record_count = COALESCE(:fm_error_record_count, fm_error_record_count), 
                stg_good_record_count = COALESCE(:stg_good_record_count, stg_good_record_count), 
                file_path = COALESCE(:file_path, file_path), 
                modified_date = CURRENT_TIMESTAMP(),
                modified_by = CURRENT_USER()

            WHERE file_id = :file_id;
        """
        sqlQuery = text(sqlQuery)

        with MetaEngine.begin() as conn:
            resultObject = conn.execute(sqlQuery, dict(object_id=object_id, file_pattern=file_pattern,
                                    file_received_date=file_received_date, file_status=file_status, file_byte_size=file_byte_size, filename=filename,
                                    expected_row_count=expected_row_count, row_count=row_count, fm_file_id=fm_file_id, fm_good_record_count=fm_good_record_count, 
                                    fm_error_record_count=fm_error_record_count, stg_good_record_count=stg_good_record_count, file_path=file_path, file_id=file_id))
            returnCode = resultObject.rowcount
        return returnCode

    except Exception as e:
        errorStr = 'ERROR (updateDataFile): ' + str(e)
        print(errorStr)
        raise

###############################################################################################  START MAIN

print('||-------------run_stage_load.py--------------||')
print('||-----')

process_name = 'run_stage_load.py'

#Get attributes from Metadata needed for process
print('|| 1. Get Attributes of the Source')
print('||-----')

#------------file Attribs
fileAttr = getDataFileRecordByFilename(MetaEngine, filename)
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
clean_none_value(escape_char)
clean_none_value(escape_unenclosed_field)

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
    exit(InvalidArgsCode)
elif object_id is None:
    print('|| Result: Failure')
    errorMSG = 'There is no Source in DATA_OBJECT that matches the filename_pattern.'
    print('|| ' + errorMSG)
    exit(InvalidArgsCode)
elif batch_id is None:
    print('|| Result: Failure')
    errorMSG = 'This file is not registered in an active Batch.'
    print('|| ' + errorMSG)
    exit(InvalidArgsCode)
elif target_id is None:
    print('|| Result: Failure')
    errorMSG = 'This Source has a target defined in target_object_name that has no TARGET record.'
    print('|| ' + errorMSG)
    exit(InvalidArgsCode)
elif (
        stage_name is None or
        stage_name == ''
    ):
    print('|| Result: Failure')
    errorMSG = 'This Source does not have a stage_name populated.'
    print('|| ' + errorMSG)
    exit(InvalidArgsCode)
print('|| Result: Success')
print('||-----')

#------------
print('|| 2a. Get Stage Table Create Statement')
print('||-----')

tableName = getStageTableName(MetaEngine, file_id)
stage_table_name = tableName.table_name[0]

#Returns a string
sqlCreateTable = getStageTableCreate(MetaEngine,source_id,schema_name,stage_table_name,add_record_id_ind)

print('||     stage_table_name = ' + str(stage_table_name))
print('||-------------------')
print('|| Create SQL = ' + str(sqlCreateTable))
print('||-------------------')
print('||-----')

#------------
print('|| 2b. Execute Stage Table Create Statement')
print('||-----')

#Returns a string
#result = ld.execLoadDBQuery(LoadEngine, sqlCreateTable)
##### ADD execution of sqlCreateTable here as create delta table similar to:
#####     spark.sql(f"""
#        -- STAGE TABLE
#        CREATE TABLE IF NOT EXISTS {bronze_db}.STG_TABLE_NAME_HERE (
#          col1 STRING,
#          col2 STRING, ...
#          created_date TIMESTAMP,
#          created_by STRING,
#          modified_date TIMESTAMP,
#          modified_by STRING
#        ) USING DELTA
#    """)
#print('|| Result: ' + ld.logMessage(result)[0])
print('||-----')

#------------
print('||-----')

#------------
print('|| 4a. Get Stage Load Statement')
print('||-----')

#Returns a string
sqlLoadTable = getStageTableLoad(MetaEngine,source_id,file_format,schema_name,stage_table_name,split_filename_pattern,stage_filename,snowflake_stage_landing_directory,text_qualifier,column_delimiter,header_row_count,error_on_column_count_mismatch,on_error,date_format, time_format, timestamp_format, escape_char, escape_unenclosed_field, null_if, trim_space, compression_str, file_encoding, strip_outer_element)
print('||-------------------')
print('|| Load SQL = ' + str(sqlLoadTable))
print('||-------------------')
print('||-----')

#------------
print('|| 4b. Execute Stage Load Statement')
print('||-----')

#------------
#------------
print('|| 4d. Collect Load Statistics')
print('||-----')
stg = getLoadHistory(LoadEngine, stage_table_name)

if stg.shape[0] == 0:
    errorMSG = 'No Records Found in Load History! Exiting...'
    print('|| ' + errorMSG)
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

result = updateDataFile(MetaEngine, file_id, None, None, None, file_status, None, None, None, None, None, file_rows, error_row_count, inserted_rows, None)
print('|| Result: ' + str(result))
print('||-----')

###########################################  START STANDARD END PROCESS LOG BLOCK
#Returns a string

if process_status.upper() == 'FAILED':
    print('|| Result: Failure')
    print('|| ' + process_message)
    exit(errorCode)

###########################################  END STANDARD END PROCESS LOG BLOCK

print('||-----')
print('||----------------SUCCESS----------------||')
print('||-----')

###############################################################################################  END MAIN

