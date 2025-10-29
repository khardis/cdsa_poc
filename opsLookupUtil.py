# ------------------------------------------------------------------------------------------------------------------
# opsLookupUtil.py
# ------------------------------------------------------------------------------------------------------------------

__version__ = "lkp_1.0"

successCode = 0
errorCode = 1
InvalidArgsCode = 2
failedStr = 'FAILED'
completedStr = 'COMPLETED'

#BATCH stuff
def getActiveBatchRecordByBatchName(ctx, batch_name):
    try:

        if ctx is None:
            print('ctx argument is required')
            raise ValueError('Invalid arguments')
        if batch_name is None:
            print('batch_name argument is required')
            raise ValueError('Invalid arguments')

        batch_name = str(batch_name) if batch_name is not None else None

        df = ctx.spark.sql(f"""
        SELECT * 
        FROM meta_db.batch
        WHERE batch_name = LTRIM(RTRIM('{batch_name}')) 
        AND end_time IS NULL 
        ORDER BY batch_id;
        """)

        return df

    except Exception as e:
        errorStr = 'ERROR (getActiveBatchRecordByBatchName): ' + str(e)
        print(errorStr)
        raise

def getActiveBatchIdByBatchName(ctx, batch_name):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if batch_name is None:
            print('batch_name argument is required')
            raise ValueError('Invalid arguments')

        batch_name = str(batch_name) if batch_name is not None else None

        df = getActiveBatchRecordByBatchName(ctx, batch_name)

        if df.count() == 0:
            return None

        dfCount = df.count()

        if dfCount > 1:
            print('WARNING (getActiveBatchIdByBatchName): MORE THAN ONE RECORD RETURNED (' + str(dfCount) + ')')
            df = df.limit(1)

        batch_id = str(df.first()['batch_id'])
        return batch_id

    except Exception as e:
        errorStr = 'ERROR (getActiveBatchIdByBatchName): ' + str(e)
        print(errorStr)
        raise

def getBatchRecordByBatchId(ctx, batch_id):
    try:
        if ctx is None:
            print("ctx argument is required")
            raise ValueError('Invalid arguments')
        if batch_id is None:
            print("batch_id argument is required")
            raise ValueError('Invalid arguments')

        batch_id = str(batch_id) if batch_id is not None else None

        sqlQuery = f"""
        SELECT * 
        FROM meta_db.batch
        WHERE batch_id = '{batch_id}';
        """

        df= ctx.spark.sql(sqlQuery)

        return df

    except Exception as e:
        errorStr = 'ERROR (getBatchRecordByBatchId): ' + str(e)
        print(errorStr)
        raise

def getBatchNameByBatchId(ctx, batch_id):
    try:

        if ctx is None:
            print("ctx argument is required")
            raise ValueError('Invalid arguments')
        if batch_id is None:
            print("batch_id argument is required")
            raise ValueError('Invalid arguments')

        batch_id = str(batch_id) if batch_id is not None else None

        df = getBatchRecordByBatchId(ctx, batch_id)

        if df.count() == 0:
            return None

        if df.count() > 1:
            print("WARNING (getBatchNameByBatchId): MORE THAN ONE RECORD RETURNED")
            df = df.limit(1)

        return str(df.first()['batch_name'])

    except Exception as e:
        errorStr = 'ERROR (getBatchNameByBatchId): ' + str(e)
        print(errorStr)
        raise

def getCompletedBatchByFilename(ctx, filename):
    try:
        if ctx is None:
            print("ctx argument is required")
            raise ValueError('Invalid arguments')
        if filename is None:
            print("filename argument is required")
            raise ValueError('Invalid arguments')

        filename = str(filename) if filename is not None else None

        sqlQuery = f"""
       SELECT b.* 
        FROM meta_db.batch b 
        JOIN ( 
          SELECT max(b.batch_id) as batch_id
          FROM meta_db.batch b 
          JOIN meta_db.data_file bdf 
          ON b.batch_id = bdf.batch_id
           WHERE b.batch_status = 'COMPLETED'
           AND bdf.filename = LTRIM(RTRIM(:my_var)) 
	  UNION ALL
	  SELECT MAX(b.batch_id) as batch_id
	  FROM meta_db.batch b
	  JOIN meta_db.extract_data_file edf
	  ON b.batch_id = edf.batch_id
	  where b.batch_status = 'COMPLETED'
	  AND edf.extract_filename = LTRIM(RTRIM('{filename}'))
        ) mb  
        ON b.batch_id = mb.batch_id;  
        """
        
        df= ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (getCompletedBatchByFilename): ' + str(e)
        print(errorStr)
        raise

#DATA_FILE stuff
def getDataFileRecordByFileId(ctx, file_id):
    try:
        if ctx is None:
            print("ctx argument is required")
            raise ValueError('Invalid arguments')
        if file_id is None:
            print("file_id argument is required")
            raise ValueError('Invalid arguments')

        open_batch_status = 'STARTED'
        file_id = str(file_id) if file_id is not None else None

        sqlQuery = f"""
        SELECT df.*
        FROM meta_db.data_file df 
        LEFT JOIN meta_db.batch b 
        ON (b.batch_id = df.batch_id AND b.batch_status = :batch_status)
        WHERE df.file_id = :my_var;
        """
        
        df= ctx.spark.sql(sqlQuery, ctx.spark, params={'my_var': file_id, 'batch_status':
            open_batch_status})
        return df

    except Exception as e:
        errorStr = 'ERROR (getDataFileRecordByFileId): ' + str(e)
        print(errorStr)
        raise

def getDataFileRecordByFilename(ctx, filename):
    try:
        if ctx is None:
            print("ctx argument is required")
            raise ValueError('Invalid arguments')
        if filename is None:
            print("filename argument is required")
            raise ValueError('Invalid arguments')

        open_batch_status = 'STARTED'

        filename = str(filename) if filename is not None else None

        sqlQuery = f"""
         SELECT df.*
        FROM meta_db.data_file df 
        LEFT JOIN meta_db.batch b 
        ON (b.batch_id = df.batch_id AND b.batch_status = '{open_batch_status}')
        WHERE df.filename = LTRIM(RTRIM('{filename}')) 
        ORDER BY file_id;
        """
        
        df= ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (getDataFileRecordByFilename): ' + str(e)
        print(errorStr)
        raise

def getFileIdByFilename(ctx, filename):
    try:

        if ctx is None:
            print("ctx argument is required")
            raise ValueError('Invalid arguments')
        if filename is None:
            print("filename argument is required")
            raise ValueError('Invalid arguments')
        filename = str(filename) if filename is not None else None
        df = getDataFileRecordByFilename(ctx, filename)

        if df.count() == 0:
            return None

        if df.count() != 1:
            print("WARNING (getFileIdByFilename): MORE THAN ONE RECORD RETURNED")
            df = df.limit(1)

        return str(df.first()['file_id'])

    except Exception as e:
        errorStr = 'ERROR (getFileIdByFilename): ' + str(e)
        print(errorStr)
        raise

def getFileNameByFileId(ctx, file_id):
    try:

        if ctx is None:
            print("ctx argument is required")
            raise ValueError('Invalid arguments')
        if file_id is None:
            print("file_id argument is required")
            raise ValueError('Invalid arguments')

        file_id = str(file_id) if file_id is not None else None

        df = getDataFileRecordByFileId(ctx, file_id)

        if df.count() == 0:
            return None

        return str(df.first()['filename'])

    except Exception as e:
        errorStr = 'ERROR (getFileNameByFileId): ' + str(e)
        print(errorStr)
        raise

def getFileAttributesByFileId(ctx, file_id):
    try:

        if ctx is None:
            print("ctx argument is required")
            raise ValueError('Invalid arguments')
        if file_id is None:
            print("file_id argument is required")
            raise ValueError('Invalid arguments')

        file_id = str(file_id) if file_id is not None else None

        sqlQuery = f"""
        SELECT a.file_path, 
                COALESCE(a.expected_row_count,0) as expected_row_count, 
                b.column_delimiter, 
                b.row_delimiter, 
                COALESCE(b.header_row_count,0) as header_row_count, 
                COALESCE(b.field_count,0) as field_count, 
                a.filename, 
                b.object_id, 
                b.object_name, 
                b.file_format, 
                b.schema_name, 
                b.landing_directory 
        FROM meta_db.data_file a 
        JOIN meta_db.data_object b 
        ON a.object_id = b.object_id 
        WHERE a.file_id = '{file_id}';
        """

        
        df = ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (getFileAttributesByFileId): ' + str(e)
        print(errorStr)
        raise

def getFileAttributesByFileName(ctx, filename):
    try:

        if ctx is None:
            print("ctx argument is required")
            raise ValueError('Invalid arguments')
        if filename is None:
            print("filename argument is required")
            raise ValueError('Invalid arguments')
        filename = str(filename) if filename is not None else None

        file_id = getFileIdByFilename(ctx, filename)
        if file_id.count() == 0:
            errorMSG = 'ERROR (getFileAttributesByFileName): No File ID for this Filename.  Exiting...'
            print(errorMSG)
            raise ValueError('Error occurred')

        myOutputData = getFileAttributesByFileId(ctx, file_id)
        return myOutputData

    except Exception as e:
        errorStr = 'ERROR (getFileAttributesByFileName): ' + str(e)
        print(errorStr)
        raise


# BATCH_DATA_FILE stuff
def getBatchDataFileRecordBySourceCode(ctx, batch_name, object_id):
    try:
        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if batch_name is None:
            print("batch_name argument is required")
            raise ValueError('Invalid arguments')
        if object_id is None:
            print("object_id argument is required")
            raise ValueError('Invalid arguments')

        batch_name = str(batch_name) if batch_name is not None else None
        object_id = str(object_id) if object_id is not None else None

        open_batch_status = 'STARTED'
        file_status = 'STAGED'

        sqlQuery = f"""
          SELECT bdf.*
        FROM meta_db.batch b
        JOIN meta_db.data_file bdf
          ON b.batch_id = bdf.batch_id
        WHERE bdf.object_id = '{object_id}'
          AND b.batch_name = '{batch_name}'
          AND b.batch_status = '{open_batch_status}'
          AND UPPER(bdf.file_status) = '{file_status}'
        """
        
        df= ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (getBatchDataFileRecordBySourceCode): ' + str(e)
        print(errorStr)
        raise

def getLastBatchDataFileRecordBySourceId(ctx, batch_name, object_id):
    try:
        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if batch_name is None:
            print("batch_name argument is required")
            raise ValueError('Invalid arguments')
        if object_id is None:
            print("object_id argument is required")
            raise ValueError('Invalid arguments')

        batch_name = str(batch_name) if batch_name is not None else None
        object_id = str(object_id) if object_id is not None else None

        open_batch_status = 'STARTED'

        sqlQuery = f"""
       SELECT bdf.*
        FROM meta_db.batch b
        JOIN meta_db.data_file bdf
          ON b.batch_id = bdf.batch_id
        WHERE bdf.object_id = '{object_id}'
          AND b.batch_name = '{batch_name}'
          AND b.batch_status = '{open_batch_status}'
          AND UPPER(bdf.file_status) IN ('STAGED', 'REGISTERED')
        ORDER BY bdf.file_id DESC
        """
        
        df= ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (getLastBatchDataFileRecordBySourceId): ' + str(e)
        print(errorStr)
        raise

def getBatchDataFileRecordByBatchFile(ctx, batch_id, file_id):
    try:
        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if batch_id is None:
            print("batch_id argument is required")
            raise ValueError('Invalid arguments')
        if file_id is None:
            print("file_id argument is required")
            raise ValueError('Invalid arguments')
        batch_id = str(batch_id) if batch_id is not None else None
        file_id = str(file_id) if file_id is not None else None
        sqlQuery = f"""
        SELECT  *
        FROM    meta_db.data_file
        WHERE   batch_id = '{batch_id}'
        AND     file_id = '{file_id}'
        """
        
        df= ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (getBatchDataFileRecordBySourceCode): ' + str(e)
        print(errorStr)
        raise

#DATA_OBJECT stuff
def getDataObjectRecordByFilename(ctx, filename):
    try:

        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if filename is None:
            print("filename argument is required")
            raise ValueError('Invalid arguments')

        filename = str(filename) if filename is not None else None

        #requires the filename_pattern in DATA_OBJECT to contain a SQL wildcard
        sqlQuery = f"""
        SELECT do.*, s.source_code
        FROM meta_db.data_object do 
        JOIN meta_db.source s ON s.source_id = do.object_id 
        WHERE '{filename}' LIKE do.filename_pattern;
        """
        
        df= ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (getDataObjectRecordByFilename): ' + str(e)
        print(errorStr)
        raise

def getDataObjectRecordByObjectId(ctx, object_id):
    try:

        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if object_id is None:
            print("object_id argument is required")
            raise ValueError('Invalid arguments')

        object_id = str(object_id) if object_id is not None else None

        sqlQuery = f"""
        SELECT *
        FROM meta_db.data_object
        WHERE object_id = '{object_id}';
        """

        
        df= ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (getDataObjectRecordByObjectId): ' + str(e)
        print(errorStr)
        raise

def getObjectIdByObjectName(ctx, object_name):
    try:

        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if object_name is None:
            print("object_name argument is required")
            raise ValueError('Invalid arguments')
        object_name = str(object_name) if object_name is not None else None

        sqlQuery = f"""
        SELECT MAX(object_id) as object_id, COUNT(*) as num_rows
        FROM meta_db.data_object
        WHERE object_name = LTRIM(RTRIM('{object_name}'));
        """

        
        df= ctx.spark.sql(sqlQuery)

        if df.count() == 0:
            return None

        return str(df.first()['object_id'])

    except Exception as e:
        errorStr = 'ERROR (getObjectIdByObjectName): ' + str(e)
        print(errorStr)
        raise


#PROCESS and PROCESS_LOG stuff
def getProcessRecordByProcessName(ctx, process_name):
    try:

        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if process_name is None:
            print("process_name argument is required")
            raise ValueError('Invalid arguments')
        process_name = str(process_name) if process_name is not None else None

        sqlQuery = f"""
        SELECT * 
        FROM meta_db.process
        WHERE process_name = LTRIM(RTRIM('{process_name}'));
        """
        
        df= ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (getProcessRecordByProcessName): ' + str(e)
        print(errorStr)
        raise

def getProcessIdByProcessName(ctx, process_name):
    try:

        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if process_name is None:
            print("process_name argument is required")
            raise ValueError('Invalid arguments')
        process_name = str(process_name) if process_name is not None else None

        df = getProcessRecordByProcessName(ctx, process_name)

        if df.count() == 0:
            return None

        if df.count() != 1:
            print("WARNING (getProcessIdByProcessName): MORE THAN ONE RECORD RETURNED")
            df = df.limit(1)

        return str(df.first()['process_id'])

    except Exception as e:
        errorStr = 'ERROR (getProcessIdByProcessName): ' + str(e)
        print(errorStr)
        raise

def getProcessLogRecordById(ctx, process_log_id):
    try:

        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if process_log_id is None:
            print("process_log_id argument is required")
            raise ValueError('Invalid arguments')

        process_log_id = str(process_log_id) if process_log_id is not None else None

        sqlQuery = f"""
        SELECT * 
        FROM meta_db.process_log
        WHERE process_log_id = '{process_log_id}'
        ORDER BY 1;
        """
        
        df= ctx.spark.sql(sqlQuery)

        return df

    except Exception as e:
        errorStr = 'ERROR (getProcessLogRecordById): ' + str(e)
        print(errorStr)
        raise

def getActiveProcessLogIdByName(ctx, process_name):
    try:

        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if process_name is None:
            print("process_name argument is required")
            raise ValueError('Invalid arguments')

        process_name = str(process_name) if process_name is not None else None

        processId = getProcessIdByProcessName(ctx, process_name)

        if processId is None:
            print("processId not found")
            raise ValueError('Error occurred')

        sqlQuery = f"""
        SELECT MAX(process_log_id) as process_log_id 
        FROM meta_db.process_log
        WHERE process_id = '{processId}' 
        AND   end_time IS NULL; 
        """
        
        df= ctx.spark.sql(sqlQuery)

        if df.count() == 0:
            return None

        return str(df.first()['process_log_id'])

    except Exception as e:
        errorStr = 'ERROR (getActiveProcessLogIdByName): ' + str(e)
        print(errorStr)
        raise


#TARGET and TARGET_VIEW stuff
def getTargetAttributesByName(ctx, target_name):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if target_name is None:
            print('target_name argument is required')
            raise ValueError('Invalid arguments')

        target_name = str(target_name) if target_name is not None else None

        sqlQuery = f"""

        SELECT  t.*, tv.*, 
                do.object_id as src_object_id,
                s.source_type,
                s.source_code,
                t.target_id as tgt_object_id,
                s.source_id
        FROM meta_db.target t
        LEFT JOIN meta_db.target_view tv
          ON t.target_id = tv.target_id
        LEFT JOIN meta_db.data_object do
          ON tv.source_object_name = do.object_name
		LEFT JOIN meta_db.source s
        ON 'VW_STG_' || UPPER (s.source_code) = UPPER (do.object_name)
          OR 'VW__STG_' || UPPER (s.source_code) = UPPER (do.object_name)
        WHERE t.target_name = '{target_name}'
        """

        
        target = ctx.spark.sql(sqlQuery)

        return target

    except Exception as e:
        errorStr = 'ERROR (getTargetAttributesByName)' + str(e)
        print(errorStr)
        raise

def getTargetViewAttribForIDChange(ctx):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')

        # 2019/02/22 add cr ee has site_id_change_col but not implemented across all projects
        sql_site_id_check = "select * from information_schema.columns where table_name = 'TARGET_VIEW' and column_name = 'site_id_change_col'"
        check_result = ctx.spark.sql(sql_site_id_check)

        if check_result.count() == 0:
            sqlQuery = f"""
            SELECT *
            FROM meta_db.target_view
            WHERE nullif(indiv_id_change_col,'') IS NOT NULL 
            OR nullif(add_id_change_col,'') IS NOT NULL 
            OR nullif(house_id_change_col,'') IS NOT NULL  
            """
        else:
            sqlQuery = f"""
            SELECT *
            FROM meta_db.target_view
            WHERE nullif(indiv_id_change_col,'') IS NOT NULL 
            OR nullif(add_id_change_col,'') IS NOT NULL 
            OR nullif(house_id_change_col,'') IS NOT NULL
            OR nullif(site_id_change_col,'') IS NOT NULL  
            """

        
        target = ctx.spark.sql(sqlQuery)

        return target

    except Exception as e:
        errorStr = 'ERROR (getTargetViewAttribForIDChange)' + str(e)
        print(errorStr)
        raise

def getTargetViewColumnOverride(ctx, view_name, target_id):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if view_name is None:
            print('view_name argument is required')
            raise ValueError('Invalid arguments')
        if target_id is None:
            print('target_id argument is required')
            raise ValueError('Invalid arguments')
        view_name = str(view_name) if view_name is not None else None
        target_id = str(target_id) if target_id is not None else None
        sqlQuery = f"""

        SELECT *, UPPER(column_name) as column_name_upper
        FROM meta_db.target_view_column_override
        WHERE target_id = '""" + str(target_id) + """'
          AND view_name = '""" + view_name + """'

        """

        
        target_cols = ctx.spark.sql(sqlQuery)

        return target_cols

    except Exception as e:
        errorStr = 'ERROR (getTargetViewColumnOverride)' + str(e)
        print(errorStr)
        raise

def getTargetPurgeInfo(ctx):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')

        sqlQuery = f"""

            SELECT * 
            FROM meta_db.target
            WHERE COALESCE(purge_type,'') <> ''

        """

        
        target_purge = ctx.spark.sql(sqlQuery)

        return target_purge

    except Exception as e:
        errorStr = 'ERROR (getTargetPurgeInfo)' + str(e)
        print(errorStr)
        raise


def getFileSourcePurgeInfo(ctx):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')

        sqlQuery = f"""          
                SELECT * 
                FROM meta_db.data_file df
                JOIN meta_db.source s
                ON df.object_id = s.source_id
                JOIN meta_db.data_object do
                on s.source_id = do.object_id
                WHERE COALESCE(s.purge_type,'') <> ''
                
        """

        
        stage_purge = ctx.spark.sql(sqlQuery)

        return stage_purge

    except Exception as e:
        errorStr = 'ERROR (getTargetPurgeInfo)' + str(e)
        print(errorStr)
        raise

#Combination Stuff
def getSourceAttributesById(ctx, source_id):
    try:
        
        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if source_id is None:
            print("source_id argument is required")
            raise ValueError('Invalid arguments')
        source_id = str(source_id) if source_id is not None else None

        sqlQuery = f"""
        
        SELECT *, sf.source_id as source_feed_id, t.target_id as target_object_id
        FROM meta_db.source s
        JOIN meta_db.data_object do
        on s.source_id = do.object_id
        LEFT JOIN meta_db.source_feed sf
        on s.source_id = sf.source_id
        LEFT JOIN meta_db.target t
        on sf.target_object_name = t.target_name
        WHERE s.source_id = '{source_id}'
        
        """
        
        df= ctx.spark.sql(sqlQuery)
        
        return df

    except Exception as e:
        errorStr = 'ERROR (getSourceAttributesById): ' + str(e)
        print(errorStr)
        raise

def getSourceAttributesByName(ctx, object_name):
    try:
        
        if spark is None:
            print("spark argument is required")
            raise ValueError('Invalid arguments')
        if object_name is None:
            print("object_name argument is required")
            raise ValueError('Invalid arguments')
        object_name = str(object_name) if object_name is not None else None

        sqlQuery = f"""
        
        SELECT *, t.target_id as target_object_id
        FROM meta_db.source s
        JOIN meta_db.data_object do
        on s.source_id = do.object_id
        LEFT JOIN meta_db.source_feed sf
        on s.source_id = sf.source_id
        LEFT JOIN meta_db.target t
        on sf.target_object_name = t.target_name
        LEFT JOIN meta_db.source_view_incremental SII
            ON SII.SOURCE_ID = S.SOURCE_ID
        WHERE s.source_code = '{object_name}'
        
        """
        
        df= ctx.spark.sql(sqlQuery)
        
        return df

    except Exception as e:
        errorStr = 'ERROR (getSourceAttributesByName): ' + str(e)
        print(errorStr)
        raise


# Generates Party cR Details
def getPartyDetails(ctx, batch_name, cr_type='KB'):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if batch_name is None:
            print('batch_name argument is required')
            raise ValueError('Invalid arguments')

        batch_name = str(batch_name) if batch_name is not None else None
        cr_type = str(cr_type) if cr_type is not None else None

        if cr_type is None:
            cr_source_code = 'KB_Match_Output'
        elif cr_type == 'KB':
            cr_source_code = 'KB_Match_Output'
        elif cr_type == 'EE':
            if batch_name == 'suppression_batch':
                cr_source_code = 'crcdi_ee_overlay_output'
            else:
                cr_source_code = 'crcdi_ee_client_output'
        else:
            cr_source_code = 'KB_Match_Output'

        sqlQuery = f"""
            SELECT df.file_id, df.batch_id
            FROM meta_db.data_file df
            JOIN meta_db.source s
                    ON df.object_id = s.source_id
            WHERE s.source_code = '{cr_source_code}'
                    AND df.batch_id = (
                            SELECT max(batch_id)
                            FROM meta_db.batch
                            WHERE batch_status = 'STARTED'
                            AND batch_name = '{batch_name}'
                                        )
        """

        result = ctx.spark.sql(sqlQuery)
        return result

    except Exception as e:
        errorStr = 'ERROR (getPartyDetails)' + str(e)
        print(errorStr)
        raise


# Generates the Stage Table name for a file_id
def getStageTableName(ctx, file_id):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if file_id is None:
            print('file_id argument is required')
            raise ValueError('Invalid arguments')

        file_id = str(file_id) if file_id is not None else None

        sqlQuery = f"""
        SELECT  UPPER(MAX(LOWER(c.stage_name) || '_' || 
                RIGHT('0000000000000' || CAST(a.file_id AS VARCHAR), 7))) as table_name 
        FROM    meta_db.data_file a
		JOIN	meta_db.source c
		  ON	a.object_id = c.source_id
        WHERE   a.file_id = '{file_id}'
        """
        
        df = ctx.spark.sql(sqlQuery)

        return df

    except Exception as e:
        errorStr = 'ERROR (getStageTableName)' + str(e)
        print(errorStr)
        raise


# Returns Source Feed Columns data
def getSourceFeedColumnsById(ctx, source_id):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if source_id is None:
            print('source_id argument is required')
            raise ValueError('Invalid arguments')

        source_id = str(source_id) if source_id is not None else None

        sqlQuery = f"""

        SELECT  column_id,
                ordinal_position,
                column_name, 
                data_type,
                max_length,
                scale,
                fixed_width_start_position,
                fixed_width_length, 
                column_property
        FROM    meta_db.source_feed_column
        WHERE   source_id = '{source_id}'
        ORDER BY ordinal_position, column_id        

        """

        
        feed_columns = ctx.spark.sql(sqlQuery)

        return feed_columns

    except Exception as e:
        errorStr = 'ERROR (getSourceFeedColumnsById)' + str(e)
        print(errorStr)
        raise


# Returns EXTRACT_CONFIG data
def getExtractAttributesByName(ctx, extract_name):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if extract_name is None:
            print('extract_name argument is required')
            raise ValueError('Invalid arguments')

        extract_name = str(extract_name) if extract_name is not None else None

        sqlQuery = f"""

        SELECT *
        FROM meta_db.extract_config
        WHERE extract_name = '{extract_name}'

        """

        
        extract = ctx.spark.sql(sqlQuery)

        return extract

    except Exception as e:
        errorStr = 'ERROR (getExtractAttributesByName)' + str(e)
        print(errorStr)
        raise


# Provider
def getProviderByName(ctx, provider_name):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if provider_name is None:
            print('provider_name argument is required')
            raise ValueError('Invalid arguments')
        provider_name = str(provider_name) if provider_name is not None else None

        sqlQuery = f"""

        SELECT *
        FROM meta_db.provider
        WHERE provider_name = '{provider_name}'

        """

        
        provider = ctx.spark.sql(sqlQuery)

        return provider

    except Exception as e:
        errorStr = 'ERROR (getProviderByName)' + str(e)
        print(errorStr)
        raise


def getProviderByID(ctx, provider_id):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if provider_id is None:
            print('provider_id argument is required')
            raise ValueError('Invalid arguments')
        provider_id = str(provider_id) if provider_id is not None else None

        sqlQuery = f"""

        SELECT *
        FROM meta_db.provider
        WHERE provider_id = '{provider_id}'

        """

        
        provider = ctx.spark.sql(sqlQuery)

        return provider

    except Exception as e:
        errorStr = 'ERROR (getProviderByID)' + str(e)
        print(errorStr)
        raise

# Receiver
def getReceiverByName(ctx, receiver_name):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if receiver_name is None:
            print('receiver_name argument is required')
            raise ValueError('Invalid arguments')
        receiver_name = str(receiver_name) if receiver_name is not None else None

        sqlQuery = f"""

        SELECT *
        FROM meta_db.receiver
        WHERE receiver_name = '{receiver_name}'

        """

        
        receiver = ctx.spark.sql(sqlQuery)

        return receiver

    except Exception as e:
        errorStr = 'ERROR (getReceiverByName)' + str(e)
        print(errorStr)
        raise


def getReceiverByID(ctx, receiver_id):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if receiver_id is None:
            print('receiver_id argument is required')
            raise ValueError('Invalid arguments')
        receiver_id = str(receiver_id) if receiver_id is not None else None

        sqlQuery = f"""

        SELECT *
        FROM meta_db.receiver
        WHERE receiver_id = '{receiver_id}'

        """

        
        receiver = ctx.spark.sql(sqlQuery)

        return receiver

    except Exception as e:
        errorStr = 'ERROR (getProviderByID)' + str(e)
        print(errorStr)
        raise



def getSourceRecordByObjectId(ctx, object_id):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if object_id is None:
            print('object_id argument is required')
            raise ValueError('Invalid arguments')
        object_id = str(object_id) if object_id is not None else None

        sqlQuery = f"""

            SELECT *
            FROM meta_db.source
            WHERE source_id = '{object_id}'

            """

        
        source = ctx.spark.sql(sqlQuery)

        return source

    except Exception as e:
        errorStr = 'ERROR (getSourceRecordByObjectId)' + str(e)
        print(errorStr)
        raise


def getSourceListForFiles(ctx):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')

        sqlQuery = f"""

            SELECT *
            FROM meta_db.source
            WHERE source_type = 'file'
              AND category <> 'CDI'

            """

        
        source = ctx.spark.sql(sqlQuery)

        return source

    except Exception as e:
        errorStr = 'ERROR (getSourceRecordByObjectId)' + str(e)
        print(errorStr)
        raise


def getTargetRecordByObjectId(ctx, object_id):
    try:

        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if object_id is None:
            print('object_id argument is required')
            raise ValueError('Invalid arguments')

        object_id = str(object_id) if object_id is not None else None

        sqlQuery = f"""

            SELECT *
            FROM meta_db.target
            WHERE target_id = '{object_id}'

            """

        
        target = ctx.spark.sql(sqlQuery)

        return target

    except Exception as e:
        errorStr = 'ERROR (getTargetRecordByObjectId)' + str(e)
        print(errorStr)
        raise


#Master Key Lookup
def getNodeListBySourceCode(ctx, source_code, isn_flag=None, cr_flag=None):
    try:
        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if source_code is None:
            print('source_code argument is required')
            raise ValueError('Invalid arguments')
        source_code = str(source_code) if source_code is not None else None
        isn_flag = str(isn_flag) if isn_flag is not None else None
        cr_flag = str(cr_flag) if cr_flag is not None else None

        if isn_flag == 'Y':
            isn_str = """ UNION ALL SELECT 'ISN', 'ISN' """
        else:
            isn_str = ''

        if cr_flag == 'Y':
            cr_str = """ UNION ALL SELECT 'cr_indiv_key', 'cr_indiv_key' """
        else:
            cr_str = ''

        sqlQuery = f"""

            SELECT
                a.column_name as left_col, 
                b.column_name as right_col,
                a.masterid_column_name as left_master,
                b.masterid_column_name as right_master
            FROM (
            SELECT CASE WHEN column_property = 'master_key_node' THEN 'masterID' ELSE column_name END as column_name,
                    column_name as masterid_column_name
                FROM meta_db.source_feed_column sfc
                JOIN meta_db.source s
                  ON sfc.source_id = s.source_id
                WHERE column_property in ('key_node','master_key_node')
                AND s.source_code = '""" + source_code + """'
                """ + cr_str + """
                """ + isn_str + """
                ) a
            JOIN (
            SELECT CASE WHEN column_property = 'master_key_node' THEN 'masterID' ELSE column_name END as column_name,
                    column_name as masterid_column_name
                FROM meta_db.source_feed_column sfc
                JOIN meta_db.source s
                  ON sfc.source_id = s.source_id
                WHERE column_property in ('key_node','master_key_node')
                AND s.source_code = '""" + source_code + """'
                """ + cr_str + """
                """ + isn_str + """
                ) b
            ON a.column_name < b.column_name
        """

        
        nodes = ctx.spark.sql(sqlQuery)

        return nodes

    except Exception as e:
        errorStr = 'ERROR (getTargetRecordByObjectId)' + str(e)
        print(errorStr)
        raise

# DataSource Append Lookup

def get_datasource_attrib_name_field_by_source_id(ctx, source_id):
    try:
        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if source_id is None:
            print('source_id argument is required')
            raise ValueError('Invalid arguments')
        source_id = str(source_id) if source_id is not None else None

        df = getSourceFeedColumnsById(ctx, source_id)
        result = df.loc[df.column_property == 'ds_attrib_name']

        if result.count() == 0:
            return None
        else:
            return result.first()['column_name']

    except Exception as e:
        errorStr = 'ERROR (get_datasource_attrib_name_field_by_source_id)' + str(e)
        print(errorStr)
        raise


def get_datasource_attrib_value_field_by_source_id(ctx, source_id):
    try:
        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if source_id is None:
            print('source_id argument is required')
            raise ValueError('Invalid arguments')
        source_id = str(source_id) if source_id is not None else None

        df = getSourceFeedColumnsById(ctx, source_id)
        result = df.loc[df.column_property == 'ds_attrib_value']

        if result.count() == 0:
            return None
        else:
            return result.first()['column_name']

    except Exception as e:
        errorStr = 'ERROR (get_datasource_attrib_value_field_by_source_id)' + str(e)
        print(errorStr)
        raise


def get_datasource_attrib_datatype_field_by_source_id(ctx, source_id):
    try:
        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if source_id is None:
            print('source_id argument is required')
            raise ValueError('Invalid arguments')
        source_id = str(source_id) if source_id is not None else None

        df = getSourceFeedColumnsById(ctx, source_id)
        result = df.loc[df.column_property == 'ds_attrib_datatype']

        if result.count() == 0:
            return None
        else:
            return result.first()['column_name']

    except Exception as e:
        errorStr = 'ERROR (get_datasource_attrib_datatype_field_by_source_id)' + str(e)
        print(errorStr)
        raise


def get_datasource_natural_keys_by_source_id(ctx, source_id):
    try:
        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if source_id is None:
            print('source_id argument is required')
            raise ValueError('Invalid arguments')
        source_id = str(source_id) if source_id is not None else None

        df = getSourceFeedColumnsById(ctx, source_id)
        result = df.loc[df.column_property == 'ds_natural_key']
        result = result[['column_name', 'data_type', 'max_length', 'scale']]
        result = result.reset_index(drop=True)

        if result.count() == 0:
            return None
        else:
            return result

    except Exception as e:
        errorStr = 'ERROR (get_datasource_natural_keys_by_source_id)' + str(e)
        print(errorStr)
        raise


def get_required_file_count_and_freq_by_batch_id(ctx, p_batch_id, p_excl_no_row_staged_file):
    """Query meta for number of files received by source
then compare that frequency to time passed.

    :param spark(dbconnection):        the sql alchemy database connction
                                            on which this will run

    :param p_batch_id(int):                   batch id that the query will run on

    :param p_excl_no_row_staged_file(string):    Flag indicating whether or not to include
                                            files containing 0 row counts as viable
                                            files contributing to the batch

    :return df(pandas df):           The returning values from the select query
                                            fired by the function
    """
    try:
        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if p_batch_id is None:
            print('p_batch_id argument is required')
            raise ValueError('Invalid arguments')
        if p_excl_no_row_staged_file is None:
            print('p_excl_no_row_staged_file arguement is required')
            raise ValueError('Invalid arguments')
        p_batch_id = str(p_batch_id) if p_batch_id is not None else None
        p_excl_no_row_staged_file = str(p_excl_no_row_staged_file) if p_excl_no_row_staged_file is not None else None

        if p_excl_no_row_staged_file.upper() == 'N':
            sqlQuery = '''SELECT * FROM
                  (select s.source_id
                  ,s.source_code
                  ,MAX(df.batch_id) as batch_last_receiving_file
                  ,COALESCE(batch_count.current_batch_file_count, 0) as current_batch_file_count
                  ,s.receipt_frequency
                  ,expected_number_of_files
                  ,do.required_ind
                  from meta_db.source s
                  INNER JOIN  meta_db.data_object do
                  ON do.object_id = s.source_id
                  LEFT JOIN meta_db.data_file df
                  ON df.object_id = do.object_id
                  LEFT JOIN (
                               select COALESCE(COUNT(df.file_id),0) as current_batch_file_count, s.source_id
                               FROM meta_db.data_file df
                               INNER JOIN meta_db.data_object do
                               ON df.object_id = do.object_id
                               INNER JOIN meta_db.source s
                               ON do.object_id = s.source_id
                               where df.batch_id = {0}
                               group by s.source_id
                  ) batch_count
                  ON s.source_id = batch_count.source_id
                  LEFT JOIN (
                               select s.source_id,
                               CASE
                               WHEN UPPER(s.receipt_frequency) = 'DAILY' THEN DATEDIFF(DAY,MAX(df.modified_date),CURRENT_TIMESTAMP())
                               WHEN UPPER(s.receipt_frequency) = 'WEEKLY' THEN DATEDIFF(WEEK, MAX(df.modified_date),CURRENT_TIMESTAMP())
                               WHEN UPPER(s.receipt_frequency) = 'MONTHLY' THEN DATEDIFF(MONTH, MAX(df.modified_date),CURRENT_TIMESTAMP())
                               WHEN UPPER(s.receipt_frequency) = 'QUARTERLY' THEN DATEDIFF(QUARTER, MAX(df.modified_date),CURRENT_TIMESTAMP())
                               END AS expected_number_of_files
                               from meta_db.data_file df
                               INNER JOIN meta_db.data_object do
                               ON df.object_id = do.object_id
                               INNER JOIN meta_db.source s
                               ON do.object_id = s.source_id
                               where df.batch_id <  {0}
                               group by s.source_id, s.receipt_frequency
                  ) nonbatch_count
                  ON s.source_id = nonbatch_count.source_id
                  WHERE LTRIM(RTRIM(NULLIF(s.receipt_frequency,''))) != ''
                  AND do.required_ind ='Y'
                  AND s.source_type like '%%file%%'
                  group by s.source_id, source_code, batch_count.current_batch_file_count, s.receipt_frequency, expected_number_of_files, do.required_ind
                  ) s1 WHERE current_batch_file_count < expected_number_of_files;
                  '''.format(p_batch_id)

        elif p_excl_no_row_staged_file.upper() == 'Y':
            sqlQuery = '''
                  SELECT * FROM
                (SELECT s.source_id
                ,s.source_code
                ,MAX(df.batch_id) AS batch_last_receiving_file
                ,COALESCE(batch_count.current_batch_file_count, 0) AS current_batch_file_count
                ,s.receipt_frequency
                ,expected_number_of_files
                ,do.required_ind
                FROM meta_db.source s
                INNER JOIN  meta_db.data_object do
                ON do.object_id = s.source_id
                LEFT JOIN meta_db.data_file df
                ON df.object_id = do.object_id
                LEFT JOIN (
                             SELECT COALESCE(COUNT(df.file_id),0) AS current_batch_file_count, s.source_id
                             FROM meta_db.data_file df
                             INNER JOIN meta_db.data_object do
                             ON df.object_id = do.object_id
                             INNER JOIN meta_db.source s
                             ON do.object_id = s.source_id
                             WHERE df.batch_id = {0}
                             AND COALESCE(df.stg_good_record_count,0) != 0
                             GROUP BY s.source_id
                ) batch_count
                ON s.source_id = batch_count.source_id
                LEFT JOIN (
                             SELECT s.source_id,
                             CASE
                             WHEN UPPER(s.receipt_frequency) = 'DAILY' THEN DATEDIFF(DAY,MAX(df.modified_date),CURRENT_TIMESTAMP())
                             WHEN UPPER(s.receipt_frequency) = 'WEEKLY' THEN DATEDIFF(WEEK, MAX(df.modified_date),CURRENT_TIMESTAMP())
                             WHEN UPPER(s.receipt_frequency) = 'MONTHLY' THEN DATEDIFF(MONTH, MAX(df.modified_date),CURRENT_TIMESTAMP())
                             WHEN UPPER(s.receipt_frequency) = 'QUARTERLY' THEN DATEDIFF(QUARTER, MAX(df.modified_date),CURRENT_TIMESTAMP())
                             END AS expected_number_of_files
                             from meta_db.data_file df
                             INNER JOIN meta_db.data_object do
                             ON df.object_id = do.object_id
                             INNER JOIN meta_db.source s
                             ON do.object_id = s.source_id
                             WHERE df.batch_id < {0}
                             AND  COALESCE(df.stg_good_record_count,0) != 0
                             GROUP BY s.source_id, s.receipt_frequency
                ) nonbatch_count
                ON s.source_id = nonbatch_count.source_id
                WHERE LTRIM(RTRIM(NULLIF(s.receipt_frequency,''))) != ''
                AND  COALESCE(df.stg_good_record_count,0) != 0
                AND do.required_ind ='Y'
                AND s.source_type like '%%file%%'
                GROUP BY s.source_id, source_code, batch_count.current_batch_file_count, s.receipt_frequency, expected_number_of_files, do.required_ind
                ) s1 WHERE current_batch_file_count < expected_number_of_files;

            '''.format(p_batch_id)

        else:
            print('''p_excl_no_row_staged_file contains invalid value, must equal 'Y' or 'N' ''')

        df = ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (get_required_file_count_and_freq_by_batch_id)' + str(e)
        print(errorStr)
        raise

def check_required_files_are_staged_by_batch_id(ctx, p_batch_id):
    """Query meta for required sources that have not files
staged for the batch.

    :param spark(dbconnection):        the sql alchemy database connction
                                            on which this will run

    :param p_batch_id(int):                   batch id that the query will run on

    :return df(pandas df):           The returning values from the select query
                                            fired by the function
    """
    try:
        if spark is None:
            print('spark argument is required')
            raise ValueError('Invalid arguments')
        if p_batch_id is None:
            print('p_batch_id argument is required')
            raise ValueError('Invalid arguments')
        p_batch_id = str(p_batch_id) if p_batch_id is not None else None

        sqlQuery = ''' SELECT a.batch_id, a.batch_name, a.source_id, a.source_code, a.required_ind, a.expected_count, a.existing_count
			FROM
			(
				SELECT  b.batch_id
					, b.batch_name
					, s.source_id
					,s.source_code
					,do.required_ind
					,1 as expected_count
					,COALESCE(ec.existing_count,0) as existing_count
				FROM    meta_db.data_object do
				JOIN    meta_db.source s
	  			ON    do.object_id = s.source_id
				JOIN    meta_db.batch b
	  			ON    b.batch_name = s.category
				LEFT JOIN (SELECT df.batch_id, df.object_id as source_id, CASE WHEN COALESCE(SUM(df.stg_good_record_count),0) > 0 THEN 1 ELSE 0 END AS existing_count 
				 		FROM meta_db.data_file df
				   		GROUP BY df.batch_id, df.object_id) ec
		   		ON ec.source_id = s.source_id
		   		AND ec.batch_id = b.batch_id
				WHERE   b.batch_id = {0}
	  			AND   do.required_ind = 'Y'
			) a
			WHERE a.expected_count > a.existing_count;
                  '''.format(p_batch_id)

        df = ctx.spark.sql(sqlQuery)
        return df

    except Exception as e:
        errorStr = 'ERROR (check_required_files_are_staged_by_batch_id)' + str(e)
        print(errorStr)
        raise
