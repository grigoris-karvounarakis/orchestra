/***********************************************************************\
 *                                                                     *
 * (c) Copyright IBM Corp. 2005 All rights reserved.                   *
 *                                                                     *
 * This sample program is owned by International Business Machines     *
 * Corporation or one of its subsidiaries ("IBM") and is copyrighted   *
 * and licensed, not sold.                                             *
 *                                                                     *
 * You may copy, modify, and distribute this sample program in any     *
 * form without payment to IBM,  for any purpose including developing, *
 * using, marketing or distributing programs that include or are       *
 * derivative works of the sample program.                             *
 *                                                                     *
 * The sample program is provided to you on an "AS IS" basis, without  *
 * warranty of any kind.  IBM HEREBY  EXPRESSLY DISCLAIMS ALL          *
 * WARRANTIES EITHER EXPRESS OR IMPLIED, INCLUDING, BUT NOT LIMITED TO *
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTIC- *
 * ULAR PURPOSE. Some jurisdictions do not allow for the exclusion or  *
 * limitation of implied warranties, so the above limitations or       *
 * exclusions may not apply to you.  IBM shall not be liable for any   *
 * damages you suffer as a result of using, modifying or distributing  *
 * the sample program or its derivatives.                              *
 *                                                                     *
 * Each copy of any portion of this sample program or any derivative   *
 * work,  must include a the above copyright notice and disclaimer of  *
 * warranty.                                                           *
 *                                                                     *
\***********************************************************************/

#include <sql.h>
#include <sqludf.h>
#include <db2ApiDf.h>

/** Stored procedure to wrap RUNSTATS command.
 *
 * This stored procedure takes the fully-qualified name of a table as input
 * parameter and performs the same operation like the DB2 command RUNSTATS
 * against this table.  The statistics are collected for all columns (with
 * distributions) and all indexes (with extended statistics).  Read access is
 * allowed while the operation is performed.  This, the functionality
 * implemented in the stored procedure is equivalent to the following command:
 *
 *<pre>
 *   RUNSTATS ON TABLE <table-name>
 *      ON ALL COLUMNS WITH DISTRIBUTION AND
 *      DETAILED INDEXES ALL
 *      ALLOW READ ACCESS
 *</pre>
 *
 * The procedure does not perform any action if the given table name is NULL.
 * If the <code>db2Runstats()</code> API encounters any error - for example,
 * the specified table does not exist or the user who called the procedure
 * does not have the necessary privileges - the error is returned to the
 * caller using SQLSTATE "38XXX".
 *
 * In order to use the procedure it must be registered in the database with
 * the following SQL statement:
 *<pre>
 *  CREATE PROCEDURE runstats(IN tableName VARCHAR(256))
 *     SPECIFIC runstats
 *     DYNAMIC RESULT SETS 0
 *     MODIFIES SQL DATA
 *     NOT DETERMINISTIC
 *     CALLED ON NULL INPUT
 *     LANGUAGE C
 *     EXTERNAL NAME 'db2_api!runstats_proc'
 *     FENCED  THREADSAFE
 *     NO EXTERNAL ACTION
 *     PARAMETER STYLE SQL;
 *</pre>
 *
 * Note: The options for the RUNSTATS could be parameterized by adding more
 * input parameters to the stored procedure.
 *
 * @param tableName name of the table agains which RUNSTATS is to be executed
 * @param tableName_ind NULL indicator for the parameter "tableName"
 */   
int SQL_API_FN runstats_proc(
	SQLUDF_VARCHAR *tableName,
	SQLUDF_NULLIND *tableName_ind,
	SQLUDF_TRAIL_ARGS)
{
    SQL_API_RC rc = SQL_RC_OK;
    db2RunstatsData data;
    struct sqlca sqlca;

    if (SQLUDF_NULL(tableName_ind)) {
	goto cleanup;
    }

    /* initialize data structures */
    data.iSamplingOption = 0;
    data.piTablename = (unsigned char *)tableName;
    data.piColumnList = NULL;
    data.piColumnDistributionList = NULL;
    data.piColumnGroupList = NULL;
    data.piIndexList = NULL;
    data.iRunstatsFlags = DB2RUNSTATS_ALL_COLUMNS | DB2RUNSTATS_DISTRIBUTION |
	DB2RUNSTATS_ALL_INDEXES | DB2RUNSTATS_EXT_INDEX |
	DB2RUNSTATS_ALLOW_READ;
    data.iNumColumns = 0;
    data.iNumColdist = 0;
    data.iNumColGroups = 0;
    data.iNumIndexes = 0;
    data.iParallelismOption = 0;
    data.iTableDefaultFreqValues = -1; /* use default */
    data.iTableDefaultQuantiles = -1; /* use default */
    data.iUtilImpactPriority = 0;
    data.iSamplingRepeatable = 0; /* unused */
    memset(&sqlca, 0x00, sizeof sqlca);

    /* call the API */
    rc = db2Runstats(db2Version820, &data, &sqlca);
    if (rc != SQL_RC_OK || SQLCODE != SQL_RC_OK) {
	memcpy(SQLUDF_STATE, "38RS1", SQLUDF_SQLSTATE_LEN);
	sprintf(SQLUDF_MSGTX, "Error %d returned by db2Runstats.",
		(int)(rc == SQL_RC_OK ? SQLCODE : rc));
	goto cleanup;
    }

 cleanup:
    return SQLZ_DISCONNECT_PROC;
}


/** Stored procedure to wrap LOAD command.
 *
 * This stored procedure takes the name of a file and a fully-qualified name
 * of a table as input parameter and loads the data from the file into the
 * table.  Thus, it performs the same operation like the DB2 command LOAD.
 *
 *<pre>
 *   LOAD FROM <file> OF <type>
 *      INSERT INTO <table>
 *      COPY NO
 *</pre>
 *
 * The procedure does not perform any action if the given table name or file
 * name is NULL.  If the <code>db2Load()</code> API encounters any error - for
 * example, the specified table does not exist or the user who called the
 * procedure does not have the necessary privileges - the error is returned to
 * the caller.
 *
 * In order to use the procedure it must be registered in the database with
 * the following SQL statement:
 *<pre>
 *  CREATE PROCEDURE load(IN fileName VARCHAR(256), IN fileType VARCHAR(3),
 *        IN tableName VARCHAR(256))
 *     SPECIFIC load
 *     DYNAMIC RESULT SETS 0
 *     MODIFIES SQL DATA
 *     NOT DETERMINISTIC
 *     CALLED ON NULL INPUT
 *     LANGUAGE C
 *     EXTERNAL NAME 'db2_api!load_proc'
 *     FENCED  THREADSAFE
 *     NO EXTERNAL ACTION
 *     PARAMETER STYLE SQL;
 *</pre>
 *
 * Note: The options for the LOAD could be parameterized by adding more input
 * parameters to the stored procedure.
 *
 * @param fileName name of the file (residing on the server) which is to be
 *                 loaded
 * @param fileType type of the file, either 'ASC', 'DEL', or 'IXF'
 * @param tableName name of the table agains which RUNSTATS is to be executed
 * @param fileName_ind NULL indicator for the parameter "fileName"
 * @param fileType_ind NULL indicator for the parameter "fileType"
 * @param tableName_ind NULL indicator for the parameter "tableName"
 */   
int SQL_API_FN load_proc(
	SQLUDF_VARCHAR *fileName,
	SQLUDF_VARCHAR *fileType,
	SQLUDF_VARCHAR *tableName,
	SQLUDF_NULLIND *fileName_ind,
	SQLUDF_NULLIND *fileType_ind,
	SQLUDF_NULLIND *tableName_ind,
	SQLUDF_TRAIL_ARGS)
{
    SQL_API_RC rc = SQL_RC_OK;
    db2LoadStruct data;
    struct sqlu_media_list fileSources;
    struct sqlu_location_entry fileLocation;
    struct sqlca sqlca;

    if (SQLUDF_NULL(fileName_ind) || SQLUDF_NULL(fileType_ind) ||
	    SQLUDF_NULL(tableName_ind)) {
	memcpy(SQLUDF_STATE, "38LD1", SQLUDF_SQLSTATE_LEN);
	memcpy(SQLUDF_MSGTX, "No file, file type, or table name specified.",
		SQLUDF_MSGTEXT_LEN);
	goto cleanup;
    }

    /*
     * initialize data structures
     */

    /* setup sources */
    data.piSourceList = &fileSources;
    fileSources.media_type = SQLU_SERVER_LOCATION;
    fileSources.sessions = 1;
    fileSources.target.location = &fileLocation;
    if (strlen(fileName) > SQLU_MEDIA_LOCATION_LEN) {
	memcpy(SQLUDF_STATE, "38LD2", SQLUDF_SQLSTATE_LEN);
	memcpy(SQLUDF_MSGTX, "The file name is too long.",
		SQLUDF_MSGTEXT_LEN);
	goto cleanup;
    }
    fileLocation.reserve_len = strlen(fileName);
    memcpy(fileLocation.location_entry, fileName,
	    fileLocation.reserve_len);

    /* no lob support */
    data.piLobPathList = NULL;

    /* load all columns */
    data.piDataDescriptor = NULL;

    /* action: INSERT */
    {
	struct sqlchar *action = (struct sqlchar *)malloc(
	    sizeof(struct sqlchar) + 12 + strlen(tableName));
	if (!action) {
	    memcpy(SQLUDF_STATE, "38LD3", SQLUDF_SQLSTATE_LEN);
	    memcpy(SQLUDF_MSGTX, "Memory allocation failed.",
		   SQLUDF_MSGTEXT_LEN);
	    goto cleanup;
	}
	action->length = sprintf(action->data, "INSERT INTO %s", tableName);
	data.piActionString = action;
    }

    /* file type is provided by the caller */
    data.piFileType = fileType;

    /* MODIFIED BY clause not supported */
    data.piFileTypeMod = NULL;

    /* all messages are discarded */
#if defined(SQLWINT)
    data.piLocalMsgFileName = "NUL";
#else /* SQLWINT */
    data.piLocalMsgFileName = "/home/stolze/msg";
#endif /* SQLWINT */

    /* system defaults are used for the remaining parameters */
    data.piTempFilesPath = NULL;
    data.piVendorSortWorkPaths = NULL;
    data.piCopyTargetList = NULL; /* no copies are created */
    data.piNullIndicators = NULL; /* no null indicators for ASC files */
    data.piLoadInfoIn = NULL;
    data.poLoadInfoOut = NULL;
    data.piPartLoadInfoIn = NULL;
    data.poPartLoadInfoOut = NULL;

   /* start the LOAD process */
   data.iCallerAction = SQLU_INITIAL;

    /* call the API */
    rc = db2Load(db2Version820, &data, &sqlca);
    if (rc != SQL_RC_OK || SQLCODE != SQL_RC_OK) {
	memcpy(SQLUDF_STATE, "38LD4", SQLUDF_SQLSTATE_LEN);
	sprintf(SQLUDF_MSGTX, "Error %d returned by db2Runstats.",
		(int)(rc == SQL_RC_OK ? SQLCODE : rc));
	goto cleanup;
    }

 cleanup:
    if (data.piActionString != NULL) {
	free(data.piActionString);
    }
    return SQLZ_DISCONNECT_PROC;
}

