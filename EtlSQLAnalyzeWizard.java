package expr.sql;

import static expr.sql.SQLWidget.COLUMN_REF;
import static expr.sql.SQLWidget.FUNC_EXPR;
import static expr.sql.SQLWidget.CASE_EXPR;
import static expr.sql.SQLWidget.PARENTH_EXPR;
import static expr.sql.SQLWidget.EXPR;
import static expr.sql.SQLWidget.SELECT_STMT_COLUMN;
import static expr.sql.SQLWidget.EXISTS_CONDITION_COLUMN;
import static expr.sql.SQLWidget.IN_SELECT_COLUMN;


import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;

import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;
import java.io.BufferedInputStream;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.BufferedOutputStream;


import static expr.sql.SQLWidget.SELECT_GRP_NAME;
import static expr.sql.SQLWidget.WHERE_GRP_NAME;
import static expr.sql.SQLWidget.GROUP_BY_GRP_NAME;
import static expr.sql.SQLWidget.HAVING_GRP_NAME;
import static expr.sql.SQLWidget.ORDER_BY_GRP_NAME;
import static expr.sql.SQLWidget.FUNC_ANALYTIC_CLAUSE_SUBGRP_NAME;
import static expr.sql.SQLWidget.EXPR_SUBGRP_NAME;
import static expr.sql.SQLWidget.CASE_EXPR_SUBGRP_NAME;
import static expr.sql.SQLWidget.FUNC_EXPR_SUBGRP_NAME;
import static expr.sql.SQLWidget.PARENTH_EXPR_SUBGRP_NAME;
import static expr.sql.SQLWidget.JOIN_GRP_NAME;
import static expr.sql.SQLWidget.FROM_GRP_NAME;
import static expr.sql.SQLWidget.FROM_TABLE_GROUP;

import expr.sql.SQLWidget.SQLColumn;
import expr.sql.SQLWidget.SQLTableRef;
import expr.sql.SQLWidget.SQLStatement;


import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static java.util.logging.Level.SEVERE;
import static java.util.logging.Level.WARNING;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.CONFIG;

import static expr.sql.SQLAnalyzeLogRecord.SQL_QUERY_FIELD_NAME;
import static expr.sql.SQLAnalyzeLogRecord.WHEN_FIELD_NAME;
import static expr.sql.SQLAnalyzeLogRecord.MORE_INFO_FIELD_NAME;
import static expr.sql.SQLAnalyzeLogRecord.GET_SQL_QUERIES_QRY_FIELD_NAME;
import static expr.sql.SQLAnalyzeLogRecord.THROWN_ERROR_FIELD_NAME;
import static expr.sql.SQLAnalyzeLogRecord.TAG_FIELD_NAME;
import static expr.sql.SQLAnalyzeLogRecord.NUMBER_OF_STMTS_FIELD_NAME;
import static expr.sql.SQLAnalyzeLogRecord.NUMBER_OF_STMTS_IN_ERR_FIELD_NAME;

/**
* Wizard class.<br>
* Instances of this class serve to retrieve SELECT SQL statements from Etl widgets, analyze those SELECT SQL statements, and produce three files: <br>
* <ol>
* <li>WC_ETL_DATA_TABLE_USE
* <li>WC_ETL_SQL_STMT
* <li>WC_ETL_STMT_USED_TAB_COL
* </ol>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public class EtlSQLAnalyzeWizard {
	
	static final java.lang.String[] EMPTY_STR_ARRAY = new java.lang.String[0];
	
	/*
	static final String INSERT_STMT_FOR_SQL_STMT = "INSERT WC_ETL_SQL_STMT (ROW_WID, OWNER_WIDGET_WID, STMT_NAME, WITH_STMT_FLG, WITH_STMT_NUMBER, NESTED_TABLE_NAME, SET_OPERATION_TYPE, " + 
			"SET_STMT_ITEM_NUMBER, ROOT_PARENT_SQL_STMT_WID, PARENT_SQL_STMT_WID, PARENT_SET_SQL_STMT_WID, DESCRIPTION, TAG_WID, CREATED_DT, " + 
			"CREATED_DT_WID, LAST_UPD_DT, LAST_UPD_DT_WID) VALUES (" + 
			"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? " + 
			")";
			
	static final String INSERT_STMT_FOR_USED_TABL_COLS = "INSERT INTO WC_ETL_STMT_USED_TAB_COL (" + 
			"ROW_WID, OWNER_WIDGET_WID, DATA_TABLE_WID, TABLE_USG_WID, SQL_STMT_WID, ROOT_PARENT_SQL_STMT_WID, " + 
			"PARENT_SQL_STMT_WID, OUTPUT_NUMBER, LOCAL_OUTPUT_NUMBER, DATA_COLUMN_WID, SOURCE_USED_TAB_COL_WID, " + 
			"COLUMN_ALIAS, CALCULATION_TYPE, LITERAL_TYPE, IS_LOGICAL_COL, PARENT_USED_TAB_COL_WID, 	SEQUENCE_NUM, " + 
			"ORDER_BY_COLS_OFFSET, COL_GROUP, CREATED_DT, CREATED_DT_WID, LAST_UPD_DT, LAST_UPD_DT_WID, TAG_WID" + 
	")" + 
	"VALUES " +  
	"(" + 
	"?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ? " + 
	")"; */

	public static class EtlSQLWidget implements java.io.Serializable, Comparable<EtlSQLWidget> {
		
		/**The class's serial version UID. */
		public static final long serialVersionUID = 0x0204FE53C1000000L;
		
			long ROW_WID; // NUMBER; //(10) DEFAULT; // 0; // NOT; // NULL; //, 
			String FOLDER; // VARCHAR2; //(150 CHAR; //) NOT; // NULL; //, 
			String WIDGET_NAME; // VARCHAR2; //(150 CHAR; //) NOT; // NULL; //, -- mapping; // name; //, mapplet; // name; // or; // reusable; // lkp; // name; //
			short WIDGET_TYPE; // VARCHAR2; //(50) NOT; // NULL; //, --'MAPPING', 'MAPPLET', 'REUSABLE_LOOKUP', ...
			String TOP_WIDGET_NAME; // VARCHAR2; //(150 CHAR; //), -- parent; // mapping; // name; // for; // nested; // widget; // of; // kind; // mapplet; // or; // same; // as; // WIDGET_NAME; //
			long PR_TARGET_TABLE_WID; // NUMBER; //(10) DEFAULT; // 0; // NOT; // NULL; //, --WID of; // the; // primary; // table; // loaded; // by; // this; // etl; // widget; //
			//--PR_TARGET_TABLE_NAME VARCHAR2; //(50 CHAR; //) DEFAULT; // 'Unspecified' NOT; // NULL; //, --simple name; // of; // the; // primary; // table; // loaded; // by; // this; // etl; // widget; // 
			long SCND_TARGET_TABLE_WID; // NUMBER; //(10) DEFAULT; // 0; // NOT; // NULL; //, --WID of; // the; // secondary; // table; // loaded; // by; // this; // etl; // widget; //, if; // any; //
			String DESCRIPTION; // VARCHAR2; //(200 CHAR; //), 
			long CREATED_DT; // DATE; // DEFAULT; // SYSDATE; // NOT; // NULL; //, 
			//CREATED_DT_WID; // NUMBER; //(10) TO_NUMBER; //(DEFAULT TO_CHAR; //(SYSDATE, 'YYYYMMDD')), 
			long LAST_UPD_DT; // DEFAULT; // SYSDATE; // NOT; // NULL; //,  
			//LAST_UPD_DT_WID; // NUMBER; //(10) TO_NUMBER; //(DEFAULT TO_CHAR; //(SYSDATE, 'YYYYMMDD')), 
			String CREATION_TAG; // VARCHAR2; //(150) --every refresh; // will; // lead; // to; // entries; // associated; // with; // the; // tag; // of; // that; // refresh; //
		
		
			EtlSQLWidget(String FOLDER, String WIDGET_NAME, short WIDGET_TYPE) {
				this.FOLDER = FOLDER;
				this.WIDGET_NAME = WIDGET_NAME;
				this.WIDGET_TYPE = WIDGET_TYPE;
			}
		
		
			public int compareTo(EtlSQLWidget other) {
				if (WIDGET_TYPE != other.WIDGET_TYPE) {
					return WIDGET_TYPE < other.WIDGET_TYPE ? -1 : 1;
				}
				int cmp = WIDGET_NAME.compareTo(other.WIDGET_NAME);
				if (cmp != 0) {
					return cmp;
				}
				return FOLDER.compareTo(other.FOLDER);
			}
			
			public boolean equals(EtlSQLWidget other) {
				return WIDGET_TYPE == other.WIDGET_TYPE && WIDGET_NAME.equals(other.WIDGET_NAME) && FOLDER.equals(other.FOLDER); 
			}
			
			public boolean equals(java.lang.Object other) {
				return other instanceof EtlSQLWidget && equals(((EtlSQLWidget)other));
			}
		
		
	}
	
	static class DBTableUse implements Comparable<DBTableUse>, java.io.Serializable {

		/**The class's serial version UID. */
		public static final long serialVersionUID = 0x02051C3901B6928AL;
	
		String dbOwner;
		String tableName;
		String tableUseName;
		
		long etlRowWid;
		
		DBTableUse(String dbOwner, String tableName, String tableUseName) {
			this.dbOwner = dbOwner;
			this.tableName = tableName;
			this.tableUseName = tableUseName;
			this.etlRowWid = -1;
		}
		
		public int compareTo(DBTableUse other) {
			int cmp = dbOwner.compareToIgnoreCase(other.dbOwner);
			if (cmp != 0) return cmp;
			cmp = tableName.compareToIgnoreCase(other.tableName);
			if (cmp != 0) return cmp;
			return tableUseName.compareToIgnoreCase(other.tableUseName);
		}
		
		public boolean equals(DBTableUse other) {
			return dbOwner.equalsIgnoreCase(other.dbOwner) && tableName.equalsIgnoreCase(other.tableName) && tableUseName.equalsIgnoreCase(other.tableUseName);
		}
		
		public boolean equals(java.lang.Object other) {
			return other instanceof DBTableUse && equals((DBTableUse)other);
		}
		
		
	}
	
	static final String UTF8_ENCODING = "UTF-8";

	DBTable[] dbTablesSmallList;
	int dbTablesCount;
	java.util.Map<DBTable, DBTable> dbTablesSet;
	
	EtlSQLWidget[] etlWidgetsSmallList;
	int etlWidgetsCount;
	java.util.Map<EtlSQLWidget, EtlSQLWidget> etlWidgetsSet;
	
	DBTableUse[] dbTableUsesSmallList;
	int dbTableUsesCount;
	java.util.Map<DBTableUse, DBTableUse> dbTableUsesSet;
	
	SQLParameterTableRef[] paramTblRefsSmallList;
	int paramTblRefsCount;
	java.util.Map<SQLParameterTableRef, SQLParameterTableRef> paramTblRefsSet;
	
	
	SQLSequenceValueRef[] seqValRefsSmallList;
	int seqValRefsCount;
	java.util.Map<SQLSequenceValueRef, SQLSequenceValueRef> seqValRefsSet;
	
	static final java.text.SimpleDateFormat DATE_TIME_FORMAT = new java.text.SimpleDateFormat("yyyyMMddHHmmss");
	
	long stmtNumberSeq;
	long usedTabColNumberSeq;
	int tableUseNumberSeq;
	transient String createdDt;
	
	EtlSQLWizardConfig config;
	String tag;
	Connection INFAREP_DBCONN;
	Connection BAW_DBCONN;
	
	String startDt;
	String tagFingerprint;
	long tagWid;
	
	transient SQLWithStmtRef[] withStmtRefs = null;
	transient int withStmtRefsCount = 0;
	
	transient final SQLPhysicalColumnSource physicalColumnSource;
	
	final FileHandler loggingHandler;
	final FileHandler sqlLoggingHandler;
	final SQLAnalyzeLogRecord logRecord;
	final SQLAnalyzeLogFormatter logFormatter;
	final Logger logger;
	final Logger sqlLogger;

	/**
	* Constructor.
	*/
	public EtlSQLAnalyzeWizard(EtlSQLWizardConfig config) {
		this.dbTablesCount = 0;
		this.etlWidgetsCount = 0;
		this.stmtNumberSeq = 0;
		this.usedTabColNumberSeq = 0;
		this.tableUseNumberSeq = 0;
		this.dbTableUsesCount = 0;
		this.paramTblRefsCount = 0;
		this.seqValRefsCount = 0;
		this.config = config != null ? config : EtlSQLWizardConfig.get();
		this.physicalColumnSource =  new SQLPhysicalColumnSource();
		
		java.lang.String filePath = EtlSQLWizardConfig.INFA_SQL_WIZARD_LOG_DIR + EtlSQLAnalyzeWizard.class.getSimpleName();
		try 
		{
			this.loggingHandler = new FileHandler(filePath + ".log", true/*append*/);
			this.loggingHandler.setEncoding(UTF8_ENCODING);
			this.sqlLoggingHandler = new FileHandler(filePath + "-sql.log", true/*append*/);
			this.sqlLoggingHandler.setEncoding(UTF8_ENCODING);
		}
		catch(java.io.IOException ioe)
		{
			throw new RuntimeException(ioe.getMessage() + ISQLWidgetConstants.LN_TERMINATOR + 
			"EtlSQLAnalyzeWizard::EtlSQLAnalyzeWizard-1: error while trying to open log files for write"
			, ioe 
			);
		}
		this.logRecord = new SQLAnalyzeLogRecord(ISQLWidgetConstants.EMPTY_STR);
		this.logFormatter = new SQLAnalyzeLogFormatter();
		this.logger = Logger.getLogger(filePath);
		Handler[] handlers = this.logger.getHandlers();
		for (int i=0;i<handlers.length;i++)
		{
			this.logger.removeHandler(handlers[i]);
		}
		this.logger.setLevel(Level.INFO);
		this.logger.addHandler(this.loggingHandler);
		this.loggingHandler.setFormatter(this.logFormatter);
		
		this.sqlLogger = Logger.getLogger(filePath + ".sql");
		handlers = this.sqlLogger.getHandlers();
		for (int i=0;i<handlers.length;i++)
		{
			this.sqlLogger.removeHandler(handlers[i]);
		}
		this.sqlLogger.setLevel(Level.INFO);
		this.sqlLogger.addHandler(this.sqlLoggingHandler);
		this.sqlLoggingHandler.setFormatter(this.logFormatter);
		
		//***********
		//NOTE: to double-check that sqlLoggingHandler is in the list of handlers - as i don't understand why attempting to log sql ends up with sql logged twice (in the normal log file and sql log file) .<br>
		//***
		handlers = this.logger.getHandlers();
		for (int i=0;i<handlers.length;i++)
		{
			Handler handler = handlers[i];
			if (handler == this.sqlLoggingHandler) {
				this.logger.removeHandler(handler);
			}
		}
		handlers = this.sqlLogger.getHandlers();
		for (int i=0;i<handlers.length;i++)
		{
			Handler handler = handlers[i];
			if (handler == this.loggingHandler) {
				this.sqlLogger.removeHandler(handler);
			}
		}
		
		this.tagFingerprint = ISQLWidgetConstants.EMPTY_STR;
	}
	/**
	* Default constructor.
	*/
	public EtlSQLAnalyzeWizard() {
		this(null);
	}
	
	
	private final void doLogTrace(String msg, Level level) {
		logFormatter.tabbedTwiceMultiline = false;
		logRecord.set(level, msg, true/*useNowAsEventTime*/);
		logRecord.setParameters(EMPTY_STR_ARRAY);
		logFormatter.eventTime = logRecord.eventTime;
		logger.log(logRecord);
	}
	private final void doLogTrace(String msg, Level level, String... fields) {
		logFormatter.tabbedTwiceMultiline = false;
		logRecord.set(level, msg, true/*useNowAsEventTime*/);
		logRecord.setParameters(fields);
		logFormatter.eventTime = logRecord.eventTime;
		logger.log(logRecord);
	}
	private final void doLogSQLTrace(String msg, Level level) {
		logFormatter.tabbedTwiceMultiline = false;
		logRecord.set(level, msg, true/*useNowAsEventTime*/);
		logRecord.setParameters(EMPTY_STR_ARRAY);
		logFormatter.eventTime = logRecord.eventTime;
		sqlLogger.log(logRecord);
	}
	private final void doLogSQLTrace(String msg, Level level, String... fields) {
		logFormatter.tabbedTwiceMultiline = false;
		logRecord.set(level, msg, true/*useNowAsEventTime*/);
		logRecord.setParameters(fields);
		logFormatter.eventTime = logRecord.eventTime;
		sqlLogger.log(logRecord);
	}
	
	
	private final SQLWithStmtRef __getCurSelectWithStmtRef(String withStmtName) {
		for (int i=0;i<withStmtRefsCount;i++)
		{
			if (withStmtRefs[i].withStmt.name.equalsIgnoreCase(withStmtName)) {
				return withStmtRefs[i];
			}
		}
		return null;
	}
	
	private final SQLWithStmtRef __putCurSelectWithStmtRefIfMissing(SQLWithStmtRef withStmtRef, final boolean merelyAdd) {
		if (withStmtRefsCount == 0) {
			if (withStmtRefs == null || withStmtRefs.length == 0) {
				withStmtRefs = new SQLWithStmtRef[5];
			}
			withStmtRefs[0] = withStmtRef;
			withStmtRefsCount = 1;
			return null;
		}
		else if (!merelyAdd) {
			for (int i=0;i<withStmtRefsCount;i++)
			{
				if (withStmtRefs[i].withStmt.name.equalsIgnoreCase(withStmtRef.withStmt.name)) {
					return withStmtRefs[i];
				}
			}
		}
		if (withStmtRefsCount >= withStmtRefs.length) {
			int newLen = withStmtRefs.length + (withStmtRefs.length >>> 1);
			SQLWithStmtRef[] arr = new SQLWithStmtRef[newLen <= withStmtRefsCount ? withStmtRefsCount + 1 : newLen];
			System.arraycopy(withStmtRefs, 0, arr, 0, withStmtRefsCount);
			withStmtRefs = arr;
		}
		withStmtRefs[withStmtRefsCount++] = withStmtRef;
		return null;
	}
	
	protected final Connection getINFAREP_DBCONN() {
		if (INFAREP_DBCONN != null) return INFAREP_DBCONN;
		try 
		{
			return (INFAREP_DBCONN = config.getINFARepConnection()); 
		}
		catch(java.sql.SQLException ex)
		{
			throw new RuntimeException(
			"EtlSQLAnalyzeWizard::getINFAREP_DBCONN-1: error while trying to get the DB connection for INFA repository"
			, ex
			);
		}
	}
	protected final Connection getBAW_DBCONN() {
		if (BAW_DBCONN != null) return BAW_DBCONN;
		try 
		{
			return (BAW_DBCONN = config.getBAWConnection()); 
		}
		catch(java.sql.SQLException ex)
		{
			throw new RuntimeException(
			"EtlSQLAnalyzeWizard::getBAW_DBCONN-1: error while trying to get the DB connection for BAW database"
			, ex
			);
		}
	}
	
	DBTableUse __getDBTableUse(String dbOwner, String tableName, String tableUseName) {
		DBTableUse tblUse = new DBTableUse(dbOwner, tableName, tableUseName);
		if (dbTableUsesCount < 65) {
			for (int i=0;i<dbTableUsesCount;i++)
			{
				if (dbTableUsesSmallList[i].equals(tblUse)) return dbTableUsesSmallList[i];
			}
			if (dbTableUsesCount < 64) {
				if (dbTableUsesCount == 0) {
					dbTableUsesSmallList = new DBTableUse[6];
					dbTableUsesSmallList[0] = tblUse;
					dbTableUsesCount = 1;
					return tblUse;
				}
				else if (dbTableUsesCount >= dbTableUsesSmallList.length) {
					int newLen = dbTableUsesSmallList.length + (dbTableUsesSmallList.length >>> 1);
					if (newLen > 64) {
						newLen = 64;
					}
					DBTableUse[] arr = new DBTableUse[newLen <= dbTableUsesCount ? dbTableUsesCount + 1 : newLen];
					System.arraycopy(dbTableUsesSmallList, 0, arr, 0, dbTableUsesCount);
					dbTableUsesSmallList = arr;
				}
				dbTableUsesSmallList[dbTableUsesCount++] = tblUse;
				return tblUse;
			}
			else {
				dbTableUsesSet = new java.util.TreeMap<DBTableUse, DBTableUse>();
			}
		}
		else {
			DBTableUse existing = dbTableUsesSet.get(tblUse);
			if (existing != null) return existing;
		}
		dbTableUsesSet.put(tblUse, tblUse);
		return tblUse;
	}
	
	
	SQLParameterTableRef __getParameterTableRef(SQLParameterTableRef paramTblRef) {
		if (paramTblRefsCount < 65) {
			for (int i=0;i<paramTblRefsCount;i++)
			{
				if (paramTblRefsSmallList[i].equals(paramTblRef)) return paramTblRefsSmallList[i];
			}
			if (paramTblRefsCount < 64) {
				if (paramTblRefsCount == 0) {
					paramTblRefsSmallList = new SQLParameterTableRef[6];
					paramTblRefsSmallList[0] = paramTblRef;
					paramTblRefsCount = 1;
					return paramTblRef;
				}
				else if (paramTblRefsCount >= paramTblRefsSmallList.length) {
					int newLen = paramTblRefsSmallList.length + (paramTblRefsSmallList.length >>> 1);
					if (newLen > 64) {
						newLen = 64;
					}
					SQLParameterTableRef[] arr = new SQLParameterTableRef[newLen <= paramTblRefsCount ? paramTblRefsCount + 1 : newLen];
					System.arraycopy(paramTblRefsSmallList, 0, arr, 0, paramTblRefsCount);
					paramTblRefsSmallList = arr;
				}
				paramTblRefsSmallList[paramTblRefsCount++] = paramTblRef;
				return paramTblRef;
			}
			else {
				paramTblRefsSet = new java.util.TreeMap<SQLParameterTableRef, SQLParameterTableRef>();
			}
		}
		else {
			SQLParameterTableRef existing = paramTblRefsSet.get(paramTblRef);
			if (existing != null) return existing;
		}
		paramTblRefsSet.put(paramTblRef, paramTblRef);
		return paramTblRef;
	}
	
	
	SQLSequenceValueRef __getSequenceValueRef(SQLSequenceValueRef seqValRef) {
		if (seqValRefsCount < 65) {
			for (int i=0;i<seqValRefsCount;i++)
			{
				if (seqValRefsSmallList[i].equals(seqValRef)) return seqValRefsSmallList[i];
			}
			if (seqValRefsCount < 64) {
				if (seqValRefsCount == 0) {
					seqValRefsSmallList = new SQLSequenceValueRef[6];
					seqValRefsSmallList[0] = seqValRef;
					seqValRefsCount = 1;
					return seqValRef;
				}
				else if (seqValRefsCount >= seqValRefsSmallList.length) {
					int newLen = seqValRefsSmallList.length + (seqValRefsSmallList.length >>> 1);
					if (newLen > 64) {
						newLen = 64;
					}
					SQLSequenceValueRef[] arr = new SQLSequenceValueRef[newLen <= seqValRefsCount ? seqValRefsCount + 1 : newLen];
					System.arraycopy(seqValRefsSmallList, 0, arr, 0, seqValRefsCount);
					seqValRefsSmallList = arr;
				}
				seqValRefsSmallList[seqValRefsCount++] = seqValRef;
				return seqValRef;
			}
			else {
				seqValRefsSet = new java.util.TreeMap<SQLSequenceValueRef, SQLSequenceValueRef>();
			}
		}
		else {
			SQLSequenceValueRef existing = seqValRefsSet.get(seqValRef);
			if (existing != null) return existing;
		}
		seqValRefsSet.put(seqValRef, seqValRef);
		return seqValRef;
	}
	
	private static final void writeWC_ETL_SQL_ANALYZE_ERRHeader(OutputStream output) throws java.io.IOException {
		output.write("TAG|".getBytes(UTF8_ENCODING));
		output.write("FOLDER|".getBytes(UTF8_ENCODING));
		output.write("WIDGET_NAME|".getBytes(UTF8_ENCODING));
		output.write("WIDGET_TYPE|".getBytes(UTF8_ENCODING));
		output.write("PARENT_WIDGET_NAME|".getBytes(UTF8_ENCODING));
		output.write("TOP_WIDGET_NAME|".getBytes(UTF8_ENCODING));
		output.write("MSG|".getBytes(UTF8_ENCODING));
		output.write("CREATED_DT\n".getBytes(UTF8_ENCODING));
	}
	
	private static final void writeWC_ETL_DATA_TABLE_USEHeader(OutputStream output) throws java.io.IOException {
		output.write("DB_OWNER|".getBytes(UTF8_ENCODING));
		output.write("DB_TABLE|".getBytes(UTF8_ENCODING));
		output.write("TABLE_USE_NAME|".getBytes(UTF8_ENCODING));
		output.write("DESCRIPTION|".getBytes(UTF8_ENCODING));
		output.write("TAG\n".getBytes(UTF8_ENCODING));
	}
	
	private static final void writeDbTableUse(String dbOwner, String tableName, String tableUseName, String tag, OutputStream output) throws java.io.IOException {
		output.write((dbOwner + "|").getBytes(UTF8_ENCODING)); //DB_OWNER
		output.write((tableName + "|").getBytes(UTF8_ENCODING)); //DB_TABLE
		output.write((tableUseName + "|").getBytes(UTF8_ENCODING)); //TABLE_USE_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //DESCRIPTION
		output.write((tag + "\n").getBytes(UTF8_ENCODING)); //TAG
	}
	
	private static final void writeDbTableUse(DBTableUse tblUse, String tag, OutputStream output) throws java.io.IOException {
		writeDbTableUse(tblUse.dbOwner, tblUse.tableName, tblUse.tableUseName, tag, output);
	}
	
	private static final void writeWC_ETL_SQL_STMTHeader(OutputStream output) throws java.io.IOException {
		output.write("FOLDER|".getBytes(UTF8_ENCODING));
		output.write("OWNER_WIDGET_NAME|".getBytes(UTF8_ENCODING));
		output.write("OWNER_WIDGET_TYPE|".getBytes(UTF8_ENCODING));
		output.write("PARENT_WIDGET_NAME|".getBytes(UTF8_ENCODING)); //new
		output.write("TOP_WIDGET_NAME|".getBytes(UTF8_ENCODING)); //new
		output.write("STMT_NUMBER|".getBytes(UTF8_ENCODING));
		output.write("STMT_NAME|".getBytes(UTF8_ENCODING));
		output.write("WITH_STMT_FLG|".getBytes(UTF8_ENCODING));
		output.write("WITH_STMT_NUMBER|".getBytes(UTF8_ENCODING));
		output.write("NESTED_TABLE_NAME|".getBytes(UTF8_ENCODING));
		output.write("SET_OPERATION_TYPE|".getBytes(UTF8_ENCODING));
		output.write("SET_STMT_ITEM_NUMBER|".getBytes(UTF8_ENCODING));
		output.write("PARENT_SQL_STMT_NUMBER|".getBytes(UTF8_ENCODING));
		output.write("ROOT_PARENT_SQL_STMT_NUM|".getBytes(UTF8_ENCODING));
		output.write("PARENT_SET_SQL_STMT_NUM|".getBytes(UTF8_ENCODING));
		output.write("PARAM_TBL_USE_NAME|".getBytes(UTF8_ENCODING));
		output.write("DESCRIPTION|".getBytes(UTF8_ENCODING));
		output.write("DISTINCT_FLG|".getBytes(UTF8_ENCODING));
		output.write("OUTPUT_COLUMNS|".getBytes(UTF8_ENCODING));
		output.write("WITH_INFA_JOINS_FLG|".getBytes(UTF8_ENCODING));
		output.write("LIMIT_VALUE|".getBytes(UTF8_ENCODING));
		output.write("LIMIT_OFFSET|".getBytes(UTF8_ENCODING));
		output.write("TOP_N|".getBytes(UTF8_ENCODING));
		output.write("TOP_N_PERCENT_FLG|".getBytes(UTF8_ENCODING));
		output.write("HINT_TEXT|".getBytes(UTF8_ENCODING));
		output.write("TAG|".getBytes(UTF8_ENCODING));
		output.write("CREATED_DT|".getBytes(UTF8_ENCODING));
		output.write("LAST_UPD_DT\n".getBytes(UTF8_ENCODING));
	}
	
	static final String toDateTimeString(java.util.Date dtm, java.lang.StringBuffer buffer) {
		//buffer.setLength(0);
		return DATE_TIME_FORMAT.format(dtm/*, buffer, null*/);
		//return buffer.toString();
	}
	static final String toDateTimeString(java.util.Date dtm) {
		return DATE_TIME_FORMAT.format(dtm);
	}
	
	private static SQLSelectStmt __getWithStatementFor(SQLTableRef fromTbl, SQLSelectStmt of) {
		if (fromTbl.isSQLStmtTable()) {
			return of.getWithStmt(fromTbl.asSQLStmtTable().name);
		}
		else if (fromTbl.isSQLStmtTableAlias()) {
			return of.getWithStmt(fromTbl.asSQLStmtTableAlias().of.name);
		}
		return null;
	}
	
	static final long SELF_PARENT_SET_NUMBER = 0x8000000000000000L;
	
	private void write(SQLSelectStmt selectStmt, long parentUSedTabColNum, long sourceUsedTabColNum, long parentSetStmtNum, int setSelectStmtItemNumber, SQLSelectStmt rootSelectStmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, int withStmtItemNumberIfApplicable, String colGrp, String colTopExprGrp, OutputStream output, OutputStream usedTabColsOutput, OutputStream tableUsesOutput, java.lang.StringBuffer buffer) throws java.io.IOException {
		if (buffer == null) {
			buffer = new java.lang.StringBuffer(32);
		}
		String createdDt = toDateTimeString(new java.util.Date(), buffer);
		stmtNumberSeq++;
		selectStmt.etlRowWid = stmtNumberSeq;
		if (parentSetStmtNum == SELF_PARENT_SET_NUMBER) {
			parentSetStmtNum = selectStmt.etlRowWid;
		}
		
		if (rootSelectStmt == null) {
			rootSelectStmt = selectStmt.getRootSelectStmt(); 
		}
		
		if (sourceUsedTabColNum < 0) {
			sourceUsedTabColNum = parentUSedTabColNum;
		}
		
		withStmtRefsCount = 0; //reset
		
		output.write((folder + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidget + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidgetType + "|").getBytes(UTF8_ENCODING));
		output.write((parentWidget + "|").getBytes(UTF8_ENCODING)); //PARENT_WIDGET
		output.write((topWidget + "|").getBytes(UTF8_ENCODING)); //TOP_WIDGET
		output.write((String.valueOf(selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //STMT_NUMBER
		if (selectStmt.getType() == SQLWidget.WITH_STATEMENT) {
			output.write((selectStmt.getName() + "|").getBytes(UTF8_ENCODING)); //STMT_NAME
			output.write("Y|".getBytes(UTF8_ENCODING)); //WITH_STMT_FLG
			output.write((String.valueOf(withStmtItemNumberIfApplicable) + "|").getBytes(UTF8_ENCODING)); //WITH_STMT_NUMBER (sequence number against parent select statement)
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //STMT_NAME
			output.write("N|".getBytes(UTF8_ENCODING)); //WITH_STMT_FLG
			output.write("|".getBytes(UTF8_ENCODING)); //WITH_STMT_NUMBER
		}
		if (selectStmt.parentSelectTbl != null) {
			output.write((selectStmt.parentSelectTbl.name + "|").getBytes(UTF8_ENCODING)); //NESTED_TABLE_NAME
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //NESTED_TABLE_NAME
		}
		if (selectStmt.getAssocOperator() > 0) {
			output.write((SQLSelectStmt.getOperatorCode(selectStmt.getAssocOperator()) + "|").getBytes(UTF8_ENCODING)); //SET_OPERATION_TYPE
		}
		else if (selectStmt.parentSelectStmt != null || selectStmt.parentSelectTbl != null) {
			output.write("NESTED_SELECT|".getBytes(UTF8_ENCODING)); //SET_OPERATION_TYPE
		}
		else {
			output.write("SELECT|".getBytes(UTF8_ENCODING)); //SET_OPERATION_TYPE
		}
		output.write((String.valueOf(setSelectStmtItemNumber) + "|").getBytes(UTF8_ENCODING)); //SET_STMT_ITEM_NUMBER
		if (selectStmt.parentSelectStmt != null) { 
			output.write((String.valueOf(selectStmt.parentSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		else if (selectStmt.parentSelectTbl != null) {
			//System.out.println(":::::::::::::::::::::YES PARENT SELECT TABLE - (selectStmt.parentSelectTbl.parentSelectStmt == null): " + (selectStmt.parentSelectTbl.parentSelectStmt == null));
			if (selectStmt.parentSelectTbl.parentSelectStmt != null) {
				output.write((String.valueOf(selectStmt.parentSelectTbl.parentSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
			}
			else {
				output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
			}
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		if (rootSelectStmt != null) {
			output.write((String.valueOf(rootSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		if (parentSetStmtNum > 0) {
			output.write((String.valueOf(parentSetStmtNum) + "|").getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		output.write("|".getBytes(UTF8_ENCODING)); //PARAM_TBL_USE_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //DESCRIPTION
		output.write((selectStmt.selectDistinct ? "Y|" : "N|").getBytes(UTF8_ENCODING)); //DISTINCT_FLG
		output.write((String.valueOf(selectStmt.outputColumnsCount) + "|").getBytes(UTF8_ENCODING)); //OUTPUT_COLUMNS
		output.write((selectStmt.withInfaJoins ? "Y|" : "N|").getBytes(UTF8_ENCODING)); //WITH_INFA_JOINS_FLG
		if (selectStmt.limit > -1) {
			output.write((String.valueOf(selectStmt.limit) + "|").getBytes(UTF8_ENCODING)); //LIMIT_VALUE
			if (selectStmt.offset > -1) {
				output.write((String.valueOf(selectStmt.offset) + "|").getBytes(UTF8_ENCODING)); //LIMIT_OFFSET
			}
			else {
				output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_OFFSET
			}
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_VALUE
			output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_OFFSET
		}
		if (!Double.isNaN(selectStmt.topN)/*selectStmt.topN != Double.NaN*/) {
			output.write((selectStmt.topN == (long)selectStmt.topN ? String.valueOf((long)selectStmt.topN) + "|" : String.valueOf(selectStmt.topN) + "|").getBytes(UTF8_ENCODING)); //TOP_N
			output.write((selectStmt.selectDistinct ? "Y|" : "N|").getBytes(UTF8_ENCODING)); //TOP_N_PERCENT_FLG
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //TOP_N
			output.write("|".getBytes(UTF8_ENCODING)); //TOP_N_PERCENT_FLG
		}
		output.write("|".getBytes(UTF8_ENCODING)); //HINT_TEXT
		output.write((tag + "|").getBytes(UTF8_ENCODING)); //TAG
		output.write((createdDt + "|").getBytes(UTF8_ENCODING)); //CREATED_DT
		output.write((createdDt + "\n").getBytes(UTF8_ENCODING)); //LAST_UPD_DT
		
		
		for (int i=0;i<selectStmt.withStmtsCount;i++)
		{
			write(selectStmt.withStmts[i], parentUSedTabColNum, sourceUsedTabColNum, parentSetStmtNum, setSelectStmtItemNumber, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, (i + 1)/*withStmtItemNumberIfApplicable*/, colGrp, colTopExprGrp, output, usedTabColsOutput, tableUsesOutput, buffer);
		}
		
		for (int i=0;i<selectStmt.fromTablesCount;i++)
		{
			SQLTableRef fromTblRef = selectStmt.fromTables[i];
			if (fromTblRef.isSQLStmtTable()) {
				writeAsStmt(fromTblRef.asSQLStmtTable(), null/*tblAlias*/, (i + 1)/*sequenceNumber*/, parentSetStmtNum, selectStmt, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, buffer);
			}
			else if (fromTblRef.isSQLStmtTableAlias()) {
				SQLStmtTableAlias stltTblAlias = fromTblRef.asSQLStmtTableAlias();
				DBTableUse tblUse = __getDBTableUse(stltTblAlias.of.schema == null || stltTblAlias.of.schema.isEmpty() ? config.DEFAULT_DB_OWNER : stltTblAlias.of.schema/*dbOwner*/, stltTblAlias.of.name/*tableName*/, stltTblAlias.aliasName/*tableUseName*/);
				if (tblUse.etlRowWid < 0) { //case the table use has never been written to the file
					tblUse.etlRowWid = ++tableUseNumberSeq;
					writeDbTableUse(tblUse, tag, tableUsesOutput);
				}
				writeAsStmt(stltTblAlias.of, stltTblAlias.aliasName/*tblAlias*/, (i + 1)/*sequenceNumber*/, parentSetStmtNum, selectStmt, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, buffer);
			}
			else if (fromTblRef.isSQLSelectTable()) { 
				writeAsStmt(fromTblRef.asSQLSelectTable(), (i + 1)/*sequenceNumber*/, parentUSedTabColNum, sourceUsedTabColNum, parentSetStmtNum, selectStmt, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, colGrp, colTopExprGrp, output, usedTabColsOutput, tableUsesOutput, buffer);
			}
			else if (fromTblRef.isSQLWithStmtRef()) {
				writeAsStmt(fromTblRef.asSQLWithStmtRef(), (i + 1)/*sequenceNumber*/, parentSetStmtNum, selectStmt, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, buffer);
			}
			else if (fromTblRef.isSQLParameterTableRef()) {
				writeAsStmt(fromTblRef.asSQLParameterTableRef(), (i + 1)/*sequenceNumber*/, parentSetStmtNum, selectStmt, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, buffer);
			}
			else { //case JOIN_TABLE_REF ==> should not happen
				throw new RuntimeException(
				"EtlSQLAnalyzeWizard::write-1: from-table of unexpected type (class=" + fromTblRef.getClass().getName() + ")"
				);
			}
		}
		
		SQLWithStmtRef withStmtRef = null;
		SQLSelectStmt withStmt = null;
		for (int i=0;i<selectStmt.columnsCount;i++)
		{
			SQLColumn col = selectStmt.columns[i]; 
			SQLTableRef fromTbl = null;
			//System.out.println("col.class: " + col.getClass().getName());
			if (col.isSQLRawColumnRef()) {
				//System.out.println("selectStmt.fromTablesCount: " + selectStmt.fromTablesCount);
				SQLRawColumnRef rawCol = col.asSQLRawColumnRef(); //(SQLRawColumnRef)col;
				if (rawCol.tableAlias.isEmpty()) {
					String pseudoColumnName = SQLPseudoColumnNames.checkPseudoColName(rawCol.name);
					if (pseudoColumnName == null) {
						col = new SQLPseudoColumnRef(pseudoColumnName, col.aliasName);
						selectStmt.columns[i] = col;
					}
					else if (selectStmt.fromTablesCount == 1) {
						fromTbl = selectStmt.fromTables[0];
						if (fromTbl.isSQLWithStmtRef()) {
							withStmtRef = fromTbl.asSQLWithStmtRef();
							SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(rawCol.name);
							if (withStmtColumn == null) {
								throw new RuntimeException(
								"EtlSQLAnalyzeWizard::write-2: unknown with-statement column (columnName=" + rawCol.name + ")"
								);
							}
							col = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, col.aliasName);
						}
						else {
							//if (fromTbl.isSQLSelectTable()) {
							//	System.out.println("EtlSQLAnalyzeWizard:::: - isSQLSelectTable=true, rawCol.name: " + rawCol.name);
							//}
							col = new SQLColumnRef(fromTbl, rawCol.name, col.aliasName);
						}
						selectStmt.columns[i] = col;
					}
				}
				else {
					fromTbl = selectStmt.getTableByAliasExt(rawCol.tableAlias);
					if (fromTbl != null) {
						//if (fromTbl.isSQLSelectTable()) {
						//	System.out.println("EtlSQLAnalyzeWizard - isSQLSelectTable=true, rawCol.name: " + rawCol.name + ", fromTbl.isNestedTableRef(): " + fromTbl.isNestedTableRef());
						//}
						if (fromTbl.isSQLWithStmtRef()) {
							withStmtRef = fromTbl.asSQLWithStmtRef();
							SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(rawCol.name);
							if (withStmtColumn == null) {
								throw new RuntimeException(
								"EtlSQLAnalyzeWizard::write-3: unknown with-statement column (columnName=" + rawCol.name + ")"
								);
							}
							col = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, col.aliasName);
						}
						else {
							col = new SQLColumnRef(fromTbl, rawCol.name, col.aliasName);
							//System.out.println("EtlSQLAnalyzeWizard - col.getLogicalColumnType(): '" + col.getLogicalColumnType() + "'");
						}
						selectStmt.columns[i] = col;
					}
				}
			}
			else {
				fromTbl = col.getTable();
			}
			long fromTableNum = fromTbl == null ? -1 : fromTbl.etlRowWid;
			write(col, fromTableNum, FROM_GRP_NAME, colTopExprGrp, parentUSedTabColNum, -1/*sourceUsedTabColNum*/, (i + 1)/*sequenceNumber*/, -1/*setSelectStmtItemNumber*/, selectStmt/*ownerSelectStmt*/, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput, tableUsesOutput, buffer);
		}
		
		for (int i=0;i<selectStmt.joinsCount;i++)
		{
			SQLJoin join = selectStmt.joins[i];
			writeAsStmt(join, (i + 1)/*sequenceNumber*/, -1/*parentSetStmtNum*/, selectStmt/*parentSelectStmt*/, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, buffer);
			for (int j=0;j<join.joinColumnsCount;j++)
			{
				SQLColumn joinCol = join.joinColumns[j]; 
				SQLTableRef fromTbl;
				if (joinCol.isSQLRawColumnRef()) {
					SQLRawColumnRef rawColRef = joinCol.asSQLRawColumnRef();
					SQLTableRef tblRef = selectStmt.getTableByAliasExt(rawColRef.tableAlias);
					fromTbl = tblRef;
					if (tblRef != null) {
						if (tblRef.isSQLWithStmtRef()) {
							withStmtRef = tblRef.asSQLWithStmtRef();
							SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(rawColRef.name);
							if (withStmtColumn == null) {
								throw new RuntimeException(
								"EtlSQLAnalyzeWizard::write-4: unknown with-statement column (columnName=" + rawColRef.name + ")"
								);
							}
							joinCol = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, joinCol.aliasName);
							join.joinColumns[i] = joinCol;
						}
						else {
							joinCol = new SQLColumnRef(tblRef, rawColRef.name, joinCol.aliasName);
							join.joinColumns[i] = joinCol;
						}
					}
					else if (rawColRef.tableAlias.isEmpty()) {
						byte literalType = SQLLiteral.checkLiteralType(rawColRef.name);
						if (literalType > -1) {
							joinCol = new SQLLiteral(rawColRef.name, literalType, rawColRef.aliasName);
							join.joinColumns[i] = joinCol;
						}
					}
				}
				else if (joinCol.isSQLColumnRef() && (fromTbl = joinCol.getTable()) != null && fromTbl.isSQLWithStmtRef()) { //fix wrong type of SQL column because didn't check if the table-ref was an SQLWithStmtRef 
					withStmtRef = fromTbl.asSQLWithStmtRef();
					SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(joinCol.getName());
					if (withStmtColumn == null) {
						throw new RuntimeException(
						"EtlSQLAnalyzeWizard::write-5: unknown with-statement column (columnName=" + joinCol.getName() + ")"
						);
					}
					joinCol = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, joinCol.aliasName);
					join.joinColumns[i] = joinCol;
				}
				else {
					fromTbl = joinCol.getTable();
					if (fromTbl == SQLStmtTable.NO_TABLE && selectStmt.fromTablesCount ==1) {
						fromTbl = selectStmt.fromTables[0];
						((SQLColumnRef)joinCol).table =fromTbl;
					}/*NOW DONE ABOVE...
					else if (fromTbl == null && (joinCol instanceof SQLRawColumnRef)) {
						SQLRawColumnRef rawCol = (SQLRawColumnRef)joinCol;
						if (rawCol.tableAlias.isEmpty()) {
							byte literalType = SQLLiteral.checkLiteralType(rawCol.name);
							if (literalType > -1) {
								joinCol = new SQLLiteral(rawCol.name, literalType, rawCol.aliasName);
								join.joinColumns[i] = joinCol;
							}
						}
					}*/
				}
				long fromTableNum = fromTbl == null ? -1 : fromTbl.etlRowWid;
				write(joinCol, fromTableNum, JOIN_GRP_NAME, colTopExprGrp, parentUSedTabColNum, -1/*sourceUsedTabColNum*/, (j + 1)/*sequenceNumber*/, -1/*setSelectStmtItemNumber*/, selectStmt/*parentSelectStmt*/, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput, tableUsesOutput, buffer);
			}
		}
		if (selectStmt.whereClause != null) {
			for (int i=0;i<selectStmt.whereClause.involvedColumnsCount;i++)
			{
				SQLColumn whereClauseCol = selectStmt.whereClause.involvedColumns[i]; 
				SQLTableRef fromTbl;
				if (whereClauseCol.isSQLRawColumnRef()) {
					SQLRawColumnRef rawColRef = whereClauseCol.asSQLRawColumnRef();
					SQLTableRef tblRef = selectStmt.getTableByAliasExt(rawColRef.tableAlias);
					fromTbl = tblRef;
					if (tblRef != null) {
						if (tblRef.isSQLWithStmtRef()) {
							withStmtRef = tblRef.asSQLWithStmtRef();
							SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(rawColRef.name);
							if (withStmtColumn == null) {
								throw new RuntimeException(
								"EtlSQLAnalyzeWizard::write-6: unknown with-statement column (columnName=" + rawColRef.name + ")"
								);
							}
							whereClauseCol = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, whereClauseCol.aliasName);
							selectStmt.whereClause.involvedColumns[i] = whereClauseCol;
						}
						else {
							whereClauseCol = new SQLColumnRef(tblRef, rawColRef.name, whereClauseCol.aliasName);
							selectStmt.whereClause.involvedColumns[i] = whereClauseCol;
						}
					}
					else if (rawColRef.tableAlias.isEmpty()) {
						byte literalType = SQLLiteral.checkLiteralType(rawColRef.name);
						if (literalType > -1) {
							whereClauseCol = new SQLLiteral(rawColRef.name, literalType, rawColRef.aliasName);
							selectStmt.whereClause.involvedColumns[i] = whereClauseCol;
						}
					}
				}
				else if (whereClauseCol.isSQLColumnRef() && (fromTbl = whereClauseCol.getTable()) != null && fromTbl.isSQLWithStmtRef()) { //fix wrong type of SQL column because didn't check if the table-ref was an SQLWithStmtRef 
					withStmtRef = fromTbl.asSQLWithStmtRef();
					SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(whereClauseCol.getName());
					if (withStmtColumn == null) {
						throw new RuntimeException(
						"EtlSQLAnalyzeWizard::write-7: unknown with-statement column (columnName=" + whereClauseCol.getName() + ")"
						);
					}
					whereClauseCol = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, whereClauseCol.aliasName);
					selectStmt.whereClause.involvedColumns[i] = whereClauseCol;
				}
				else {
					fromTbl = whereClauseCol.getTable();
					if (fromTbl == SQLStmtTable.NO_TABLE && selectStmt.fromTablesCount ==1) {
						fromTbl = selectStmt.fromTables[0];
						((SQLColumnRef)whereClauseCol).table =fromTbl;
					}/*NOW DONE ABOVE...
					else if (fromTbl == null && (whereClauseCol instanceof SQLRawColumnRef)) {
						SQLRawColumnRef rawCol = (SQLRawColumnRef)whereClauseCol;
						if (rawCol.tableAlias.isEmpty()) {
							byte literalType = SQLLiteral.checkLiteralType(rawCol.name);
							if (literalType > -1) {
								whereClauseCol = new SQLLiteral(rawCol.name, literalType, rawCol.aliasName);
								selectStmt.whereClause.involvedColumns[i] = whereClauseCol;
							}
						}
					}*/
				}
				long fromTableNum = fromTbl == null ? -1 : fromTbl.etlRowWid;
				write(whereClauseCol, fromTableNum, WHERE_GRP_NAME, colTopExprGrp, parentUSedTabColNum, -1/*sourceUsedTabColNum*/, (i + 1)/*sequenceNumber*/, -1/*setSelectStmtItemNumber*/, selectStmt/*parentSelectStmt*/, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput, tableUsesOutput, buffer);
			}
		}
		if (selectStmt.groupByClause != null) {
			for (int i=0;i<selectStmt.groupByClause.columnsCount;i++)
			{
				SQLColumn grpByClauseCol = selectStmt.groupByClause.columns[i]; 
				SQLTableRef fromTbl;
				if (grpByClauseCol.isSQLRawColumnRef()) {
					SQLRawColumnRef rawColRef = grpByClauseCol.asSQLRawColumnRef();
					SQLTableRef tblRef = selectStmt.getTableByAliasExt(rawColRef.tableAlias);
					fromTbl = tblRef;
					if (tblRef != null) {
						if (tblRef.isSQLWithStmtRef()) {
							withStmtRef = tblRef.asSQLWithStmtRef();
							SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(rawColRef.name);
							if (withStmtColumn == null) {
								throw new RuntimeException(
								"EtlSQLAnalyzeWizard::write-10: unknown with-statement column (columnName=" + rawColRef.name + ")"
								);
							}
							grpByClauseCol = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, grpByClauseCol.aliasName);
							selectStmt.groupByClause.columns[i] = grpByClauseCol;
						}
						else {
							grpByClauseCol = new SQLColumnRef(tblRef, rawColRef.name, grpByClauseCol.aliasName);
							selectStmt.groupByClause.columns[i] = grpByClauseCol;
						}
					}
					else if (rawColRef.tableAlias.isEmpty()) {
						byte literalType = SQLLiteral.checkLiteralType(rawColRef.name);
						if (literalType > -1) {
							grpByClauseCol = new SQLLiteral(rawColRef.name, literalType, rawColRef.aliasName);
							selectStmt.groupByClause.columns[i] = grpByClauseCol;
						}
					}
				}
				else if (grpByClauseCol.isSQLColumnRef() && (fromTbl = grpByClauseCol.getTable()) != null && fromTbl.isSQLWithStmtRef()) { //fix wrong type of SQL column because didn't check if the table-ref was an SQLWithStmtRef 
					withStmtRef = fromTbl.asSQLWithStmtRef();
					SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(grpByClauseCol.getName());
					if (withStmtColumn == null) {
						throw new RuntimeException(
						"EtlSQLAnalyzeWizard::write-11: unknown with-statement column (columnName=" + grpByClauseCol.getName() + ")"
						);
					}
					grpByClauseCol = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, grpByClauseCol.aliasName);
					selectStmt.groupByClause.columns[i] = grpByClauseCol;
				}
				else {
					fromTbl = grpByClauseCol.getTable();
					if (fromTbl == SQLStmtTable.NO_TABLE && selectStmt.fromTablesCount ==1) {
						fromTbl = selectStmt.fromTables[0];
						((SQLColumnRef)grpByClauseCol).table =fromTbl;
					}/*NOW DONE ABOVE...
					else if (fromTbl == null && (grpByClauseCol instanceof SQLRawColumnRef)) {
						SQLRawColumnRef rawCol = (SQLRawColumnRef)grpByClauseCol;
						if (rawCol.tableAlias.isEmpty()) {
							byte literalType = SQLLiteral.checkLiteralType(rawCol.name);
							if (literalType > -1) {
								grpByClauseCol = new SQLLiteral(rawCol.name, literalType, rawCol.aliasName);
								selectStmt.groupByClause.columns[i] = grpByClauseCol;
							}
						}
					}*/
				}
				long fromTableNum = fromTbl == null ? -1 : fromTbl.etlRowWid;
				write(grpByClauseCol, fromTableNum, GROUP_BY_GRP_NAME, colTopExprGrp, parentUSedTabColNum, -1/*sourceUsedTabColNum*/, (i + 1)/*sequenceNumber*/, -1/*setSelectStmtItemNumber*/, selectStmt/*parentSelectStmt*/, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput, tableUsesOutput, buffer);
			}
		}
		if (selectStmt.havingClause != null) {
			for (int i=0;i<selectStmt.havingClause.involvedColumnsCount;i++)
			{
				SQLColumn havingClauseCol = selectStmt.havingClause.involvedColumns[i]; 
				SQLTableRef fromTbl;
				if (havingClauseCol.isSQLRawColumnRef()) {
					SQLRawColumnRef rawColRef = havingClauseCol.asSQLRawColumnRef();
					SQLTableRef tblRef = selectStmt.getTableByAliasExt(rawColRef.tableAlias);
					fromTbl = tblRef;
					if (tblRef != null) {
						if (tblRef.isSQLWithStmtRef()) {
							withStmtRef = tblRef.asSQLWithStmtRef();
							SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(rawColRef.name);
							if (withStmtColumn == null) {
								throw new RuntimeException(
								"EtlSQLAnalyzeWizard::write-8: unknown with-statement column (columnName=" + rawColRef.name + ")"
								);
							}
							havingClauseCol = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, havingClauseCol.aliasName);
							selectStmt.havingClause.involvedColumns[i] = havingClauseCol;
						}
						else {
							havingClauseCol = new SQLColumnRef(tblRef, rawColRef.name, havingClauseCol.aliasName);
							selectStmt.havingClause.involvedColumns[i] = havingClauseCol;
						}
					}
					else if (rawColRef.tableAlias.isEmpty()) {
						byte literalType = SQLLiteral.checkLiteralType(rawColRef.name);
						if (literalType > -1) {
							havingClauseCol = new SQLLiteral(rawColRef.name, literalType, rawColRef.aliasName);
							selectStmt.havingClause.involvedColumns[i] = havingClauseCol;
						}
					}
				}
				else if (havingClauseCol.isSQLColumnRef() && (fromTbl = havingClauseCol.getTable()) != null && fromTbl.isSQLWithStmtRef()) { //fix wrong type of SQL column because didn't check if the table-ref was an SQLWithStmtRef 
					withStmtRef = fromTbl.asSQLWithStmtRef();
					SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(havingClauseCol.getName());
					if (withStmtColumn == null) {
						throw new RuntimeException(
						"EtlSQLAnalyzeWizard::write-9: unknown with-statement column (columnName=" + havingClauseCol.getName() + ")"
						);
					}
					havingClauseCol = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, havingClauseCol.aliasName);
					selectStmt.havingClause.involvedColumns[i] = havingClauseCol;
				}
				else {
					fromTbl = havingClauseCol.getTable();
					if (fromTbl == SQLStmtTable.NO_TABLE && selectStmt.fromTablesCount ==1) {
						fromTbl = selectStmt.fromTables[0];
						((SQLColumnRef)havingClauseCol).table =fromTbl;
					}/*NOW DONE ABOVE
					else if (fromTbl == null && (havingClauseCol instanceof SQLRawColumnRef)) {
						SQLRawColumnRef rawCol = (SQLRawColumnRef)havingClauseCol;
						if (rawCol.tableAlias.isEmpty()) {
							byte literalType = SQLLiteral.checkLiteralType(rawCol.name);
							if (literalType > -1) {
								havingClauseCol = new SQLLiteral(rawCol.name, literalType, rawCol.aliasName);
								selectStmt.havingClause.involvedColumns[i] = havingClauseCol;
							}
						}
					}*/
				}
				long fromTableNum = fromTbl == null ? -1 : fromTbl.etlRowWid;
				write(havingClauseCol, fromTableNum, HAVING_GRP_NAME, colTopExprGrp, parentUSedTabColNum, -1/*sourceUsedTabColNum*/, (i + 1)/*sequenceNumber*/, -1/*setSelectStmtItemNumber*/, selectStmt/*parentSelectStmt*/, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput, tableUsesOutput, buffer);
			}
		}
		if (selectStmt.orderByClause != null) {
			for (int i=0;i<selectStmt.orderByClause.columnsCount;i++)
			{
				SQLColumn orderByClauseCol = selectStmt.orderByClause.columns[i]; 
				SQLTableRef fromTbl;
				if (orderByClauseCol.isSQLRawColumnRef()) {
					SQLRawColumnRef rawColRef = orderByClauseCol.asSQLRawColumnRef();
					SQLTableRef tblRef = selectStmt.getTableByAliasExt(rawColRef.tableAlias);
					fromTbl = tblRef;
					if (tblRef != null) {
						if (tblRef.isSQLWithStmtRef()) {
							withStmtRef = tblRef.asSQLWithStmtRef();
							SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(rawColRef.name);
							if (withStmtColumn == null) {
								throw new RuntimeException(
								"EtlSQLAnalyzeWizard::write-10: unknown with-statement column (columnName=" + rawColRef.name + ")"
								);
							}
							orderByClauseCol = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, orderByClauseCol.aliasName);
							selectStmt.orderByClause.columns[i] = orderByClauseCol;
						}
						else {
							orderByClauseCol = new SQLColumnRef(tblRef, rawColRef.name, orderByClauseCol.aliasName);
							selectStmt.orderByClause.columns[i] = orderByClauseCol;
						}
					}
					else if (rawColRef.tableAlias.isEmpty()) {
						byte literalType = SQLLiteral.checkLiteralType(rawColRef.name);
						if (literalType > -1) {
							orderByClauseCol = new SQLLiteral(rawColRef.name, literalType, rawColRef.aliasName);
							selectStmt.orderByClause.columns[i] = orderByClauseCol;
						}
					}
				}
				else if (orderByClauseCol.isSQLColumnRef() && (fromTbl = orderByClauseCol.getTable()) != null && fromTbl.isSQLWithStmtRef()) { //fix wrong type of SQL column because didn't check if the table-ref was an SQLWithStmtRef 
					withStmtRef = fromTbl.asSQLWithStmtRef();
					SQLColumn withStmtColumn = withStmtRef.withStmt.getColumn(orderByClauseCol.getName());
					if (withStmtColumn == null) {
						throw new RuntimeException(
						"EtlSQLAnalyzeWizard::write-11: unknown with-statement column (columnName=" + orderByClauseCol.getName() + ")"
						);
					}
					orderByClauseCol = new SQLWithStmtColumnRef(withStmtRef, withStmtColumn, orderByClauseCol.aliasName);
					selectStmt.orderByClause.columns[i] = orderByClauseCol;
				}
				else {
					fromTbl = orderByClauseCol.getTable();
					if (fromTbl == SQLStmtTable.NO_TABLE && selectStmt.fromTablesCount ==1) {
						fromTbl = selectStmt.fromTables[0];
						orderByClauseCol.asSQLColumnRef().table = fromTbl;
					}/*NOW DONE ABOVE...
					else if (fromTbl == null && (orderByClauseCol instanceof SQLRawColumnRef)) {
						SQLRawColumnRef rawCol = (SQLRawColumnRef)orderByClauseCol;
						if (rawCol.tableAlias.isEmpty()) {
							byte literalType = SQLLiteral.checkLiteralType(rawCol.name);
							if (literalType > -1) {
								orderByClauseCol = new SQLLiteral(rawCol.name, literalType, rawCol.aliasName);
								selectStmt.orderByClause.columns[i] = orderByClauseCol;
							}
						}
					}*/
				}
				long fromTableNum = fromTbl == null ? -1 : fromTbl.etlRowWid;
				write(orderByClauseCol, fromTableNum, GROUP_BY_GRP_NAME, colTopExprGrp, parentUSedTabColNum, -1/*sourceUsedTabColNum*/, (i + 1)/*sequenceNumber*/, -1/*setSelectStmtItemNumber*/, selectStmt/*parentSelectStmt*/, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput, tableUsesOutput, buffer);
			}
		}
	} 
	
	private final void writeAsStmt(SQLParameterTableRef paramTableRef, int sequenceNumber, long parentSetStmtNum, SQLSelectStmt selectStmt, SQLSelectStmt rootSelectStmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, OutputStream output, java.lang.StringBuffer buffer) throws java.io.IOException {
		if (buffer == null) {
			buffer = new java.lang.StringBuffer(32);
		}
		String createdDt = toDateTimeString(new java.util.Date(), buffer);
		stmtNumberSeq++;
		paramTableRef.etlRowWid = stmtNumberSeq;
		
		output.write((folder + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidget + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidgetType + "|").getBytes(UTF8_ENCODING));
		output.write((parentWidget + "|").getBytes(UTF8_ENCODING)); //PARENT_WIDGET
		output.write((topWidget + "|").getBytes(UTF8_ENCODING)); //TOP_WIDGET
		output.write((String.valueOf(paramTableRef.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //STMT_NUMBER
		output.write((paramTableRef.schema + "|").getBytes(UTF8_ENCODING)); //STMT_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_STMT_FLG
		output.write((String.valueOf(sequenceNumber) + "|").getBytes(UTF8_ENCODING)); //WITH_STMT_NUMBER
		output.write((paramTableRef.name + "|").getBytes(UTF8_ENCODING)); //NESTED_TABLE_NAME
		output.write((FROM_TABLE_GROUP/*SQLWidget.getType(join.getType())*/ + "|").getBytes(UTF8_ENCODING)); //SET_OPERATION_TYPE
		output.write((String.valueOf(SQLTableRef.PARAM_TABLE_REF) + "|").getBytes(UTF8_ENCODING)); //SET_STMT_ITEM_NUMBER
		if (selectStmt != null) {
			output.write((String.valueOf(selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		if (rootSelectStmt != null) {
			output.write((String.valueOf(rootSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		if (parentSetStmtNum > 0) {
			output.write((String.valueOf(parentSetStmtNum) + "|").getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		output.write((paramTableRef.tableAlias + "|").getBytes(UTF8_ENCODING)); //PARAM_TBL_USE_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //DESCRIPTION
		output.write("|".getBytes(UTF8_ENCODING)); //DISTINCT_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //OUTPUT_COLUMNS
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_INFA_JOINS_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_VALUE
		output.write((String.valueOf(paramTableRef.parametirizedParts) + "|").getBytes(UTF8_ENCODING)); //LIMIT_OFFSET
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N_PERCENT_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //HINT_TEXT
		output.write((tag + "|").getBytes(UTF8_ENCODING)); //TAG
		output.write((createdDt + "|").getBytes(UTF8_ENCODING)); //CREATED_DT
		output.write((createdDt + "\n").getBytes(UTF8_ENCODING)); //LAST_UPD_DT
	}
	private final void writeAsStmt(SQLStmtTable stmtTbl, String tblAlias, int sequenceNumber, long parentSetStmtNum, SQLSelectStmt selectStmt, SQLSelectStmt rootSelectStmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, OutputStream output, java.lang.StringBuffer buffer) throws java.io.IOException {
		if (buffer == null) {
			buffer = new java.lang.StringBuffer(32);
		}
		String createdDt = toDateTimeString(new java.util.Date(), buffer);
		stmtNumberSeq++;
		stmtTbl.etlRowWid = stmtNumberSeq;
		
		//System.out.println("writeAsStmt for SQLStmtTable - stmtNumberSeq: " + stmtNumberSeq + ", stmtTbl.etlRowWid: " + stmtTbl.etlRowWid);
		
		output.write((folder + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidget + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidgetType + "|").getBytes(UTF8_ENCODING));
		output.write((parentWidget + "|").getBytes(UTF8_ENCODING)); //PARENT_WIDGET
		output.write((topWidget + "|").getBytes(UTF8_ENCODING)); //TOP_WIDGET
		output.write((String.valueOf(stmtTbl.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //STMT_NUMBER
		output.write((stmtTbl.schema + "|").getBytes(UTF8_ENCODING)); //STMT_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_STMT_FLG
		output.write((String.valueOf(sequenceNumber) + "|").getBytes(UTF8_ENCODING)); //WITH_STMT_NUMBER
		output.write((stmtTbl.name + "|").getBytes(UTF8_ENCODING)); //NESTED_TABLE_NAME
		output.write((FROM_TABLE_GROUP/*SQLWidget.getType(join.getType())*/ + "|").getBytes(UTF8_ENCODING)); //SET_OPERATION_TYPE
		output.write((String.valueOf(tblAlias != null && !tblAlias.isEmpty() ? SQLTableRef.STMT_TABLE_ALIAS: SQLTableRef.STMT_TABLE) + "|").getBytes(UTF8_ENCODING)); //SET_STMT_ITEM_NUMBER
		if (selectStmt != null) {
			output.write((String.valueOf(selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		if (rootSelectStmt != null) {
			output.write((String.valueOf(rootSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		if (parentSetStmtNum > 0) {
			output.write((String.valueOf(parentSetStmtNum) + "|").getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		if (tblAlias != null) {
			output.write((tblAlias + "|").getBytes(UTF8_ENCODING)); //PARAM_TBL_USE_NAME
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARAM_TBL_USE_NAME
		}
		output.write("|".getBytes(UTF8_ENCODING)); //DESCRIPTION
		output.write("|".getBytes(UTF8_ENCODING)); //DISTINCT_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //OUTPUT_COLUMNS
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_INFA_JOINS_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_VALUE
		output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_OFFSET
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N_PERCENT_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //HINT_TEXT
		output.write((tag + "|").getBytes(UTF8_ENCODING)); //TAG
		output.write((createdDt + "|").getBytes(UTF8_ENCODING)); //CREATED_DT
		output.write((createdDt + "\n").getBytes(UTF8_ENCODING)); //LAST_UPD_DT
	}
	
	private final void writeAsStmt(SQLSelectTable selectTbl, int sequenceNumber, long parentUSedTabColNum, long sourceUsedTabColNum, long parentSetStmtNum, SQLSelectStmt selectStmt, SQLSelectStmt rootSelectStmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, String colGrp, String colTopExprGrp, OutputStream output, OutputStream usedTabColsOutput, OutputStream tableUsesOutput, java.lang.StringBuffer buffer) throws java.io.IOException {
		if (buffer == null) {
			buffer = new java.lang.StringBuffer(32);
		}
		String createdDt = toDateTimeString(new java.util.Date(), buffer);
		stmtNumberSeq++;
		selectTbl.etlRowWid = stmtNumberSeq;
		
		write(selectTbl.selectStmt, parentUSedTabColNum, sourceUsedTabColNum, -1/*parentSetStmtNum*/, -1/*setSelectStmtItemNumber*/, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, -1/*withStmtItemNumberIfApplicable*/, colGrp, colTopExprGrp, output, usedTabColsOutput, tableUsesOutput, buffer);
		
		output.write((folder + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidget + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidgetType + "|").getBytes(UTF8_ENCODING));
		output.write((parentWidget + "|").getBytes(UTF8_ENCODING)); //PARENT_WIDGET
		output.write((topWidget + "|").getBytes(UTF8_ENCODING)); //TOP_WIDGET
		output.write((String.valueOf(selectTbl.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //STMT_NUMBER
		output.write("|".getBytes(UTF8_ENCODING)); //STMT_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_STMT_FLG
		output.write((String.valueOf(sequenceNumber) + "|").getBytes(UTF8_ENCODING)); //WITH_STMT_NUMBER
		output.write((selectTbl.name + "|").getBytes(UTF8_ENCODING)); //NESTED_TABLE_NAME
		output.write((FROM_TABLE_GROUP/*SQLWidget.getType(join.getType())*/ + "|").getBytes(UTF8_ENCODING)); //SET_OPERATION_TYPE
		output.write((String.valueOf(SQLTableRef.SELECT_TABLE) + "|").getBytes(UTF8_ENCODING)); //SET_STMT_ITEM_NUMBER
		if (selectStmt != null) {
			output.write((String.valueOf(selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		if (rootSelectStmt != null) {
			output.write((String.valueOf(rootSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		if (parentSetStmtNum > 0) {
			output.write((String.valueOf(parentSetStmtNum) + "|").getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		output.write((selectTbl.name/*tableAlias*/ + "|").getBytes(UTF8_ENCODING)); //PARAM_TBL_USE_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //DESCRIPTION
		output.write("|".getBytes(UTF8_ENCODING)); //DISTINCT_FLG
		output.write((String.valueOf(selectTbl.selectStmt.outputColumnsCount) + "|").getBytes(UTF8_ENCODING)); //OUTPUT_COLUMNS
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_INFA_JOINS_FLG
		output.write((String.valueOf(selectTbl.selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //LIMIT_VALUE
		output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_OFFSET
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N_PERCENT_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //HINT_TEXT
		output.write((tag + "|").getBytes(UTF8_ENCODING)); //TAG
		output.write((createdDt + "|").getBytes(UTF8_ENCODING)); //CREATED_DT
		output.write((createdDt + "\n").getBytes(UTF8_ENCODING)); //LAST_UPD_DT
	}
	
	
	private final void writeAsStmt(SQLWithStmtRef withStmtTblRef, int sequenceNumber, long parentSetStmtNum, SQLSelectStmt selectStmt, SQLSelectStmt rootSelectStmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, OutputStream output, java.lang.StringBuffer buffer) throws java.io.IOException {
		if (buffer == null) {
			buffer = new java.lang.StringBuffer(32);
		}
		String createdDt = toDateTimeString(new java.util.Date(), buffer);
		stmtNumberSeq++;
		withStmtTblRef.etlRowWid = stmtNumberSeq;
		
		output.write((folder + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidget + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidgetType + "|").getBytes(UTF8_ENCODING));
		output.write((parentWidget + "|").getBytes(UTF8_ENCODING)); //PARENT_WIDGET
		output.write((topWidget + "|").getBytes(UTF8_ENCODING)); //TOP_WIDGET
		output.write((String.valueOf(stmtNumberSeq) + "|").getBytes(UTF8_ENCODING)); //STMT_NUMBER
		output.write("|".getBytes(UTF8_ENCODING)); //STMT_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_STMT_FLG
		output.write((String.valueOf(sequenceNumber) + "|").getBytes(UTF8_ENCODING)); //WITH_STMT_NUMBER
		output.write((withStmtTblRef.withStmt.name + "|").getBytes(UTF8_ENCODING)); //NESTED_TABLE_NAME
		output.write((FROM_TABLE_GROUP/*SQLWidget.getType(join.getType())*/ + "|").getBytes(UTF8_ENCODING)); //SET_OPERATION_TYPE
		output.write((String.valueOf(SQLTableRef.WITH_STMT_TABLE_REF) + "|").getBytes(UTF8_ENCODING)); //SET_STMT_ITEM_NUMBER
		if (selectStmt != null) {
			output.write((String.valueOf(selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		if (rootSelectStmt != null) {
			output.write((String.valueOf(rootSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		if (parentSetStmtNum > 0) {
			output.write((String.valueOf(parentSetStmtNum) + "|").getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		if (withStmtTblRef.aliasName != null) {
			output.write((withStmtTblRef.aliasName + "|").getBytes(UTF8_ENCODING)); //PARAM_TBL_USE_NAME
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARAM_TBL_USE_NAME
		}
		output.write("|".getBytes(UTF8_ENCODING)); //DESCRIPTION
		output.write("|".getBytes(UTF8_ENCODING)); //DISTINCT_FLG
		output.write((String.valueOf(withStmtTblRef.withStmt.outputColumnsCount) + "|").getBytes(UTF8_ENCODING)); //OUTPUT_COLUMNS
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_INFA_JOINS_FLG
		output.write((String.valueOf(withStmtTblRef.withStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //LIMIT_VALUE
		output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_OFFSET
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N_PERCENT_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //HINT_TEXT
		output.write((tag + "|").getBytes(UTF8_ENCODING)); //TAG
		output.write((createdDt + "|").getBytes(UTF8_ENCODING)); //CREATED_DT
		output.write((createdDt + "\n").getBytes(UTF8_ENCODING)); //LAST_UPD_DT
	}
	
	private final void writeAsStmt(SQLSequenceValueRef seqValRef, long parentSetStmtNum, SQLSelectStmt rootSelectStmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, OutputStream output, java.lang.StringBuffer buffer) throws java.io.IOException {
		throw new UnsupportedOperationException(
		"EtlSQLAnalyzeWizard::writeAsStmt-2: the method has yet to be effectively supported"
		);
	}
	
	private final void writeAsStmt(SQLJoin join, int sequenceNumber, long parentSetStmtNum, SQLSelectStmt selectStmt, SQLSelectStmt rootSelectStmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, OutputStream output, java.lang.StringBuffer buffer) throws java.io.IOException {
		//JOIN_GRP_NAME ("JOIN") ==> SET_OPERATION_TYPE
		//sequenceNumber ==> WITH_STMT_NUMBER
		//joinType ==> SET_STMT_ITEM_NUMBER
		//throw new UnsupportedOperationException(
		//"EtlSQLAnalyzeWizard::writeAsStmt-3: the method has yet to be effectively supported"
		//);
		if (buffer == null) {
			buffer = new java.lang.StringBuffer(32);
		}
		String createdDt = toDateTimeString(new java.util.Date(), buffer);
		stmtNumberSeq++;
		join.etlRowWid = stmtNumberSeq;
		
		if (rootSelectStmt == null) {
			rootSelectStmt = selectStmt.getRootSelectStmt(); 
		}
		
		output.write((folder + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidget + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidgetType + "|").getBytes(UTF8_ENCODING));
		output.write((parentWidget + "|").getBytes(UTF8_ENCODING)); //PARENT_WIDGET
		output.write((topWidget + "|").getBytes(UTF8_ENCODING)); //TOP_WIDGET
		output.write((String.valueOf(join.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //STMT_NUMBER
		output.write((join.joinedTbl.getSchema() + "|").getBytes(UTF8_ENCODING)); //STMT_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_STMT_FLG
		output.write((String.valueOf(sequenceNumber) + "|").getBytes(UTF8_ENCODING)); //WITH_STMT_NUMBER
		output.write((join.joinedTbl.getTableName() + "|").getBytes(UTF8_ENCODING)); //NESTED_TABLE_NAME
		output.write((JOIN_GRP_NAME/*SQLWidget.getType(join.getType())*/ + "|").getBytes(UTF8_ENCODING)); //SET_OPERATION_TYPE
		output.write((String.valueOf(join.joinType) + "|").getBytes(UTF8_ENCODING)); //SET_STMT_ITEM_NUMBER
		if (selectStmt != null) {
			output.write((String.valueOf(selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		if (rootSelectStmt != null) {
			output.write((String.valueOf(rootSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		if (parentSetStmtNum > 0) {
			output.write((String.valueOf(parentSetStmtNum) + "|").getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		output.write((join.joinedTbl.getAliasName() + "|").getBytes(UTF8_ENCODING)); //PARAM_TBL_USE_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //DESCRIPTION
		output.write("|".getBytes(UTF8_ENCODING)); //DISTINCT_FLG
		output.write((String.valueOf(join.joinColumnsCount) + "|").getBytes(UTF8_ENCODING)); //OUTPUT_COLUMNS
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_INFA_JOINS_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_VALUE
		output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_OFFSET
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N_PERCENT_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //HINT_TEXT
		output.write((tag + "|").getBytes(UTF8_ENCODING)); //TAG
		output.write((createdDt + "|").getBytes(UTF8_ENCODING)); //CREATED_DT
		output.write((createdDt + "\n").getBytes(UTF8_ENCODING)); //LAST_UPD_DT
	}
	
	private static final void writeWC_ETL_STMT_USED_TAB_COLHeader(OutputStream output) throws java.io.IOException {
		output.write("FOLDER|".getBytes(UTF8_ENCODING));
		output.write("OWNER_WIDGET_NAME|".getBytes(UTF8_ENCODING));
		output.write("OWNER_WIDGET_TYPE|".getBytes(UTF8_ENCODING));
		output.write("PARENT_WIDGET_NAME|".getBytes(UTF8_ENCODING)); //new, was missing
		output.write("TOP_WIDGET_NAME|".getBytes(UTF8_ENCODING)); //new, was missing
		output.write("USED_TAB_COL_NUM|".getBytes(UTF8_ENCODING));
		output.write("FROM_TABLE_NUM|".getBytes(UTF8_ENCODING)); //new
		output.write("DB_OWNER|".getBytes(UTF8_ENCODING));
		output.write("DB_TABLE_NAME|".getBytes(UTF8_ENCODING));
		output.write("TABLE_USE_NAME|".getBytes(UTF8_ENCODING));
		output.write("SELECT_TABLE_SQL_STMT_NUMBER|".getBytes(UTF8_ENCODING));
		output.write("COL_SQL_STMT_NUMBER|".getBytes(UTF8_ENCODING)); //new as of 2017-03-29 - serves to free column SQL_STMT_NUMBER for holding the number of the associated select_column_stmt, exists_condition_column or source with_stmt_statement for it to hold the number of the select stmt which owns the column
		output.write("SQL_STMT_NUMBER|".getBytes(UTF8_ENCODING)); //as of 2017-03-29, holds the owner sql statement number to make it a common dimension with the table for statements if the latter is used as a fact table
		output.write("PARENT_SQL_STMT_NUMBER|".getBytes(UTF8_ENCODING));
		output.write("ROOT_PARENT_SQL_STMT_NUMBER|".getBytes(UTF8_ENCODING));
		output.write("OUTPUT_NUMBER|".getBytes(UTF8_ENCODING));
		output.write("LOCAL_OUTPUT_NUMBER|".getBytes(UTF8_ENCODING));
		output.write("DATA_COLUMN_NAME|".getBytes(UTF8_ENCODING));
		output.write("SOURCE_USED_TAB_COL_NUM|".getBytes(UTF8_ENCODING));
		output.write("COLUMN_ALIAS|".getBytes(UTF8_ENCODING));
		output.write("CALCULATION_TYPE|".getBytes(UTF8_ENCODING));
		output.write("LITERAL_TYPE|".getBytes(UTF8_ENCODING));
		output.write("IS_LOGICAL_COL|".getBytes(UTF8_ENCODING));
		output.write("PARENT_USED_TAB_COL_NUM|".getBytes(UTF8_ENCODING));
		output.write("SEQUENCE_NUM|".getBytes(UTF8_ENCODING));
		output.write("ORDER_BY_COLS_OFFSET|".getBytes(UTF8_ENCODING));
		output.write("COL_GROUP|".getBytes(UTF8_ENCODING));
		output.write("COL_TOP_EXPR_GROUP|".getBytes(UTF8_ENCODING));
		output.write("LITERAL_VALUE|".getBytes(UTF8_ENCODING));
		output.write("PARAM_NAME_PREFIX|".getBytes(UTF8_ENCODING)); //ws missing
		output.write("ANALYTIC_FUNCTION_FLG|".getBytes(UTF8_ENCODING)); 
		output.write("WHICH_SEQ_VALUE|".getBytes(UTF8_ENCODING));
		output.write("SOURCE_COL_TYPE_FLG|".getBytes(UTF8_ENCODING));
		output.write("SOURCE_PHYSICAL_DB_OWNER|".getBytes(UTF8_ENCODING));
		output.write("SOURCE_PHYSICAL_DB_TABLE|".getBytes(UTF8_ENCODING));
		output.write("SOURCE_PHYSICAL_COLUMN|".getBytes(UTF8_ENCODING)); 
		output.write("SOURCE_STMT_NUMBER|".getBytes(UTF8_ENCODING));
		output.write("SOURCE_DEPTH|".getBytes(UTF8_ENCODING));
		output.write("CREATED_DT|".getBytes(UTF8_ENCODING));
		output.write("LAST_UPD_DT|".getBytes(UTF8_ENCODING));
		output.write("TAG\n".getBytes(UTF8_ENCODING));
	}

	private void write(SQLColumn col, long fromTableNum, String colGrp, String colTopExprGrp, long prentUSedTabColNum, long sourceUsedTabColNum, int sequenceNumber, int setSelectStmtItemNumber, SQLSelectStmt ownerSelectStmt, SQLSelectStmt rootSelectStmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, OutputStream output, OutputStream usedTabColsOutput, OutputStream tableUsesOutput, java.lang.StringBuffer buffer) throws java.io.IOException {
		if (rootSelectStmt == null && ownerSelectStmt != null) {
			rootSelectStmt = ownerSelectStmt.getRootSelectStmt();
		}
		usedTabColNumberSeq++;
		col.__setEtlRowWid(usedTabColNumberSeq);
		String createdDt = toDateTimeString(new java.util.Date(), buffer);
		if (buffer == null) {
			buffer = new java.lang.StringBuffer(32);
		}
		if (sourceUsedTabColNum < 0) {
			sourceUsedTabColNum = prentUSedTabColNum;
		}
		SQLExprColumn exprCol = col instanceof SQLExprColumn ? (SQLExprColumn)col : null;
		
		usedTabColsOutput.write((folder + "|").getBytes(UTF8_ENCODING)); //FOLDER
		usedTabColsOutput.write((ownerWidget + "|").getBytes(UTF8_ENCODING)); //OWNER_WIDGET
		usedTabColsOutput.write((ownerWidgetType + "|").getBytes(UTF8_ENCODING)); //OWNER_WIDGET_TYPE
		usedTabColsOutput.write((parentWidget + "|").getBytes(UTF8_ENCODING)); //PARENT_WIDGET
		usedTabColsOutput.write((topWidget + "|").getBytes(UTF8_ENCODING)); //TOP_WIDGET
		usedTabColsOutput.write((String.valueOf(usedTabColNumberSeq) + "|").getBytes(UTF8_ENCODING)); //USED_TAB_COL_NUM
		if (fromTableNum > -1) {
			usedTabColsOutput.write((String.valueOf(fromTableNum) + "|").getBytes(UTF8_ENCODING)); //FROM_TABLE_NUM
		}
		else {
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //FROM_TABLE_NUM
		}
		switch(col.getType())
		{
		case SQLWidget.COLUMN_REF: 
			if (col.isRaw()) {
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //DB_OWNER
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //DB_TABLE_NAME
				usedTabColsOutput.write((((SQLRawColumnRef)col).getTableAlias() + "|").getBytes(UTF8_ENCODING)); //TABLE_USE_NAME
			}
			else {
				SQLColumnRef colRef = (SQLColumnRef)col;
				SQLTableRef tblRef = colRef.getTable();
				usedTabColsOutput.write((tblRef.getSchema() + "|").getBytes(UTF8_ENCODING)); //DB_OWNER
				String tableName = tblRef.getTableName();
				usedTabColsOutput.write((tableName + "|").getBytes(UTF8_ENCODING)); //DB_TABLE_NAME
				String tableAlias = tblRef.getAliasName();
				//System.out.println("tableName: " + tableName + ", tableAlias: " + tableAlias + ", colRef.name: " + colRef.name);
				if (tableName.equals(tableAlias)) {
					tableAlias = SQLWidget.EMPTY_STR;
				}
				usedTabColsOutput.write((tableAlias + "|").getBytes(UTF8_ENCODING)); //TABLE_USE_NAME
			}
			break;
		case SQLWidget.SEQUENCE_VALUE_REF: 
			SQLSequenceValueRef seqValREf = (SQLSequenceValueRef)col;
			usedTabColsOutput.write((seqValREf.ownerSchema + "|").getBytes(UTF8_ENCODING)); //DB_OWNER
			usedTabColsOutput.write((seqValREf.sequenceName + "|").getBytes(UTF8_ENCODING)); //DB_TABLE_NAME
			usedTabColsOutput.write((seqValREf.dbLink == null ? "|"  : seqValREf.dbLink + "|").getBytes(UTF8_ENCODING)); //TABLE_USE_NAME
			//
			//*** NO LONGER SURe WHY I SHOULD CREATE A STATEMENT FOR A SEQUENCE_VALUE_REF, DON't SEE THE RATIONALE
			//
			//SQLSequenceValueRef existing = __getSequenceValueRef(seqValREf);
			//if (existing == null) {
			//	writeAsStmt(seqValREf, -1/*parentSetStmtNum*/, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, buffer);
			//	existing = seqValREf;
			//}
			break;
		case SQLWidget.WITH_STMT_COLUMN_REF: 
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //DB_OWNER
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //DB_TABLE_NAME
			usedTabColsOutput.write((((SQLWithStmtColumnRef)col).withStmt.aliasName + "|").getBytes(UTF8_ENCODING)); //TABLE_USE_NAME
			break;
		default:
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //DB_OWNER
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //DB_TABLE_NAME
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //TABLE_USE_NAME
			break;
		}
		if (ownerSelectStmt.parentSelectTbl != null) {
			usedTabColsOutput.write((String.valueOf(ownerSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //SELECT_TABLE_SQL_STMT_NUMBER--relevant when the column references an output column of a table defined as a select statement; for PARAMETER_TABLE_REF, it holds the wid of the record from statement table which represents the PARAMETER_TABLE_REF--relevant when the column references an output column of a table defined as a select statement FOR PARAMETER_TABLE_REF, it holds the wid of the record from statement table which represents the PARAMETER_TABLE_REF
		}
		else {
			SQLTableRef tblREf = col.getTable();
			if (tblREf != null) {
				usedTabColsOutput.write((String.valueOf(tblREf.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //SELECT_TABLE_SQL_STMT_NUMBER
			}
			else {
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SELECT_TABLE_SQL_STMT_NUMBER
			}
		}
		if (col.getType() == SQLWidget.SELECT_STMT_COLUMN) {
			SQLSelectStmtColumn stmtCol = (SQLSelectStmtColumn)col;
			if (colTopExprGrp == null || colTopExprGrp.isEmpty()) {
				colTopExprGrp = col.getDefaultTopExprSubGroup();
			}
			write(stmtCol.selectStmt, stmtCol.etlRowWid/*parentUSedTabColNum*/, sourceUsedTabColNum, -1/*parentSetStmtNum*/, setSelectStmtItemNumber, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, -1/*withStmtItemNumberIfApplicable*/, colGrp, colTopExprGrp, output, usedTabColsOutput, tableUsesOutput, buffer); 
			usedTabColsOutput.write((String.valueOf(stmtCol.selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //COL_SQL_STMT_NUMBER --relevant for columns of kind SELECT_STMT_COLUMN
		}
		else if (col.getType() == SQLWidget.EXISTS_CONDITION_COLUMN) {
			SQLExistsConditionColumn stmtCol = (SQLExistsConditionColumn)col;
			if (colTopExprGrp == null || colTopExprGrp.isEmpty()) {
				colTopExprGrp = col.getDefaultTopExprSubGroup();
			}
			write(stmtCol.selectStmt, stmtCol.etlRowWid/*parentUSedTabColNum*/, sourceUsedTabColNum, -1/*parentSetStmtNum*/, setSelectStmtItemNumber, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, -1/*withStmtItemNumberIfApplicable*/, colGrp, colTopExprGrp, output, usedTabColsOutput, tableUsesOutput, buffer); 
			usedTabColsOutput.write((String.valueOf(stmtCol.selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //COL_SQL_STMT_NUMBER --relevant for columns of kind SELECT_STMT_COLUMN
		}
		else if (col.getType() == SQLWidget.IN_SELECT_COLUMN) {
			SQLInSelectColumn stmtCol = (SQLInSelectColumn)col;
			if (colTopExprGrp == null || colTopExprGrp.isEmpty()) {
				colTopExprGrp = col.getDefaultTopExprSubGroup();
			}
			write(stmtCol.selectStmt, stmtCol.etlRowWid/*parentUSedTabColNum*/, sourceUsedTabColNum, -1/*parentSetStmtNum*/, setSelectStmtItemNumber, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, -1/*withStmtItemNumberIfApplicable*/, colGrp, colTopExprGrp, output, usedTabColsOutput, tableUsesOutput, buffer); 
			usedTabColsOutput.write((String.valueOf(stmtCol.selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //COL_SQL_STMT_NUMBER --relevant for columns of kind SELECT_STMT_COLUMN
		}
		else if (col.getType() == SQLWidget.WITH_STMT_COLUMN_REF) {
			usedTabColsOutput.write((String.valueOf(((SQLWithStmtColumnRef)col).withStmt.withStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //COL_SQL_STMT_NUMBER --relevant for columns of kind WITH_STMT_COLUMN_REF
		}
		else {
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //COL_SQL_STMT_NUMBER --relevant for columns of kind SELECT_STMT_COLUMN
		}
		if (ownerSelectStmt != null) {
			usedTabColsOutput.write((String.valueOf(ownerSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //SQL_STMT_NUMBER
		}
		else {
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SQL_STMT_NUMBER
		}
		SQLSelectStmt parentSelectStmt = null;
		if (ownerSelectStmt != null) {
			if (ownerSelectStmt.parentSelectStmt != null) {
				parentSelectStmt = ownerSelectStmt.parentSelectStmt;
			}
			else if (ownerSelectStmt.parentSelectTbl != null) {
				parentSelectStmt = ownerSelectStmt.parentSelectTbl.parentSelectStmt;
			}
		}
		if (parentSelectStmt != null) {
			usedTabColsOutput.write((String.valueOf(parentSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		else {
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		if (rootSelectStmt != null) {
			usedTabColsOutput.write((String.valueOf(rootSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUMBER
		}
		else {
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUMBER
		}
		usedTabColsOutput.write((String.valueOf(col.outputNumber) + "|").getBytes(UTF8_ENCODING)); //OUTPUT_NUMBER
		if (col.localOutputNumber < 0) {
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //LOCAL_OUTPUT_NUMBER
		}
		else {
			usedTabColsOutput.write((String.valueOf(col.localOutputNumber) + "|").getBytes(UTF8_ENCODING)); //LOCAL_OUTPUT_NUMBER
		}
		switch(col.getType())
		{
		case SQLWidget.COLUMN_REF: 
			usedTabColsOutput.write((col.getName() + "|").getBytes(UTF8_ENCODING)); //DATA_COLUMN_NAME
			break ;
		case SQLWidget.WITH_STMT_COLUMN_REF: 
			SQLWithStmtColumnRef withStmtColRef = (SQLWithStmtColumnRef)col;
			if (col.getLogicalColumnType() != 'Y') { //OLD: if (!withStmtColRef.withStmtColumn.isLogicalColumn()) {
				usedTabColsOutput.write((withStmtColRef.withStmtColumn.getName() + "|").getBytes(UTF8_ENCODING)); //DATA_COLUMN_NAME
			}
			else {
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //DATA_COLUMN_NAME
			}
			break;
		default:
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //DATA_COLUMN_NAME
			break ;
		}
		usedTabColsOutput.write((String.valueOf(sourceUsedTabColNum) + "|").getBytes(UTF8_ENCODING)); //SOURCE_USED_TAB_COL_NUM
		usedTabColsOutput.write((col.aliasName + "|").getBytes(UTF8_ENCODING)); //COLUMN_ALIAS
		usedTabColsOutput.write((SQLWidget.getTypeCode(col.getType()) + "|").getBytes(UTF8_ENCODING)); //CALCULATION_TYPE
		
		physicalColumnSource.physicalColumnLvl = 0;
		byte chkPhysicalSrcRslt = col.getPhysicalColumnSource(physicalColumnSource);
		SQLColumn physicalColumn = chkPhysicalSrcRslt > (byte)0 ? physicalColumnSource.physicalColumn : null; //col.getPhysicalColumn();
		
		switch(col.getType())
		{
		case SQLWidget.CONSTANT: 
			usedTabColsOutput.write((SQLLiteral.getLiteralTypeCode(((SQLLiteral)col).literalType) + "|").getBytes(UTF8_ENCODING)); //LITERAL_TYPE
			break ;
		case SQLWidget.COLUMN_REF: 
			if (physicalColumn != null && physicalColumn.getType() == SQLWidget.CONSTANT) {
				usedTabColsOutput.write((SQLLiteral.getLiteralTypeCode(((SQLLiteral)physicalColumn).literalType) + "|").getBytes(UTF8_ENCODING)); //LITERAL_TYPE
			}
			else {
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //LITERAL_TYPE
			}
			break ;
		case SQLWidget.WITH_STMT_COLUMN_REF: 
			if (physicalColumn != null && physicalColumn.getType() == SQLWidget.CONSTANT) {
				usedTabColsOutput.write((SQLLiteral.getLiteralTypeCode(((SQLLiteral)physicalColumn).literalType) + "|").getBytes(UTF8_ENCODING)); //LITERAL_TYPE
			}
			else {
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //LITERAL_TYPE
			}
			break ;
		default:
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //LITERAL_TYPE
			break ;
		}
		usedTabColsOutput.write((col.getLogicalColumnType() + "|").getBytes(UTF8_ENCODING)); //OLD: usedTabColsOutput.write((col.isLogicalColumn() ? "Y|" : "N|").getBytes(UTF8_ENCODING)); //IS_LOGICAL_COL
		if (prentUSedTabColNum < 0) {
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //PARENT_USED_TAB_COL_NUM
		}
		else {
			usedTabColsOutput.write((String.valueOf(prentUSedTabColNum) + "|").getBytes(UTF8_ENCODING)); //PARENT_USED_TAB_COL_NUM
		}
		usedTabColsOutput.write((String.valueOf(sequenceNumber) + "|").getBytes(UTF8_ENCODING)); //SEQUENCE_NUM
		switch(col.getType()) {
		case SQLWidget.FUNC_ANALYTIC_CLAUSE: 
			usedTabColsOutput.write((String.valueOf(((SQLFunctionAnalyticClause)col).orderByColsOffset) + "|").getBytes(UTF8_ENCODING)); //ORDER_BY_COLS_OFFSET
			break;
		case SQLWidget.PARAMETER_COLUMN: 
			if (col.isSQLIndexedParameterColumn()) {
				usedTabColsOutput.write((String.valueOf(col.asSQLIndexedParameterColumn().index) + "|").getBytes(UTF8_ENCODING)); //ORDER_BY_COLS_OFFSET
			}
			else {
				usedTabColsOutput.write((String.valueOf(col.asSQLParameterColumn().parametirizedParts) + "|").getBytes(UTF8_ENCODING)); //ORDER_BY_COLS_OFFSET
			}
			break;
		default: 
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //ORDER_BY_COLS_OFFSET
		}
		if (colGrp != null && !colGrp.isEmpty()) {
			usedTabColsOutput.write((colGrp + "|").getBytes(UTF8_ENCODING)); //COL_GROUP
		}
		else {
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //COL_GROUP
		}
		if (colTopExprGrp != null && !colTopExprGrp.isEmpty()) {
			usedTabColsOutput.write((colTopExprGrp + "|").getBytes(UTF8_ENCODING)); //COL_TOP_EXPR_GROUP
		}
		else {
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //COL_TOP_EXPR_GROUP
		}
		switch(col.getType())
		{
		case SQLWidget.CONSTANT: 
			usedTabColsOutput.write((col.asSQLLiteral()/*OLD: ((SQLLiteral)col)*/.value + "|").getBytes(UTF8_ENCODING)); //LITERAL_VALUE
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //PARAM_NAME_PREFIX
			if (col.asSQLLiteral().isWorkedOutFromParam) {
				//'P' for worked ut from Parameter
				usedTabColsOutput.write("P|".getBytes(UTF8_ENCODING)); //ANALYTIC_FUNCTION_FLG
			}
			else {
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //ANALYTIC_FUNCTION_FLG
			}
			break;
		case SQLWidget.PARAMETER_COLUMN:
			usedTabColsOutput.write((col.getName() + "|").getBytes(UTF8_ENCODING)); //LITERAL_VALUE
			usedTabColsOutput.write((((SQLParameterColumn)col).ownerSchema + "|").getBytes(UTF8_ENCODING)); //PARAM_NAME_PREFIX
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //ANALYTIC_FUNCTION_FLG
			break; 
		case SQLWidget.FUNC_EXPR: 
			usedTabColsOutput.write((col.getName() + "|").getBytes(UTF8_ENCODING)); //LITERAL_VALUE
			usedTabColsOutput.write((((SQLFuncExprColumn)col).ownerSchema + "|").getBytes(UTF8_ENCODING)); //PARAM_NAME_PREFIX
			usedTabColsOutput.write((((SQLFuncExprColumn)col).analyticClause != null ? "Y|" : "N|").getBytes(UTF8_ENCODING)); //ANALYTIC_FUNCTION_FLG
			break; 
		case SQLWidget.PSEUDO_COLUMN_REF: 
			usedTabColsOutput.write((col.getName() + "|").getBytes(UTF8_ENCODING)); //LITERAL_VALUE
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //PARAM_NAME_PREFIX
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //ANALYTIC_FUNCTION_FLG
			break; 
		case SQLWidget.WITH_STMT_COLUMN_REF: 
			//OLD NOW COPIED INTO DATA_TABLE_USE_NAME: usedTabColsOutput.write((((SQLWithStmtColumnRef)col).withStmt.aliasName + "|").getBytes(UTF8_ENCODING)); //LITERAL_VALUE
			if (physicalColumn != null) {
				if (physicalColumn.getType() == SQLWidget.CONSTANT) {
					usedTabColsOutput.write((((SQLLiteral)physicalColumn).value + "|").getBytes(UTF8_ENCODING)); //LITERAL_VALUE
				}
				else if (physicalColumn.getType() == SQLWidget.PSEUDO_COLUMN_REF) {
					usedTabColsOutput.write((physicalColumn.getName() + "|").getBytes(UTF8_ENCODING)); //LITERAL_VALUE
				}
				else {
					usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //LITERAL_VALUE
				}
			}
			else {
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //LITERAL_VALUE
			}
			usedTabColsOutput.write((col.getName() + "|").getBytes(UTF8_ENCODING)); //PARAM_NAME_PREFIX
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //ANALYTIC_FUNCTION_FLG
			break; 
		case SQLWidget.COLUMN_REF: 
			if (physicalColumn != null) {
				if (physicalColumn.getType() == SQLWidget.CONSTANT) {
					usedTabColsOutput.write((((SQLLiteral)physicalColumn).value + "|").getBytes(UTF8_ENCODING)); //LITERAL_VALUE
				}
				else if (physicalColumn.getType() == SQLWidget.PSEUDO_COLUMN_REF) {
					usedTabColsOutput.write((physicalColumn.getName() + "|").getBytes(UTF8_ENCODING)); //LITERAL_VALUE
				}
				else {
					usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //LITERAL_VALUE
				}
			}
			else {
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //LITERAL_VALUE
			}
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //PARAM_NAME_PREFIX
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //ANALYTIC_FUNCTION_FLG
			break ;
		default : 
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //LITERAL_VALUE
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //PARAM_NAME_PREFIX
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //ANALYTIC_FUNCTION_FLG
		} 
		if (col.getType() == SQLWidget.SEQUENCE_VALUE_REF) {
			SQLSequenceValueRef seqValRef = (SQLSequenceValueRef)col;
			usedTabColsOutput.write((seqValRef.whichSeqValue.equalsIgnoreCase(SQLSequenceValueRef.NEXTVAL_SEQ_VALUE_NAME) ? "N|" : 
							seqValRef.whichSeqValue.equalsIgnoreCase(SQLSequenceValueRef.CURRVAL_SEQ_VALUE_NAME) ? "C|" : seqValRef.whichSeqValue + "|").getBytes(UTF8_ENCODING)); //WHICH_SEQ_VALUE
		}
		else {
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //WHICH_SEQ_VALUE
		}
		
		switch(col.getLogicalColumnType())
		{
		case 'T':
		case 'W':
			//SQLColumn physicalColumn = col.getPhysicalColumn(); //moved up for it to be used the output the literal value
			if (physicalColumn != null) {
				//System.out.println("EtlSQLAnalyzeWizard - physicalColumn.class: " + physicalColumn.getClass().getName() + " col.getName(): " + col.getName());
				SQLTableRef physicalTbl = physicalColumn.getTable();
				switch(physicalColumn.getType())
				{
				case ISQLWidgetConstants.CONSTANT:
				case ISQLWidgetConstants.PSEUDO_COLUMN_REF:
					usedTabColsOutput.write(((physicalColumn.getType() == ISQLWidgetConstants.CONSTANT ? 'L' : 'P') + "|").getBytes(UTF8_ENCODING)); //SOURCE_COL_TYPE_FLG
					usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_DB_OWNER
					usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_DB_TABLE
					usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_COLUMN
					if (physicalColumnSource.sourceStmt != null) {
						usedTabColsOutput.write((physicalColumnSource.sourceStmt.etlRowWid + "|").getBytes(UTF8_ENCODING)); //SOURCE_STMT_NUMBER
					}
					else {
						usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_STMT_NUMBER
					}
					usedTabColsOutput.write((physicalColumnSource.physicalColumnLvl + "|").getBytes(UTF8_ENCODING)); //SOURCE_DEPTH
					break ;
				default:
					usedTabColsOutput.write("S|".getBytes(UTF8_ENCODING)); //SOURCE_COL_TYPE_FLG
					usedTabColsOutput.write((physicalTbl.getSchema() + "|").getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_DB_OWNER
					usedTabColsOutput.write((physicalTbl.getTableName() + "|").getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_DB_TABLE
					usedTabColsOutput.write((physicalColumn.getName() + "|").getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_COLUMN
					if (physicalColumnSource.sourceStmt != null) {
						usedTabColsOutput.write((physicalColumnSource.sourceStmt.etlRowWid + "|").getBytes(UTF8_ENCODING)); //SOURCE_STMT_NUMBER
					}
					else {
						usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_STMT_NUMBER
					}
					usedTabColsOutput.write((physicalColumnSource.physicalColumnLvl + "|").getBytes(UTF8_ENCODING)); //SOURCE_DEPTH
					break;
				}
			}
			else {
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_COL_TYPE_FLG
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_DB_OWNER
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_DB_TABLE
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_COLUMN
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_STMT_NUMBER
				usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_DEPTH
			}
			break; 
		default:
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_COL_TYPE_FLG
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_DB_OWNER
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_DB_TABLE
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_PHYSICAL_COLUMN
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_STMT_NUMBER
			usedTabColsOutput.write("|".getBytes(UTF8_ENCODING)); //SOURCE_DEPTH
			break;
		}
		
		usedTabColsOutput.write((createdDt + "|").getBytes(UTF8_ENCODING)); //CREATED_DT
		usedTabColsOutput.write((createdDt + "|").getBytes(UTF8_ENCODING)); //LAST_UPD_DT
		usedTabColsOutput.write((tag + "\n").getBytes(UTF8_ENCODING)); //TAG
		
		
		if (exprCol != null) {
			for (int i=0;i<exprCol.involvedColumnsCount;i++)
			{
				SQLColumn involvedCol = exprCol.involvedColumns[i];
				SQLTableRef tblREf = involvedCol.getTable();
				if (tblREf == null) {
					if (involvedCol instanceof SQLRawColumnRef) {
						SQLRawColumnRef rawCol = (SQLRawColumnRef)involvedCol;
						if (rawCol.tableAlias.isEmpty()) {
							if (ownerSelectStmt.fromTablesCount == 1) {
								tblREf = ownerSelectStmt.fromTables[0];
								involvedCol = new SQLColumnRef(tblREf, rawCol.name, involvedCol.aliasName);
								exprCol.involvedColumns[i] = involvedCol;
							}
						}
						else {
							tblREf = ownerSelectStmt.getTableByAliasExt(rawCol.tableAlias);
							if (tblREf != null) {
								involvedCol = new SQLColumnRef(tblREf, rawCol.name, involvedCol.aliasName);
								exprCol.involvedColumns[i] = involvedCol;
							}
						}
					}
				}
				if (colTopExprGrp == null) {
					colTopExprGrp = exprCol.getDefaultTopExprSubGroup();
				}
				write(exprCol.involvedColumns[i], (tblREf != null ? tblREf.etlRowWid : -1)/*fromTableNum*/ , colGrp, colTopExprGrp, exprCol.etlRowWid/*prentUSedTabColNum*/, sourceUsedTabColNum, (i + 1)/*sequenceNumber*/, setSelectStmtItemNumber, ownerSelectStmt, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput, tableUsesOutput, buffer); 
			}
		}
		
	}
	
	private final void writeStatement(SQLSelectStmt selectStmt, long parentSetStmtNum, int setSelectStmtItemNumber, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, int withStmtItemNumberIfApplicable, OutputStream output, OutputStream usedTabColsOutput, OutputStream tableUsesOutput, java.lang.StringBuffer buffer) throws java.io.IOException {
		write(selectStmt, -1/*parentUSedTabColNum*/, -1/*sourceUsedTabColNum*/, parentSetStmtNum, setSelectStmtItemNumber, null/*rootSelectStmt*/, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, withStmtItemNumberIfApplicable, null/*colGrp*/, null/*colTopExprGrp*/, output, usedTabColsOutput, tableUsesOutput, buffer);
	}
	
	public void writeStatement(SQLSelectStmt selectStmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, int withStmtItemNumberIfApplicable, OutputStream output, OutputStream usedTabColsOutput, OutputStream tableUsesOutput, java.lang.StringBuffer buffer) throws java.io.IOException {
		writeStatement(selectStmt, -1/*parentSetStmtNum*/, -1/*setSelectStmtItemNumber*/, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, withStmtItemNumberIfApplicable, output, usedTabColsOutput, tableUsesOutput, buffer);
	}
	
	public void writeStatement(SQLCombiningStatement stmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, OutputStream output, OutputStream usedTabColsOutput, OutputStream tableUsesOutput, java.lang.StringBuffer buffer) throws java.io.IOException {
		if (stmt.itemsCount < 1) return;
		SQLStatement/*SQLSelectStmt*/ parentSetStmt = stmt.items[0];
		if (parentSetStmt.isSQLSelectStmt()) {
			writeStatement(parentSetStmt.asSQLSelectStmt()/*selectStmt*/, SELF_PARENT_SET_NUMBER/*parentSetStmtNum*/, 1/*setSelectStmtItemNumber*/, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, -1/*withStmtItemNumberIfApplicable*/, output, usedTabColsOutput, tableUsesOutput, buffer); 
		}
		else {
			__writeAsCombiningStatement(parentSetStmt.asSQLCombiningStatement().parentSelectTbl, SELF_PARENT_SET_NUMBER/*parentSetStmtNum*/, 1/*setSelectStmtItemNumber*/, stmt/*rootSelectStmt*/, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput,tableUsesOutput, buffer);
		}
		for (int i=1;i<stmt.itemsCount;i++)
		{
			SQLStatement/*SQLSelectStmt*/ itm = stmt.items[i];
			if (itm.isSQLSelectStmt()) {
				writeStatement(itm.asSQLSelectStmt()/*selectStmt*/, parentSetStmt.etlRowWid/*parentSetStmtNum*/, (i + 1)/*setSelectStmtItemNumber*/, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, -1/*withStmtItemNumberIfApplicable*/, output, usedTabColsOutput, tableUsesOutput, buffer); 
			}
			else {
				__writeAsCombiningStatement(itm.asSQLCombiningStatement().parentSelectTbl, parentSetStmt.etlRowWid/*parentSetStmtNum*/, (i + 1)/*setSelectStmtItemNumber*/, stmt/*rootSelectStmt*/, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput,tableUsesOutput, buffer);
			}
		}
	}
	
	private final void __writeAsCombiningStatement(SQLCombiningSelectTable combiningSelectTbl, final long parentSetStmtNum, final int setSelectStmtItemNumber, final SQLStatement/*SQLSelectStmt*/ rootSelectStmt, final String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, OutputStream output, OutputStream usedTabColsOutput, OutputStream tableUsesOutput, java.lang.StringBuffer buffer) throws java.io.IOException {
		stmtNumberSeq++;
		combiningSelectTbl.etlRowWid = stmtNumberSeq;
		
		if (buffer == null) {
			buffer = new java.lang.StringBuffer(32);
		}
		String createdDt = toDateTimeString(new java.util.Date(), buffer);
		
		output.write((folder + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidget + "|").getBytes(UTF8_ENCODING));
		output.write((ownerWidgetType + "|").getBytes(UTF8_ENCODING));
		output.write((parentWidget + "|").getBytes(UTF8_ENCODING)); //PARENT_WIDGET
		output.write((topWidget + "|").getBytes(UTF8_ENCODING)); //TOP_WIDGET
		output.write((String.valueOf(combiningSelectTbl.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //STMT_NUMBER
		output.write("|".getBytes(UTF8_ENCODING)); //STMT_NAME
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_STMT_FLG
		output.write((String.valueOf(SQLTableRef.COMBINING_SELECT_TABLE)/*sequenceNumber*/ + "|").getBytes(UTF8_ENCODING)); //WITH_STMT_NUMBER
		output.write((combiningSelectTbl.name + "|").getBytes(UTF8_ENCODING)); //NESTED_TABLE_NAME
		SQLStatement firstItem = combiningSelectTbl.selectStmt.items[0];
		byte operType = firstItem.isSQLCombiningStatement() ? firstItem.asSQLCombiningStatement().__getAssocOperator() : firstItem.asSQLSelectStmt().getAssocOperator();
		if (operType > (byte)-1) {
			output.write((SQLSelectStmt.getOperatorCode(operType) + "|").getBytes(UTF8_ENCODING)); //SET_OPERATION_TYPE
		}
		else {
			output.write(("|").getBytes(UTF8_ENCODING)); //SET_OPERATION_TYPE 
		}
		output.write((String.valueOf(setSelectStmtItemNumber) + "|").getBytes(UTF8_ENCODING)); //SET_STMT_ITEM_NUMBER
		if (combiningSelectTbl.parentSelectStmt != null) {
			output.write((String.valueOf(combiningSelectTbl.parentSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SQL_STMT_NUMBER
		}
		if (rootSelectStmt != null) {
			output.write((String.valueOf(rootSelectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //ROOT_PARENT_SQL_STMT_NUM
		}
		if (parentSetStmtNum > 0) {
			output.write((String.valueOf(parentSetStmtNum) + "|").getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARENT_SET_SQL_STMT_NUM
		}
		if (combiningSelectTbl.name != null) {
			output.write((combiningSelectTbl.name + "|").getBytes(UTF8_ENCODING)); //PARAM_TBL_USE_NAME
		}
		else {
			output.write("|".getBytes(UTF8_ENCODING)); //PARAM_TBL_USE_NAME
		}
		output.write("|".getBytes(UTF8_ENCODING)); //DESCRIPTION
		output.write("|".getBytes(UTF8_ENCODING)); //DISTINCT_FLG
		output.write((String.valueOf(combiningSelectTbl.selectStmt.getDrivingStmt().outputColumnsCount) + "|").getBytes(UTF8_ENCODING)); //OUTPUT_COLUMNS
		output.write("|".getBytes(UTF8_ENCODING)); //WITH_INFA_JOINS_FLG
		output.write((String.valueOf(combiningSelectTbl.selectStmt.etlRowWid) + "|").getBytes(UTF8_ENCODING)); //LIMIT_VALUE
		output.write("|".getBytes(UTF8_ENCODING)); //LIMIT_OFFSET
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N
		output.write("|".getBytes(UTF8_ENCODING)); //TOP_N_PERCENT_FLG
		output.write("|".getBytes(UTF8_ENCODING)); //HINT_TEXT
		output.write((tag + "|").getBytes(UTF8_ENCODING)); //TAG
		output.write((createdDt + "|").getBytes(UTF8_ENCODING)); //CREATED_DT
		output.write((createdDt + "\n").getBytes(UTF8_ENCODING)); //LAST_UPD_DT
		
		
		for (int i=0;i<combiningSelectTbl.selectStmt.itemsCount;i++)
		{
			SQLStatement/*SQLSelectStmt*/ itm = combiningSelectTbl.selectStmt.items[i];
			if (itm.isSQLSelectStmt()) {
				writeStatement(itm.asSQLSelectStmt()/*selectStmt*/, combiningSelectTbl.etlRowWid/*parentSetStmtNum*/, (i + 1)/*setSelectStmtItemNumber*/, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, -1/*withStmtItemNumberIfApplicable*/, output, usedTabColsOutput, tableUsesOutput, buffer); 
			}
			else {
				__writeAsCombiningStatement(itm.asSQLCombiningStatement().parentSelectTbl, combiningSelectTbl.etlRowWid/*parentSetStmtNum*/, (i + 1)/*setSelectStmtItemNumber*/, rootSelectStmt, folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput,tableUsesOutput, buffer);
			}
		}
		
	}
	
	public void writeStatement(SQLStatement stmt, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, OutputStream output, OutputStream usedTabColsOutput, OutputStream tableUsesOutput, java.lang.StringBuffer buffer) throws java.io.IOException {
		//System.out.println("IN writeStatement");
		if (stmt.isSQLSelectStmt()) {
			writeStatement(stmt.asSQLSelectStmt(), folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, -1/*withStmtItemNumberIfApplicable*/, output, usedTabColsOutput, tableUsesOutput, buffer);
			return ;
		}
		writeStatement(stmt.asSQLCombiningStatement(), folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput, tableUsesOutput, buffer);
	}
	
	public void writeStatement(String stmtSqlText, String folder, String ownerWidget, String ownerWidgetType, String parentWidget, String topWidget, String tag, OutputStream output, OutputStream usedTabColsOutput, OutputStream tableUsesOutput, java.lang.StringBuffer buffer) throws java.io.IOException {
		writeStatement(SQLSelectStmtParseUtil.parseSQLStatement(stmtSqlText/*sqlText*/), folder, ownerWidget, ownerWidgetType, parentWidget, topWidget, tag, output, usedTabColsOutput, tableUsesOutput, buffer);
	}
	
	
	private static final void dump_to_flat_file(String filePath, String folder, String widgetName, String widgetType, String parentWidget, String topWidget, String tagFingerprint, String outputDir, boolean printHierDumpToConsole) {
		java.io.OutputStream tblUseOutputStream = null;
		java.io.OutputStream stmtOutputStream = null;
		java.io.OutputStream tblUsedColOutputStream = null;
		
		if (outputDir.charAt(outputDir.length() - 1) != java.io.File.separatorChar) {
			outputDir = outputDir + java.io.File.separatorChar;
		}
		String outputFile = outputDir + "WC_ETL_SQL_STMT.psv";
		
		try 
		{
			stmtOutputStream = new java.io.BufferedOutputStream(new FileOutputStream(outputFile));
			writeWC_ETL_SQL_STMTHeader(stmtOutputStream);
		}
		catch(Exception ex)
		{
			System.out.println("Error: " + ex.getMessage());
			return ;
		}
		outputFile = outputDir + "WC_ETL_STMT_USED_TAB_COL.psv";
		try 
		{
			tblUsedColOutputStream = new java.io.BufferedOutputStream(new FileOutputStream(outputFile));
			writeWC_ETL_STMT_USED_TAB_COLHeader(tblUsedColOutputStream);
		}
		catch(Exception ex)
		{
			System.out.println("Error: " + ex.getMessage());
			return ;
		}
		outputFile = outputDir + "WC_ETL_DATA_TABLE_USE.psv";
		try 
		{
			tblUseOutputStream = new java.io.BufferedOutputStream(new FileOutputStream(outputFile));
			writeWC_ETL_DATA_TABLE_USEHeader(tblUseOutputStream);
		}
		catch(Exception ex)
		{
			System.out.println("Error: " + ex.getMessage());
			return ;
		}
		
		//writeWC_ETL_DATA_TABLE_USEHeader
		//writeWC_ETL_SQL_STMTHeader
		//writeWC_ETL_STMT_USED_TAB_COLHeader
		
		SQLStatement stmt = SQLUtil.parseSQLStatementFromFile(filePath);
		if (printHierDumpToConsole) {
			System.out.println(stmt);
		}
		EtlSQLAnalyzeWizard wizard = new EtlSQLAnalyzeWizard();
		try
		{
			wizard.writeStatement(stmt, folder, widgetName, widgetType, parentWidget, topWidget, tagFingerprint, 
							stmtOutputStream, tblUsedColOutputStream, tblUseOutputStream, null);
			stmtOutputStream.close(); tblUsedColOutputStream.close(); tblUseOutputStream.close(); 
		}
		catch(Exception e)
		{
			System.out.println("Error: " + e.getMessage());
			e.printStackTrace();
			return ;
		}
		finally
		{
			try {stmtOutputStream.close(); tblUsedColOutputStream.close(); tblUseOutputStream.close(); } catch(Exception ex) {}
		}
		
	}
	
	static final String SQL_QUERY_WIDG_TYPE = "Sql Query";
	static final String SQL_QUERY_WIDG_TYPE_CD = "SQL_QUERY";
	static final String LOOKUP_SQL_OVERRIDE_WIDG_TYPE = "Lookup Sql Override";
	static final String LOOKUP_SQL_OVERRIDE_WIDG_TYPE_CD = "LOOKUP_SQL_OVERRIDE";
	static final String SRC_QUAL_WIDG_TYPE = "Source Qualifier";
	static final String SRC_QUAL_WIDG_TYPE_CD = "SOURCE_QUALIFIER";
	static final String LKP_PROCEDURE_WIDG_TYPE = "Lookup Procedure";
	static final String LKP_PROCEDURE_WIDG_TYPE_CD = "LOOKUP_PROCEDURE";
	
	private static final String ensureWidgetTypeCode(String widgetType) {
		if (SQL_QUERY_WIDG_TYPE.equals(widgetType)) {
			return SQL_QUERY_WIDG_TYPE_CD;
		}
		else if (LOOKUP_SQL_OVERRIDE_WIDG_TYPE.equals(widgetType)) {
			return LOOKUP_SQL_OVERRIDE_WIDG_TYPE_CD;
		}
		else if (SRC_QUAL_WIDG_TYPE.equals(widgetType)) {
			return SRC_QUAL_WIDG_TYPE_CD;
		}
		else if (LKP_PROCEDURE_WIDG_TYPE.equals(widgetType)) {
			return LKP_PROCEDURE_WIDG_TYPE_CD;
		}
		return widgetType;
	}
	
	private final String makeTag() {
		startDt = toDateTimeString(new java.util.Date());
		String tag = config.TAG_PREFIX + '-' + config.INFAREP_FOLDERS.replace(';', ':') + '-';
		int diff = 150 - tag.length() - startDt.length();
		if (diff < 0) {
			diff *= -1;
			diff++;
			return tag.substring(0, 150 - diff) + '-' + startDt;
		}
		return tag + startDt;
	}
	
	private final void createTag() throws java.sql.SQLException {
		CallableStatement cstmt = getBAW_DBCONN().prepareCall("{? = call BI_APPS.BBB_CREATE_SQL_CHG_CHECK_TAG(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}");
		cstmt.registerOutParameter(1, java.sql.Types.BIGINT);
		tagFingerprint = makeTag();
		cstmt.setString(2, tagFingerprint); //IN_FINGERPRINT
		cstmt.setString(3, "tag for sql analyze for folders " + config.INFAREP_FOLDERS.replace(';', ',')); //IN_DESCRIPTION
		cstmt.setString(4, startDt); //IN_START_DT
		cstmt.setString(5, startDt); //IN_CREATED_DT
		cstmt.setString(6, "Y"); //IN_MOST_RECENT_FLG
		cstmt.setString(7, "Y"); //IN_INPROGRESS_STRAIGHT_FLG
		String[] items = config.INFAREP_FOLDERS.split(";");
		StringBuilder buf = new StringBuilder("'" + items[0].trim() + "'");
		for (int i=1;i<items.length;i++)
		{
			buf.append(", '" + items[i].trim() + "'");
		}
		cstmt.setString(8, buf.toString()); //IN_FOLDERS
		cstmt.setString(9, config.DEFAULT_DB_OWNER); //IN_DEFAULT_DB_OWNER
		
		items = config.EXCLUDED_TABLE_OWNERS.split(";");
		buf.setLength(0);
		buf.append("'" + items[0].trim() + "'");
		for (int i=1;i<items.length;i++)
		{
			buf.append(", '" + items[i].trim() + "'");
		}
		cstmt.setString(10, buf.toString()); //IN_EXCLUDED_TABLE_OWNERS
		
		cstmt.setString(11, config.TRGT_DB_OWNER); //IN_TRGT_DB_OWNER
		cstmt.setString(12, config.OWNER_APP); //IN_OWNER_APP
		cstmt.setString(13, config.TRGT_OWNER_APP); //IN_TRGT_OWNER_APP
		cstmt.setString(14, config.INFAREP_TEST_MAPPING_FILTER); //IN_MAPPINGS_FILTER
		
		cstmt.executeUpdate();
		tagWid = cstmt.getLong(1);
	}
	
	private final void markTagAsToBeLoadedIntoBAW() throws java.sql.SQLException {
		Statement stmt = getBAW_DBCONN().createStatement(); 
		//BUG-FIX - 2017-05-09 - was using tableUseNumberSeq to update column NUMBER_OF_USED_TAB_COLS
		stmt.executeUpdate("UPDATE BI_APPS.WC_ETL_SQL_CHG_CHECK_TAG SET NUMBER_OF_SQL_STMTS=" + stmtNumberSeq + ", NUMBER_OF_USED_TAB_COLS=" + usedTabColNumberSeq + ", STATUS='TO_CONTINUE_IN_INFA', TO_CONTINUE_IN_INFA_DT=TO_DATE(" + toDateTimeString(new java.util.Date()) + ", 'YYYYMMDDHH24MISS') WHERE ROW_WID=" + tagWid);
	}
	
	static final String GET_SQL_SELECT_STMTS_TMPLT = "SELECT TRIE as WIDGET_KEY, folder FOLDER, mapping_name_father TOP_WIDGET_NAME, mapping_name PARENT_WIDGET_NAME, instance_name WIDGET_NAME, object_type_name WIDGET_TYPE_NAME, line_no LINE_NO, SQL_LINE_TEXT FROM <BI_INFOREP>.V_BBB_INFA_SQL_QUERIES <WHERE_CLAUSE>";
	
	private final void doWork() {
		try 
		{
			createTag();
		}
		catch(java.sql.SQLException ex)
		{
			doLogTrace("Failed to start EtlSQLAnalyzeWizard process", SEVERE, THROWN_ERROR_FIELD_NAME, ex.getMessage());
			throw new RuntimeException(ex.getMessage() + ISQLWidgetConstants.LN_TERMINATOR + 
			"Failed to start EtlSQLAnalyzeWizard process"
			, ex
			);
		}
		
		doLogTrace("EtlSQLAnalyzeWizard - dwStart", INFO, TAG_FIELD_NAME, tagFingerprint);
		if (config.SQL_LOGGING_ON) {
			doLogSQLTrace("EtlSQLAnalyzeWizard - dwStart", INFO, TAG_FIELD_NAME, tagFingerprint);
		}
		
		java.io.OutputStream tblUseOutputStream = null;
		java.io.OutputStream stmtOutputStream = null;
		java.io.OutputStream tblUsedColOutputStream = null;
		java.io.OutputStream errorStream = null;
		
		String outputDir = config.OUTPUT_DIR;
		if (outputDir.charAt(outputDir.length() - 1) != java.io.File.separatorChar) {
			outputDir = outputDir + java.io.File.separatorChar;
		}
		String outputFile = outputDir + "WC_ETL_SQL_STMT.psv";
		
		try 
		{
			stmtOutputStream = new java.io.BufferedOutputStream(new FileOutputStream(outputFile));
			writeWC_ETL_SQL_STMTHeader(stmtOutputStream);
		}
		catch(Exception ex)
		{
			System.out.println("Error: " + ex.getMessage());
			doLogTrace("EtlSQLAnalyzeWizard - dwEnd", SEVERE, TAG_FIELD_NAME, tagFingerprint, THROWN_ERROR_FIELD_NAME, ex.getMessage());
			return ;
		}
		outputFile = outputDir + "WC_ETL_STMT_USED_TAB_COL.psv";
		try 
		{
			tblUsedColOutputStream = new java.io.BufferedOutputStream(new FileOutputStream(outputFile));
			writeWC_ETL_STMT_USED_TAB_COLHeader(tblUsedColOutputStream);
		}
		catch(Exception ex)
		{
			System.out.println("Error: " + ex.getMessage());
			doLogTrace("EtlSQLAnalyzeWizard - dwEnd", SEVERE, TAG_FIELD_NAME, tagFingerprint, THROWN_ERROR_FIELD_NAME, ex.getMessage());
			return ;
		}
		outputFile = outputDir + "WC_ETL_DATA_TABLE_USE.psv";
		try 
		{
			tblUseOutputStream = new java.io.BufferedOutputStream(new FileOutputStream(outputFile));
			writeWC_ETL_DATA_TABLE_USEHeader(tblUseOutputStream);
		}
		catch(Exception ex)
		{
			System.out.println("Error: " + ex.getMessage());
			doLogTrace("EtlSQLAnalyzeWizard - dwEnd", SEVERE, TAG_FIELD_NAME, tagFingerprint, THROWN_ERROR_FIELD_NAME, ex.getMessage());
			return ;
		}
		outputFile = outputDir + "WC_ETL_SQL_ANALYZE_ERR.psv";
		try 
		{
			errorStream = new java.io.BufferedOutputStream(new FileOutputStream(outputFile));
			writeWC_ETL_SQL_ANALYZE_ERRHeader(errorStream);
		}
		catch(Exception ex)
		{
			System.out.println("Error: " + ex.getMessage());
			doLogTrace("EtlSQLAnalyzeWizard - dwEnd", SEVERE, TAG_FIELD_NAME, tagFingerprint, THROWN_ERROR_FIELD_NAME, ex.getMessage());
			return ;
		}
		
		String whereClause = "";
		if (config.INFAREP_FOLDERS == null || config.INFAREP_FOLDERS.isEmpty()) {
			if (config.INFAREP_TEST_MAPPING_FILTER != null && !config.INFAREP_TEST_MAPPING_FILTER.isEmpty()) {
				whereClause = "WHERE " + config.INFAREP_TEST_MAPPING_FILTER;
			}
		}
		else {
			String[] selectedFolders = config.INFAREP_FOLDERS.split(";"); 
			whereClause = "WHERE folder IN ('" + selectedFolders[0] + "'";
			for (int i=1;i<selectedFolders.length;i++)
			{
				whereClause += ", '" + selectedFolders[0] + "'";
			}
			whereClause += ')';
			if (config.INFAREP_TEST_MAPPING_FILTER != null && !config.INFAREP_TEST_MAPPING_FILTER.isEmpty()) {
				whereClause += " AND " + config.INFAREP_TEST_MAPPING_FILTER;
			}
		}
		final String sql = GET_SQL_SELECT_STMTS_TMPLT.replace("<WHERE_CLAUSE>", whereClause).replace("<BI_INFOREP>", config.INFAREP_DB_ONWER); 
		doLogSQLTrace("Query for retrieving sql statements from INFA repository", INFO, GET_SQL_QUERIES_QRY_FIELD_NAME, sql);
		
		SQLStatement sqlStmt = null;
		int numOfStatementsCausingErr = 0, numberOfStatements = 0;
		try 
		{
			Statement stmt = getINFAREP_DBCONN().createStatement();
			ResultSet rs = null;
			try 
			{
				rs = stmt.executeQuery(sql);
			}
			catch(Exception ex)
			{
				doLogTrace("EtlSQLAnalyzeWizard - dwEnd", SEVERE, TAG_FIELD_NAME, tagFingerprint, THROWN_ERROR_FIELD_NAME, ex.getMessage(), GET_SQL_QUERIES_QRY_FIELD_NAME, sql);
				return ;
			}
			StringBuilder sqlText = new StringBuilder(512);
			String key = null, prevKey = "";
			String folder = null, widgetName = null, parentWidgetName = null, topWidgetName = null, widgetType = null;
			String stmtSQL = null;
			while (rs.next())
			{
				numberOfStatements++;
				key = rs.getString("WIDGET_KEY"); //TRIE
				//System.out.println("key: " + key + ", SQL_LINE_TEXT: " + rs.getString("SQL_LINE_TEXT"));
				if (!prevKey.equals(key)) {
					if (sqlText.length() != 0) {
						widgetType = ensureWidgetTypeCode(widgetType); 
						System.out.println("key: " + prevKey + ", sqlText: " + sqlText);
						stmtSQL = sqlText.toString();
						if (config.SQL_LOGGING_ON) {
							doLogSQLTrace("STATEMENT", INFO, "ownerWidget", prevKey, SQL_QUERY_FIELD_NAME, stmtSQL);
						}
						sqlStmt = null;
						try 
						{
							sqlStmt = SQLSelectStmtParseUtil.parseSQLStatement(stmtSQL);
							this.writeStatement(sqlStmt, folder, widgetName/*ownerWidget*/, widgetType/*ownerWidgetType*/, parentWidgetName, topWidgetName, tagFingerprint, stmtOutputStream, tblUsedColOutputStream, tblUseOutputStream, null);
						}
						catch(Exception ex)
						{
							logError(tagFingerprint, folder, widgetName, widgetType, parentWidgetName, topWidgetName, ex.getMessage(), errorStream);
							numOfStatementsCausingErr++;
							if (config.LOGGING_ON) {
								if (sqlStmt == null) {
									doLogTrace("Error while trying to parse sql", WARNING, "ownerWidget", key, THROWN_ERROR_FIELD_NAME, ex.getMessage());
								}
								else {
									doLogTrace("Error while trying to parse sql and write result", WARNING, "ownerWidget", key, "statementNumber" + java.lang.Long.toString(sqlStmt.etlRowWid), THROWN_ERROR_FIELD_NAME, ex.getMessage());
								}
							}
						}
						sqlText.setLength(0);
						//System.out.println("EtlSQLAnalyzeWizard::::sqlText after reset: " + sqlText);
					}
					prevKey = key;
					sqlText.append(rs.getString("SQL_LINE_TEXT"));
					folder = rs.getString("FOLDER");
					topWidgetName = rs.getString("TOP_WIDGET_NAME");
					parentWidgetName = rs.getString("PARENT_WIDGET_NAME");
					widgetName = rs.getString("WIDGET_NAME");
					widgetType = rs.getString("WIDGET_TYPE_NAME");
				} //end if (!prevKey.equals(key))
				else {
					sqlText/*.append(' ')*/.append(rs.getString("SQL_LINE_TEXT")); //BUG-FIX - 2017-05-17 - commented out the insertion of space in between previous chunk and current chunk as it was 1) unnecessary and 2) inserting a space in the middle of a column name
				}
				//System.out.println("EtlSQLAnalyzeWizard:::: key: " + key + ", sqlText: " + sqlText);
			} //end while
			if (sqlText.length() != 0) {
				System.out.println("EtlSQLAnalyzeWizard::::lastSqlText: " + sqlText);
				widgetType = ensureWidgetTypeCode(widgetType); 
				stmtSQL = sqlText.toString();
				if (config.SQL_LOGGING_ON) {
					doLogSQLTrace("STATEMENT", INFO, "ownerWidget", key, SQL_QUERY_FIELD_NAME, stmtSQL);
				}
				sqlStmt = null;
				try 
				{
					sqlStmt = SQLSelectStmtParseUtil.parseSQLStatement(sqlText.toString());
					this.writeStatement(sqlStmt, folder, widgetName/*ownerWidget*/, widgetType/*ownerWidgetType*/, parentWidgetName, topWidgetName, tagFingerprint, stmtOutputStream, tblUsedColOutputStream, tblUseOutputStream, null);
				}
				catch(Exception ex)
				{
					logError(tagFingerprint, folder, widgetName, widgetType, parentWidgetName, topWidgetName, ex.getMessage(), errorStream);
					numOfStatementsCausingErr++;
					if (config.LOGGING_ON) {
						if (sqlStmt == null) {
							doLogTrace("Error while trying to parse sql", WARNING, "ownerWidget", key, THROWN_ERROR_FIELD_NAME, ex.getMessage());
						}
						else {
							doLogTrace("Error while trying to parse sql and write result", WARNING, "ownerWidget", key, "statementNumber" + java.lang.Long.toString(sqlStmt.etlRowWid), THROWN_ERROR_FIELD_NAME, ex.getMessage());
						}
					}
				}
			}
			try 
			{
				markTagAsToBeLoadedIntoBAW();
			}
			catch(java.sql.SQLException sqle)
			{
				doLogTrace("Error while trying to mark the process as TO_CONTINUE_IN_INFA", WARNING, TAG_FIELD_NAME, tagFingerprint, THROWN_ERROR_FIELD_NAME, sqle.getMessage());
				throw new RuntimeException(
				"EtlSQLAnalyzeWizard::doWork-3: SQL error while trying to mark the process as to continue in INFA"
				, sqle
				);
			}
			doLogTrace("EtlSQLAnalyzeWizard - dwEnd", INFO, TAG_FIELD_NAME, tagFingerprint, NUMBER_OF_STMTS_FIELD_NAME, java.lang.Integer.toString(numberOfStatements), NUMBER_OF_STMTS_IN_ERR_FIELD_NAME, java.lang.Integer.toString(numOfStatementsCausingErr));
			if (config.SQL_LOGGING_ON) {
				doLogSQLTrace("EtlSQLAnalyzeWizard - dwEnd", INFO, TAG_FIELD_NAME, tagFingerprint, NUMBER_OF_STMTS_FIELD_NAME, java.lang.Integer.toString(numberOfStatements), NUMBER_OF_STMTS_IN_ERR_FIELD_NAME, java.lang.Integer.toString(numOfStatementsCausingErr));
			}
		}
		catch(java.sql.SQLException e)
		{
			doLogTrace("EtlSQLAnalyzeWizard - dwEnd", SEVERE, TAG_FIELD_NAME, tagFingerprint, THROWN_ERROR_FIELD_NAME, e.getMessage(), NUMBER_OF_STMTS_FIELD_NAME, java.lang.Integer.toString(numberOfStatements), NUMBER_OF_STMTS_IN_ERR_FIELD_NAME, java.lang.Integer.toString(numOfStatementsCausingErr));
			if (config.SQL_LOGGING_ON) {
				doLogSQLTrace("EtlSQLAnalyzeWizard - dwEnd", SEVERE, TAG_FIELD_NAME, tagFingerprint, THROWN_ERROR_FIELD_NAME, e.getMessage(), NUMBER_OF_STMTS_FIELD_NAME, java.lang.Integer.toString(numberOfStatements), NUMBER_OF_STMTS_IN_ERR_FIELD_NAME, java.lang.Integer.toString(numOfStatementsCausingErr));
			}
			throw new RuntimeException(
			"EtlSQLAnalyzeWizard::doWork-1: SQL error while trying to get and analyze SQL statements"
			, e
			);
		}
		catch(Exception ex)
		{
			doLogTrace("EtlSQLAnalyzeWizard - dwEnd", SEVERE, TAG_FIELD_NAME, tagFingerprint, THROWN_ERROR_FIELD_NAME, ex.getMessage(), NUMBER_OF_STMTS_FIELD_NAME, java.lang.Integer.toString(numberOfStatements), NUMBER_OF_STMTS_IN_ERR_FIELD_NAME, java.lang.Integer.toString(numOfStatementsCausingErr));
			if (config.SQL_LOGGING_ON) {
				doLogSQLTrace("EtlSQLAnalyzeWizard - dwEnd", SEVERE, TAG_FIELD_NAME, tagFingerprint, THROWN_ERROR_FIELD_NAME, ex.getMessage(), NUMBER_OF_STMTS_FIELD_NAME, java.lang.Integer.toString(numberOfStatements), NUMBER_OF_STMTS_IN_ERR_FIELD_NAME, java.lang.Integer.toString(numOfStatementsCausingErr));
			}
			throw new RuntimeException(
			"EtlSQLAnalyzeWizard::doWork-2: error while trying to get, analyze and dump SQL statements"
			, ex
			);
		}
		finally{
			try {stmtOutputStream.close(); tblUsedColOutputStream.close(); tblUseOutputStream.close(); errorStream.close(); } catch(Exception ex) {}
		}
	}
	
	static final String encodeErrMsg(String errMsg) {
		if (errMsg == null || errMsg.isEmpty()) return ISQLWidgetConstants.EMPTY_STR;
		int len = errMsg.length();
		StringBuilder buf = null; //new StringBuilder();
		int from = 0, i = 0;
		char ch = errMsg.charAt(0);
		main_loop: 
		do
		{
			switch(ch)
			{
			case '\r':
			case '\n':
				int end = i;
				__successive_ln_term_loop:
				do 
				{
					i++;
					if (i == len) {
						return buf == null ? errMsg.substring(0, end) : buf.toString();
					}
					ch = errMsg.charAt(i);
					if (ch == '\r' || ch == '\n') continue __successive_ln_term_loop;
					break ;
				}  while (true); //end __successive_ln_term_loop
				if (buf == null) {
					buf = new StringBuilder(len + 3);
				}
				buf.append(errMsg.substring(from, end));
				buf.append("<BR>");
				from = i;
				continue main_loop;
			default:
				i++;
				if (i == len) {
					if (buf == null) return errMsg;
					buf.append(errMsg.substring(from, len));
					return buf.toString();
				}
				ch = errMsg.charAt(i);
				continue main_loop;
			}
		} while (true); //main_loop
	}
	
	void logError(String tagFingerprint, String folder, String widgetName, String widgetType, String parentWidgetName, String topWidgetName, String errMsg, OutputStream errorStream) {
		if (errMsg == null || errMsg.isEmpty()) {
			errMsg = "error"; 
		}
		else {
			errMsg = errMsg.replace("\r\n", "<BR>");
		}
		try 
		{
			errorStream.write((tagFingerprint + '|').getBytes(UTF8_ENCODING));
			errorStream.write((folder + '|').getBytes(UTF8_ENCODING));
			errorStream.write((widgetName + '|').getBytes(UTF8_ENCODING));
			errorStream.write((widgetType + '|').getBytes(UTF8_ENCODING));
			errorStream.write((parentWidgetName + '|').getBytes(UTF8_ENCODING));
			errorStream.write((topWidgetName + '|').getBytes(UTF8_ENCODING));
			errorStream.write((encodeErrMsg(errMsg) + '|').getBytes(UTF8_ENCODING));
			String createdDt = toDateTimeString(new java.util.Date());
			errorStream.write((createdDt + '\n').getBytes(UTF8_ENCODING));
		}
		catch(java.io.IOException ioe)
		{
			ioe.printStackTrace();
		}
	}
	
	public static final void main(String[] args) {
		/*
		switch "-dw" //for doWork
		switch "-tf" //for testfile from config
		switch "-i" for input sql file combined with "-f", "-w", "-wt", "-pw", "-tw", "-o", "-prh2c"
		switch "-ph2c" ["-tf" | "-i" <inputFilePath>]
		
		*/
		boolean printHierDumpToConsole = false;
		String what = "-tf";
		if (args.length != 0) {
			what = args[0].trim();
			//System.out.println("what: '" + what + "', what.equals(\"-h\"): " + what.equals("-h"));
			if (!what.equals("-dw") && !what.equals("-tf") && !what.equals("-i") && !what.equals("-prh2c") && !what.equals("-h")) {
				return ;
			}
			else if (what.equals("-tf") && args.length > 1) {
				printHierDumpToConsole = args[1].equals("-prh2c");
			}
			else if (what.equals("-h")) {
				StringBuilder buf = new StringBuilder(1024);
				buf.append("Usage options: ").append(ISQLWidgetConstants.LN_TERMINATOR);
				buf.append("\t** java.exe [-cp <classpath>] expr.sql.EtlSQLAnalyzeWizard -dw  //to get work (with INFA repository as source for SQLs) to be done").append(ISQLWidgetConstants.LN_TERMINATOR); 
				buf.append("\t** java.exe [-cp <classpath>] expr.sql.EtlSQLAnalyzeWizard -tf [-prh2c] //to get the single SQL in the configured SQL test-file to be analyzed - print to console of the result tree is optional").append(ISQLWidgetConstants.LN_TERMINATOR); 
				buf.append("\t** java.exe [-cp <classpath>] expr.sql.EtlSQLAnalyzeWizard -i <inputSQLFilePath> -f <folder> -w <widgetName> -wt <widgetType> -pw <parentWidgetName> -tw <topWidgetName> [-o <outputDirectory> [-prh2c] | -prh2c] //to get the single SQL in the indicated input file to be analyzed - print to console of the result tree is optional").append(ISQLWidgetConstants.LN_TERMINATOR); 
				buf.append("\t** java.exe [-cp <classpath>] expr.sql.EtlSQLAnalyzeWizard -prh2c -tf //to get the tree representation of the result of the analysis of the input SQL test file specified in the config file to be dump to the console, of course in an hierarchical fashion").append(ISQLWidgetConstants.LN_TERMINATOR);
				buf.append("\t** java.exe [-cp <classpath>] expr.sql.EtlSQLAnalyzeWizard -prh2c -i <inputFilePath> //to get the tree representation of the result of the analysis of the indicated file to be dump to the console, of course in an hierarchical fashion").append(ISQLWidgetConstants.LN_TERMINATOR); 
				System.out.println(buf);
				return ;
			}
		}
		if (what.equals("-tf")) {
			System.out.println("YEAH");
			EtlSQLWizardConfig config = EtlSQLWizardConfig.get();
			String outputDir = config.OUTPUT_DIR == null || config.OUTPUT_DIR.isEmpty() ? "C:\\Users\\hp\\Documents\\sj\\source\\sql\\" : config.OUTPUT_DIR;
			String filePath = config.SQL_QUERY_TEST_FILE_NAME == null || config.SQL_QUERY_TEST_FILE_NAME.isEmpty() ? "C:\\Users\\hp\\Documents\\sj\\source\\sql\\simple_select.sql" : 
									EtlSQLWizardConfig.INFA_SQL_WIZARD_TESTFILES_DIR + config.SQL_QUERY_TEST_FILE_NAME;
			System.out.println("TEST_FILE: " + filePath + ", OUTPUT_DIR: " + outputDir); 
			String folder = "TESFILE_FOLDER";
			String widgetName = "SQL_TEST_FILE";
			String widgetType = "SELECT_STATEMENT"; 
			String parentWidget = "SQL_TEST_FILE";
			int index = filePath.lastIndexOf(java.io.File.separatorChar);
			String topWidget = index < 0 ? filePath : filePath.substring(index + 1);
			String tagFingerprint = "TF-" + toDateTimeString(new java.util.Date());
			dump_to_flat_file(filePath, folder, widgetName, widgetType, parentWidget, topWidget, tagFingerprint, outputDir, printHierDumpToConsole);
		}
		else if (what.equals("-dw")) {
			EtlSQLAnalyzeWizard wizard = new EtlSQLAnalyzeWizard();
			wizard.doWork();
		}
		else {
			if (args.length < 12) {
				System.out.println("Insufficient number of arguments, at least 12 arguments are expected");
				return ;
			}
			//switch "-i" for input sql file combined with "-f", "-w", "-wt", "-pw", "-tw", "-o", "-prh2c"
			String inputFilePath = args[1];
			if (!args[2].equals("-f")) {
				System.out.println("folder is expected for the second argument");
				return ;
			}
			String folder = args[3];
			if (!args[4].equals("-w")) {
				System.out.println("widget name is expected for the third argument");
				return ;
			}
			String widgetName = args[5];
			if (!args[6].equals("-wt")) {
				System.out.println("widget type is expected for the fourth argument");
				return ;
			}
			String widgetType = args[7];
			if (!args[8].equals("-pw")) {
				System.out.println("parent widget name is expected for the fifth argument");
				return ;
			}
			String parentWidgetName = args[9];
			if (!args[10].equals("-tw")) {
				System.out.println("top widget name is expected for the sixth argument");
				return ;
			}
			String topWidgetName = args[11];
			String outputDir = null;
			if (args.length > 12) {
				if (args[12].equals("-prh2c")) {
					printHierDumpToConsole = true;
				}
				else if (args[12].equals("-o")) {
					if (args.length < 14) {
						if (args[13].equals("-prh2c")) {
							System.out.println("missing output directory after \"-o\" switch");
							return ;
						}
						outputDir = args[13];
					}
					if (args.length > 14) {
						printHierDumpToConsole = args[14].equals("-prh2c");
					}
				}
			}
			if (outputDir == null) {
				EtlSQLWizardConfig config = EtlSQLWizardConfig.get();
				outputDir = config.OUTPUT_DIR == null || config.OUTPUT_DIR.isEmpty() ? "C:\\Users\\hp\\Documents\\sj\\source\\sql\\" : config.OUTPUT_DIR;;
			}
			String tagFingerprint = "IF-" + toDateTimeString(new java.util.Date());
			dump_to_flat_file(inputFilePath, folder, widgetName, widgetType, parentWidgetName, topWidgetName, tagFingerprint, outputDir, printHierDumpToConsole);
		}
		
		//System.out.println("(long)1.56d: " + (long)1.56d);
		
	}

}