package expr.sql;

import static expr.sql.SQLPseudoColumnNames.ROWNUM_COLNAME;
import static expr.sql.SQLPseudoColumnNames.ROWID_COLNAME;
import static expr.sql.SQLPseudoColumnNames.SYSDATE_COLNAME;
import static expr.sql.SQLPseudoColumnNames.OBJECT_ID_COLNAME;
import static expr.sql.SQLPseudoColumnNames.SYS_NC_OID$_COLNAME;
import static expr.sql.SQLPseudoColumnNames.LEVEL_COLNAME;
import static expr.sql.SQLPseudoColumnNames.CURRENT_DATE_COLNAME;
import static expr.sql.SQLPseudoColumnNames.CURRENT_TIMESTAMP_COLNAME;
import static expr.sql.SQLPseudoColumnNames.SESSIONTIMEZONE_COLNAME;
import static expr.sql.SQLPseudoColumnNames.USER_COLNAME;
import static expr.sql.SQLPseudoColumnNames.SYSTIMESTAMP_COLNAME;

/**
* Class for providing support for SQL widgets of kind <code>SEQUENCE_VALUE_REF</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Right to use and modify given to <code>Michelin</code> corporation
*/
public class SQLPseudoColumnRef extends SQLWidget.SQLColumn {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x02051A9EB1B6928AL;
	
/*
	public static final String ROWNUM_COLNAME = "ROWNUM"; 
	public static final String ROWID_COLNAME = "ROWID"; 
	public static final String SYSDATE_COLNAME = "SYSDATE"; 
	public static final String OBJECT_ID_COLNAME = "OBJECT_ID"; 
	public static final String SYS_NC_OID$_COLNAME = "SYS_NC_OID$"; 
	public static final String LEVEL_COLNAME = "LEVEL"; 
	public static final String CURRENT_DATE_COLNAME = "CURRENT_DATE"; 
	public static final String CURRENT_TIMESTAMP_COLNAME = "CURRENT_TIMESTAMP"; 
	public static final String SESSIONTIMEZONE_COLNAME = "SESSIONTIMEZONE"; 
	public static final String USER_COLNAME = "USER"; 
	public static final String SYSTIMESTAMP_COLNAME = "SYSTIMESTAMP";
*/	
	
	public static final SQLPseudoColumnRef ROWNUM = new SQLPseudoColumnRef(ROWNUM_COLNAME); 
	public static final SQLPseudoColumnRef ROWID = new SQLPseudoColumnRef(ROWID_COLNAME); 
	public static final SQLPseudoColumnRef SYSDATE = new SQLPseudoColumnRef(SYSDATE_COLNAME); 
	public static final SQLPseudoColumnRef OBJECT_ID = new SQLPseudoColumnRef(OBJECT_ID_COLNAME); 
	public static final SQLPseudoColumnRef SYS_NC_OID$ = new SQLPseudoColumnRef(SYS_NC_OID$_COLNAME); 
	public static final SQLPseudoColumnRef LEVEL = new SQLPseudoColumnRef(LEVEL_COLNAME); 
	public static final SQLPseudoColumnRef CURRENT_DATE = new SQLPseudoColumnRef(CURRENT_DATE_COLNAME); 
	public static final SQLPseudoColumnRef CURRENT_TIMESTAMP = new SQLPseudoColumnRef(CURRENT_TIMESTAMP_COLNAME); 
	public static final SQLPseudoColumnRef SESSIONTIMEZONE = new SQLPseudoColumnRef(SESSIONTIMEZONE_COLNAME); 
	public static final SQLPseudoColumnRef USER = new SQLPseudoColumnRef(USER_COLNAME); 
	public static final SQLPseudoColumnRef SYSTIMESTAMP = new SQLPseudoColumnRef(SYSTIMESTAMP_COLNAME); 
	
	protected final String name;

	SQLPseudoColumnRef(String name, String aliasName) {
		super(aliasName);
		this.name = name;
	}
	
	SQLPseudoColumnRef(String name) {
		this(name, EMPTY_STR);
	}
	
	/**
	* Gets the name of the pseudo column.
	*/
	public String getName() {
		return name;
	}
	
	/**
	* {@inheritDoc}
	* @return {@link #PSEUDO_COLUMN_REF PSEUDO_COLUMN_REF}
	*/
	public final byte getType() {
		return PSEUDO_COLUMN_REF;
	}
	/**
	* {@inheritDoc}
	* @return {@code false}
	*/
	public final boolean isColumnRef() {return false; }
	/**
	* {@inheritDoc}
	* @return {@code false}
	*/
	public final boolean isConstant() {return false; }
	/**
	* {@inheritDoc}
	* @return {@code false}
	*/
	public final boolean isExpr() {return false; }
	/**
	* {@inheritDoc}
	* @return {@code false}
	*/
	public final boolean isSelectStmtColumn() {return false; }
	/**
	* {@inheritDoc}
	* @return {@code true}
	*/
	public boolean isLogicalColumn() {return true; }
	/**
	* {@inheritDoc}
	* @return {@code 'Y'}
	*/
	public final char getLogicalColumnType() {return 'Y'; }
	/**
	* {@inheritDoc}
	* @return {@code this}
	*/
	public final SQLColumn getPhysicalColumn() {
		return this;
	}
	/**
	* {@inheritDoc}
	* @return {@code 0}
	*/
	public byte getPhysicalColumnSource(final SQLPhysicalColumnSource output) {
		return 0;
	}
	
	
	public SQLPseudoColumnRef derive(String aliasName) {
		if (this.aliasName.isEmpty()) {
			if (aliasName == null || aliasName.isEmpty()) return this;
			return new SQLPseudoColumnRef(this.name, aliasName);
		}
		else if (aliasName == null || aliasName.isEmpty()) {
			SQLPseudoColumnRef pseudoColRef = checkPseudoColName(this.name);
			return pseudoColRef != null ? pseudoColRef : new SQLPseudoColumnRef(this.name);
		}
		else if (this.aliasName == aliasName || this.aliasName.equalsIgnoreCase(aliasName)) {
			return this;
		}
		return new SQLPseudoColumnRef(this.name, aliasName);
	}
	
	
	protected void __getChars(String tabsIndent, StringBuilder buf) {
		buf.append(tabsIndent).append("type: ").append(getTypeCode(getType())).append(LN_TERMINATOR);
		__prependMoreInfo(tabsIndent, buf);
		buf.append(tabsIndent).append("name: ").append(name).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("aliasName: ").append(aliasName).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("outputNumber: ").append(outputNumber).append(LN_TERMINATOR);
		if (localOutputNumber > -1) {
			buf.append(tabsIndent).append("localOutputNumber: ").append(localOutputNumber).append(LN_TERMINATOR);
		}
		__appendMoreColumnInfo(tabsIndent, buf);
	}
	
	/**
	* @return null if the supplied string is not equal to one of the known/handled pseudo column names; the reference non aliased <code>SQLPseudoColumnRef</code> for the indicated pseudo column name, otherwise.
	*/
	public static final SQLPseudoColumnRef checkPseudoColName(String colName) {
		if (ROWNUM_COLNAME.equalsIgnoreCase(colName)) return ROWNUM;
		else if (SYSDATE_COLNAME.equalsIgnoreCase(colName)) return SYSDATE;
		else if (ROWID_COLNAME.equalsIgnoreCase(colName)) return ROWID;
		else if (OBJECT_ID_COLNAME.equalsIgnoreCase(colName)) return OBJECT_ID;
		else if (CURRENT_TIMESTAMP_COLNAME.equalsIgnoreCase(colName)) return CURRENT_TIMESTAMP;
		else if (CURRENT_DATE_COLNAME.equalsIgnoreCase(colName)) return CURRENT_DATE;
		else if (SYSTIMESTAMP_COLNAME.equalsIgnoreCase(colName)) return SYSTIMESTAMP;
		else if (SESSIONTIMEZONE_COLNAME.equalsIgnoreCase(colName)) return SESSIONTIMEZONE;
		else if (USER_COLNAME.equalsIgnoreCase(colName)) return USER;
		else if (LEVEL_COLNAME.equalsIgnoreCase(colName)) return LEVEL;
		else if (SYS_NC_OID$_COLNAME.equalsIgnoreCase(colName)) return SYS_NC_OID$;
		return null;
	}

}