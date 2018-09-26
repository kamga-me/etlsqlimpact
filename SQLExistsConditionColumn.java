package expr.sql;

/**
* Class for providing support for SQL widgets of kind <code>EXISTS_CONDITION_COLUMN</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Right to use and modify given to <code>Michelin</code> corporation
*/
public class SQLExistsConditionColumn extends SQLSelectStmtColumn {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204B17151B6928AL;
	
	SQLExistsConditionColumn(SQLSelectStmt selectStmt) {
		super(selectStmt);
	}
	SQLExistsConditionColumn() {
		this(null);
	}
	
	/**
	* {@inheritDoc}
	* @return {@link #EXISTS_CONDITION_COLUMN EXISTS_CONDITION_COLUMN}
	*/
	public final byte getType() {
		return EXISTS_CONDITION_COLUMN;
	}
//	/**
//	* {@inheritDoc}
//	* @return {@code true}
//	*/
//	public final boolean isLogicalColumn() {
//		return true;
//	}
	
	/**
	* {@inheritDoc}
	*/
	public String getDefaultTopExprSubGroup() {
		return EXISTS_CONDITION_COLUMN_SUBGRP_NAME;
	}
	
}