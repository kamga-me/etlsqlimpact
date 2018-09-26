package expr.sql;

/**
* Class for providing support for SQL widgets of kind <code>IN_SELECT_COLUMN</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Right to use and modify given to <code>Michelin</code> corporation
*/
public class SQLInSelectColumn extends SQLSelectStmtColumn {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204B17761B6928AL;
	
	SQLInSelectColumn(SQLSelectStmt selectStmt) {
		super(selectStmt);
	}
	SQLInSelectColumn() {
		this(null);
	}
	
	/**
	* {@inheritDoc}
	* @return {@link #IN_SELECT_COLUMN IN_SELECT_COLUMN}
	*/
	public final byte getType() {
		return IN_SELECT_COLUMN;
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
		return IN_SELECT_COLUMN_SUBGRP_NAME;
	}
	
}