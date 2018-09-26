package expr.sql;

/**
* Class for providing support for SQL widgets of kind <code>COLUMN</code>, that each nests a select statement.<br>
*
* @author Marc E. KAMGA
* @version 1.0
*
*/
public class SQLSelectStmtColumn extends SQLWidget.SQLColumn {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204AD43E1B6928AL;
	
	protected SQLSelectStmt selectStmt;
	//protected SQLSelectStmt parentSelectStmt; //MOVED TO SQLSelectStmt class
	
	protected long etlRowWid;

	SQLSelectStmtColumn(SQLSelectStmt selectStmt) {
		super();
		this.selectStmt = selectStmt;
	}
	SQLSelectStmtColumn(SQLSelectStmt selectStmt, String aliasName) {
		super(aliasName);
		this.selectStmt = selectStmt;
	}
	/**
	* {@inheritDoc}
	*/
	public /*final */byte getType() {
		return SELECT_STMT_COLUMN;
	}
	/**
	* {@inheritDoc}
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
	* @return {@code true}
	*/
	public final boolean isSelectStmtColumn() {return true; }
	/**
	* Returns the associated select statement.
	*/
	public SQLSelectStmt getSelectStmt() {return selectStmt; }
		
	//public SQLSelectStmt getParentSelectStmt() {return parentSelectStmt; } //MOVED TO SQLSelectStmt class
	
	/*MOVED TO SQLSelectStmt class
	public SQLTableRef getTableByAlias(String tableAlias) {
		SQLTableRef tblRef = selectStmt.getTableByAlias(tableAlias);
		return tblRef != null ? tblRef : parentSelectStmt != null ? parentSelectStmt.getTableByAlias(tableAlias) : null;
	}
	*/
	/**
	* {@inheritDoc}
	*/
	public String getDefaultTopExprSubGroup() {
		return SELECT_STMT_COLUMN_SUBGRP_NAME;
	}
	
	/**
	* {@inheritDoc}
	*/
	public long getEtlRowWid() {
		return etlRowWid;
	}
	void __setEtlRowWid(long etlRowWid) {
		this.etlRowWid = etlRowWid;
	}
	
	void __trim() {
		if (selectStmt != null) {
			selectStmt.__trim();
		}
	}
	
	protected void __appendMoreColumnInfo(String tabsIndent, StringBuilder buf) {
		buf.append(tabsIndent).append("selectStmt: ").append(LN_TERMINATOR);
		selectStmt.__getChars(tabsIndent + '\t', buf);
	}
	
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isLogicalColumn() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>'S'</code>
	*/
	public final char getLogicalColumnType() {
		return 'S';
	}
	/**
	* {@inheritDoc}
	* @return <code>null</code>
	*/
	public final SQLColumn getPhysicalColumn() {
		return null;
	}
	/**
	* {@inheritDoc}
	* @return {@code -1}
	*/
	public final byte getPhysicalColumnSource(final SQLPhysicalColumnSource output) {
		return -1;
	}
	
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isSQLSelectStmtColumn() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLSelectStmtColumn asSQLSelectStmtColumn() {return this;}
	
	
}