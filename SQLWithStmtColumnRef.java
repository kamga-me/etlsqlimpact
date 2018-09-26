package expr.sql;

/**
* Class for providing support for SQL column refs of kind <code>WITH_STMT_COLUMN_REF</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Right to use and modify given to <code>Michelin</code> corporation
*/
public class SQLWithStmtColumnRef extends SQLWidget.SQLColumn {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204B18551B6928AL;

	protected SQLWithStmtRef/*SQLWithStmt*/ withStmt;
	protected SQLColumn withStmtColumn; //ouput column from with statement

	SQLWithStmtColumnRef(SQLWithStmtRef withStmt, SQLColumn withStmtColumn) {
		super();
		this.withStmt = withStmt;
		this.withStmtColumn = withStmtColumn;
	}
	SQLWithStmtColumnRef(SQLWithStmtRef withStmt, SQLColumn withStmtColumn, String aliasName) {
		super(aliasName);
		this.withStmt = withStmt;
		this.withStmtColumn = withStmtColumn;
	}

	/**
	* {@inheritDoc}
	* @return {@link #WITH_STMT_COLUMN_REF WITH_STMT_COLUMN_REF}
	*/
	public final byte getType() {
		return WITH_STMT_COLUMN_REF;
	}
	/**
	* {@inheritDoc}
	* @return {@code true}
	*/
	public final boolean isColumnRef() {return true; }
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
	*/
	public final SQLWithStmtRef/*SQLTableRef*/ getTable() {return getWithStmt(); }
	/**
	* {@inheritDoc}
	*/
	public String getTableName() {return EMPTY_STR; }
	/**
	* {@inheritDoc}
	*/
	public SQLWithStmtRef getWithStmt() {return withStmt; }
	/**
	* Returns the name of the output column of the backing with-statement to which this <code>SQLWithStmtColumnRef</code> binds.
	*/
	public String getColumnName() {return withStmtColumn.getOutputName(); }
	/**
	* {@inheritDoc}
	*/
	public final String getName() {return getColumnName(); }
	/**
	* {@inheritDoc}
	*/
	public SQLColumn getAssociatedColumm() {
		return withStmtColumn;
	}
	/**
	* {@inheritDoc}
	* @return {@code true}
	*/
	public final boolean isLogicalColumn() {
		return true;
	}

	/**
	* {@inheritDoc}
	* @return {@code 'W'}
	*/
	public final char getLogicalColumnType() {
		return 'W';
	}

	/**
	* {@inheritDoc}
	*/
	public SQLColumn getPhysicalColumn() {
		switch(withStmtColumn.getLogicalColumnType())
		{
		case 'N': 
			return withStmtColumn;
		case 'Y': 
		case 'S': 
			return null;
		}
		return withStmt.withStmt.getPhysicalColumnFor(withStmtColumn.getOutputName());
	}
	
	/**
	* {@inheritDoc}
	*/
	public byte getPhysicalColumnSource(final SQLPhysicalColumnSource output) {
		switch(withStmtColumn.getLogicalColumnType())
		{
		case 'N': 
			return 0;
		case 'Y': 
		case 'S': //for select-stmt-column
			return -1;
		}
		return withStmt.withStmt.getPhysicalColumnSourceFor(withStmtColumn.getOutputName(), output) ? (byte)1 : (byte)-1;
	}

	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isSQLWithStmtColumnRef() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLWithStmtColumnRef asSQLWithStmtColumnRef() {return this; }

	protected void __getChars(String tabsIndent, StringBuilder buf) {
		buf.append(tabsIndent).append("type: ").append(getTypeCode(getType())).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("withStmtTableRef: ").append(withStmt.withStmt.name).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("tableAlias: ").append(withStmt.getAliasName()).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("columnName: ").append(getName()).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("aliasName: ").append(aliasName).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("outputNumber: ").append(outputNumber).append(LN_TERMINATOR);
		if (localOutputNumber > -1) {
			buf.append(tabsIndent).append("localOutputNumber: ").append(localOutputNumber).append(LN_TERMINATOR);
		}
	}

}