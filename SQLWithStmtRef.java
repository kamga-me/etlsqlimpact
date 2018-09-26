package expr.sql;

/**
* Class for providing support for SQL widgets of kind <code>WITH_STATEMENT_REF</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Right to use and modify given to <code>Michelin</code> corporation
*/
public class SQLWithStmtRef extends SQLWidget.SQLTableRef {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x02051DB3A1B6928AL;
	
	protected SQLWithStmt withStmt;
	protected String aliasName;
	
	/**
	* Constructor.
	*/
	SQLWithStmtRef(SQLWithStmt withStmt, String aliasName) {
		this.withStmt = withStmt;
		this.aliasName = aliasName; 
	}
	/**
	* Constructor.
	*/
	SQLWithStmtRef(SQLWithStmt withStmt) {
		this(withStmt, EMPTY_STR);
	}
	
	/**
	* {@inheritDoc}
	*/
	public final boolean isSQLQuery() {return true; }
	/**
	* {@inheritDoc}
	*/
	public final boolean isWithStmtRef() {return true; }
	/**
	* {@inheritDoc}
	*/
	public final boolean isJoin() {return false; }
	/**
	* {@inheritDoc}
	*/
	public final boolean isAlias() {return false; }
	/**
	* {@inheritDoc}
	*/
	public final String getSchema() {return EMPTY_STR; }
	/**
	* {@inheritDoc}
	*/
	public final String getTableName() {return withStmt.name; }
	/**
	* {@inheritDoc}
	*/
	public String getAliasName() {return aliasName.isEmpty() ? withStmt.name : aliasName; }
	/**
	* {@inheritDoc}
	* @return {@link #WITH_STMT_TABLE_REF WITH_STMT_TABLE_REF}
	*/
	public final byte getKind() {return WITH_STMT_TABLE_REF; }
	
	
	protected void __getChars(String tabsIndent, StringBuilder buf) {
		buf.append(tabsIndent).append("type: ").append(getTypeCode(getType())).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("isWithStmtRef: ").append(true).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("withStmtName: ").append(withStmt.name).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("aliasName: ").append(aliasName).append(LN_TERMINATOR);
	}
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isSQLWithStmtRef() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLWithStmtRef asSQLWithStmtRef() {return this; }
	

}