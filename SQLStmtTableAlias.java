package expr.sql;

/**
*
* @author Marc E. KAMGA
* @version 1.0
*/
public class SQLStmtTableAlias extends SQLWidget.SQLTableRef {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204EE76C1B6928AL;
	
	protected SQLStmtTable of;
	protected String aliasName;
	
	SQLStmtTableAlias() {
		super();
	}
	SQLStmtTableAlias(String aliasName, SQLStmtTable of) {
		super();
		this.aliasName = aliasName;
		this.of = of;
	}
	
	/**
	* {@inheritDoc}
	*/
	public final boolean isSQLQuery() {return false; }
	/**
	* {@inheritDoc}
	*/
	public final boolean isAlias() {return true; }
	/**
	* {@inheritDoc}
	*/
	public final String getSchema() {return of.schema; }
	/**
	* {@inheritDoc}
	*/
	public final String getTableName() {return of.name; }
	/**
	* {@inheritDoc}
	*/
	public final SQLStmtTable of() {return of; }
	/**
	* {@inheritDoc}
	*/
	public final String getAliasName() {return aliasName; }
	
	/**
	* {@inheritDoc}
	* @return {@link #STMT_TABLE_ALIAS STMT_TABLE_ALIAS}
	*/
	public final byte getKind() {return STMT_TABLE_ALIAS; }
	
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isSQLStmtTableAlias() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLStmtTableAlias asSQLStmtTableAlias() {return this; }
	
	/**
	* {@inheritDoc}
	*/
	public boolean isNestedTableRef() {
		return of.isSQLSelectTable(); 
	}

}