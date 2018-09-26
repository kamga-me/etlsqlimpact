package expr.sql;

/**
*
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public class SQLStmtTable extends SQLWidget.SQLTableRef implements Comparable<SQLStmtTable> {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204EE7181B6928AL;
	
	public static final SQLStmtTable NO_TABLE = new SQLStmtTable(EMPTY_STR, EMPTY_STR);
	
	protected String schema;
	protected String name;

	SQLStmtTable(String schema, String name) {
		super();
		this.schema = schema;
		this.name = name;
	}
	/**
	* {@inheritDoc}
	*/
	public final boolean isSQLQuery() {return false; }
	/**
	* {@inheritDoc}
	*/
	public final boolean isAlias() {return false; }
	
	public final String getSchema() {return schema; }
	
	public final String getTableName() {return name; }
	
	public final String getAliasName() {return name; }
	
	/**
	* {@inheritDoc}
	* @return {@link #STMT_TABLE STMT_TABLE}
	*/
	public final byte getKind() {return STMT_TABLE; }
	
	
	public int hashCode() {
		return schema.hashCode() ^ name.hashCode();
	}
	
	public int compareTo(SQLStmtTable other) {
		int cmpRslt = schema.compareTo(other.schema);
		if (cmpRslt != 0) return cmpRslt;
		return name.compareTo(other.name);
	}
	
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/	
	public final boolean isSQLStmtTable() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLStmtTable asSQLStmtTable() {return this; }

}