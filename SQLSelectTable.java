package expr.sql;

/**
* Class for providing support for (from) tables of kind <code>select</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public class SQLSelectTable extends SQLSelectTbl/*SQLWidget.SQLTableRef*/ {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204C87301000000L;
	
	//protected String name; //NOTE: moved to super class on 2017-06-20
	protected SQLSelectStmt selectStmt;
	
	//protected SQLSelectStmt parentSelectStmt; //NOTE: moved to super class on 2017-06-20
	
	/**
	* Constructor.<br>
	*/
	SQLSelectTable() {
		super();
	}
	/**
	* Constructor.<br>
	*/
	SQLSelectTable(final SQLSelectStmt selectStmt) {
		this(selectStmt, EMPTY_STR);
	}
	/**
	* Constructor.<br>
	*/
	SQLSelectTable(final SQLSelectStmt selectStmt, final String name) {
		super();
		this.selectStmt = selectStmt;
		if (selectStmt != null) {
			selectStmt.parentSelectTbl = this;
		}
		this.name = name;
	}
	
	/**
	* {@inheritDoc}
	* @return {@link #SELECT_TABLE SELECT_TABLE}
	*/
	public final byte getKind() {return SELECT_TABLE; }
	/**
	* {@inheritDoc}
	*/
	public final boolean isSQLQuery() {return true; }
	/**
	* {@inheritDoc}
	*/
	public final boolean isAlias() {return false; }
	
	public final String getSchema() {return EMPTY_STR; }
	
	public final String getTableName() {return name; }
	
	public final SQLSelectStmt getSelectStmt() {return selectStmt; }
	
	protected void __appendMoreTableInfo(String tabsIndent, StringBuilder buf) {
		buf.append(tabsIndent).append("selectStmt: ").append(LN_TERMINATOR);
		selectStmt.__getChars(tabsIndent + '\t', buf);
	}
	
	void __trim() {
		if (selectStmt != null) {
			selectStmt.__trim();
		}
	}
	
	/**
	* Gets the output select statement.<br>
	*/
	public SQLSelectStmt getOutputSelectStmt() {
		return parentSelectStmt != null ? parentSelectStmt.getOutputSelectStmt() : null;
	}
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isNestedTableRef() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/	
	public final boolean isSQLSelectTable() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLSelectTable asSQLSelectTable() {return this; }

}