package expr.sql;

/**
* Class for providing support for (from) tables of kind <code>combining-select</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public class SQLCombiningSelectTable extends SQLSelectTbl/*SQLWidget.SQLTableRef*/ {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x020DAEE261B6928AL;

	protected boolean withinBrackets;
	//protected String name; //NOTE: moved to super class on 2017-06-20
	protected SQLCombiningStatement selectStmt;
	
	//protected SQLSelectStmt parentSelectStmt; //NOTE: moved to super class on 2017-06-20
	
	/**
	* Constructor.<br>
	*/
	SQLCombiningSelectTable(final boolean withinBrackets) {
		super();
		this.withinBrackets = withinBrackets;
		if (withinBrackets) {
			this.selectStmt = new SQLCombiningStatement();
			this.selectStmt.parentSelectTbl = this;
		}
	}
	/**
	* Constructor.<br>
	*/
	SQLCombiningSelectTable() {
		this(false/*withinBrackets*/);
	}
	/**
	* Constructor.<br>
	*/
	SQLCombiningSelectTable(final SQLCombiningStatement selectStmt) {
		this(selectStmt, EMPTY_STR);
	}
	/**
	* Constructor.<br>
	*/
	SQLCombiningSelectTable(final SQLCombiningStatement selectStmt, final String name) {
		this(selectStmt, name, false/*withinBrackets*/);
	}
	/**
	* Constructor.<br>
	*/
	SQLCombiningSelectTable(final SQLCombiningStatement selectStmt, final boolean withinBrackets) {
		this(selectStmt, EMPTY_STR, withinBrackets);
	}
	/**
	* Constructor.<br>
	*/
	SQLCombiningSelectTable(final SQLCombiningStatement selectStmt, final String name, final boolean withinBrackets) {
		super();
		this.selectStmt = selectStmt;
		if (selectStmt != null) {
			selectStmt.parentSelectTbl = this;
		}
		this.name = name;
		this.withinBrackets = withinBrackets;
	}

	/**
	* {@inheritDoc}
	* @return {@link #COMBINING_SELECT_TABLE COMBINING_SELECT_TABLE}
	*/
	public final byte getKind() {return COMBINING_SELECT_TABLE; }
	/**
	* {@inheritDoc}
	*/
	public final boolean isSQLQuery() {return true; }
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
	public final String getTableName() {return name; }
	/**
	* {@inheritDoc}
	*/
	public final SQLCombiningStatement getSelectStmt() {return selectStmt; }
	
	protected void __appendMoreTableInfo(String tabsIndent, StringBuilder buf) {
		buf.append(tabsIndent).append("withinBrackets: ").append(withinBrackets).append(LN_TERMINATOR);
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
	public final boolean isSQLCombiningSelectTable() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLCombiningSelectTable asSQLCombiningSelectTable() {return this; }


}