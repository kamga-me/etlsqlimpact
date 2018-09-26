package expr.sql;

/**
* This is the class for providing support for SQL widgets of kind <code>COLUMN_REF</code> and in raw format.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Right to use and modify given to <code>Michelin</code> corporation
*/
public class SQLRawColumnRef extends SQLWidget.SQLColumn {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204CA1161000000L;
	
	protected String tableAlias; //tableAlias or tableName
	protected String name;

	SQLRawColumnRef(String tableAlias, String name) {
		super();
		this.tableAlias = tableAlias;
		this.name = name;
	}
	SQLRawColumnRef(String tableAlias, String name, String aliasName) {
		super(aliasName);
		this.tableAlias = tableAlias;
		this.name = name;
	}
	/**
	* {@inheritDoc}
	* @return {@link #COLUMN_REF COLUMN_REF}
	*/
	public final byte getType() {
		return COLUMN_REF;
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
	* @return {@code true}
	*/
	final boolean isRaw() {return true; }
	/**
	* Returns the alias used for the table the column is associated with.
	*/
	public String getTableAlias() {return tableAlias; }
	/**
	* {@inheritDoc}
	*/
	public String getColumnName() {return name; }
	/**
	* {@inheritDoc}
	*/
	public final String getName() {return getColumnName(); }
	
	/**
	* {@inheritDoc}
	* @return {@code this}
	*/
	public SQLColumn getPhysicalColumn() {
		return this; 
	}
	/**
	* {@inheritDoc}
	* @return {@code 0}
	*/
	public byte getPhysicalColumnSource(final SQLPhysicalColumnSource output) {
		return 0;
	}
	/**
	* {@inheritDoc}
	*/
	public final char getLogicalColumnType() {
		return 'N';
	}
	/**
	* {@inheritDoc}
	*/
	public final boolean isLogicalColumn() {
		return false;
	}

	protected void __getChars(String tabsIndent, StringBuilder buf) {
		buf.append(tabsIndent).append("type: ").append(getTypeCode(getType())).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("tableAlias: ").append(tableAlias).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("name: ").append(name).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("aliasName: ").append(aliasName).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("outputNumber: ").append(outputNumber).append(LN_TERMINATOR);
		if (localOutputNumber > -1) {
			buf.append(tabsIndent).append("localOutputNumber: ").append(localOutputNumber).append(LN_TERMINATOR);
		}
	}
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isSQLRawColumnRef() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLRawColumnRef asSQLRawColumnRef() {return this; }
	
}