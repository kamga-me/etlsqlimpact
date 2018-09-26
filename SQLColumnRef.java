package expr.sql;

/**
* This is the primary class for providing support for SQL widgets of kind <code>COLUMN_REF</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Right to use and modify given to <code>Michelin</code> corporation
*/
public class SQLColumnRef extends SQLWidget.SQLColumn {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204B18381B6928AL;
	
	protected SQLTableRef table;
	protected String name;

	SQLColumnRef(SQLTableRef table, String name) {
		super();
		this.table = table;
		this.name = name;
	}
	SQLColumnRef(SQLTableRef table, String name, String aliasName) {
		super(aliasName);
		this.table = table;
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
	* Returns the <code>SQLTableRef</code> for the table this is bound to.
	*/	
	public SQLTableRef getTable() {return table; }
	/**
	* Returns the name of the table this is bound to.
	*/	
	public String getTableName() {return table.getTableName(); }
	/**
	* {@inheritDoc}
	*/
	public String getName() {return name; }/**
	* {@inheritDoc}
	* @return {@code false}
	*/
	final boolean isRaw() {return false; }
	
	/**
	* {@inheritDoc}
	*/
	public boolean isLogicalColumn() {
		return table != null && table.isNestedTableRef(); //false;
	}
	/**
	* {@inheritDoc}
	* @return {@code 'N'} for not-a-logical column or {@code 'T'} for column sourced from/bounded to a nested select table
	*/
	public final char getLogicalColumnType() {
		return table != null && table.isNestedTableRef() ? 'T' : 'N';
	}
	
	/**
	* {@inheritDoc}
	*/
	public SQLColumn getPhysicalColumn() {
		if (table == null || !table.isNestedTableRef()) return this;
		else if (table.isSQLSelectTable()) {
			try 
			{
				return table.asSQLSelectTable().selectStmt.getPhysicalColumnFor(name);
			}
			catch(Exception ex)
			{
				throw new IllegalStateException(ex.getMessage() + LN_TERMINATOR + 
				"SQLColumnRef::getPhysicalColumn-1: SQL column object in illegal state"
				, ex 
				);
			}
		}
		else if (!table.isSQLStmtTableAlias()) {
			throw new IllegalStateException(
			"SQLColumnRef::getPhysicalColumn-2: SQL column object in illegal state - table of kind SQLStmtTableAlias expected"
			);
		}
		SQLStmtTableAlias tblAlias = table.asSQLStmtTableAlias();
		if (!tblAlias.of.isSQLSelectTable()) {
			throw new IllegalStateException(
			"SQLColumnRef::getPhysicalColumn-3: SQL column object in illegal state - table of kind nested select table expected"
			);
		}
		try 
		{
			return tblAlias.of.asSQLSelectTable().selectStmt.getPhysicalColumnFor(name);
		}
		catch(Exception ex)
		{
			throw new IllegalStateException(ex.getMessage() + LN_TERMINATOR + 
			"SQLColumnRef::getPhysicalColumn-4: SQL column object in illegal state"
			, ex 
			);
		}
	}
	
	/**
	* {@inheritDoc}
	*/
	public byte getPhysicalColumnSource(final SQLPhysicalColumnSource output) {
		if (table == null || !table.isNestedTableRef()) return 0;
		else if (table.isSQLSelectTable()) {
			try 
			{
				return table.asSQLSelectTable().selectStmt.getPhysicalColumnSourceFor(name, output) ? (byte)1 : (byte)-1;
			}
			catch(Exception ex)
			{
				throw new IllegalStateException(ex.getMessage() + LN_TERMINATOR + 
				"SQLColumnRef::getPhysicalColumnSource-1: SQL column object in illegal state"
				, ex 
				);
			}
		}
		else if (!table.isSQLStmtTableAlias()) {
			throw new IllegalStateException(
			"SQLColumnRef::getPhysicalColumnSource-2: SQL column object in illegal state - table of kind SQLStmtTableAlias expected"
			);
		}
		SQLStmtTableAlias tblAlias = table.asSQLStmtTableAlias();
		if (!tblAlias.of.isSQLSelectTable()) {
			throw new IllegalStateException(
			"SQLColumnRef::getPhysicalColumnSource-3: SQL column object in illegal state - table of kind nested select table expected"
			);
		}
		try 
		{
			return tblAlias.of.asSQLSelectTable().selectStmt.getPhysicalColumnSourceFor(name, output) ? (byte)1 : (byte)-1;
		}
		catch(Exception ex)
		{
			throw new IllegalStateException(ex.getMessage() + LN_TERMINATOR + 
			"SQLColumnRef::getPhysicalColumnSource-4: SQL column object in illegal state"
			, ex 
			);
		}
	}
	/**
	* {@inheritDoc}
	*/
	public final boolean isSQLColumnRef() {return true; }
	/**
	* {@inheritDoc}
	*/
	public final SQLColumnRef asSQLColumnRef() {return this; }
	
	//public String toString() {return };

}