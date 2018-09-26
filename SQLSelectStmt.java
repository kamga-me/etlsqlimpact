package expr.sql;

/**
* Class for providing support for SQL widgets of kind <code>SELECT_STATEMENT</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public class SQLSelectStmt extends SQLWidget.SQLStatement implements ISQLStatementCombiningOperators, ISQLSelectDistinctMarks {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204A03771B6928AL;
//	
//	public static final byte UNION = 1;
//	
//	public static final byte UNION_ALL = 2;
//	
//	public static final byte MINUS = 3;
//	
//	public static final byte INTERSECT = 4;
//	/**
//	* MS SQL Server equivalent for MINUS
//	*/
//	public static final byte EXCEPT = 5;
	
	
	protected SQLSelectStmt[] withStmts;
	protected int withStmtsCount;
	protected boolean selectDistinct;
	protected SQLColumn[] columns; //select columns
	protected int columnsCount;
	protected SQLTableRef[] fromTables; //includes tables declared using JOIN statements, those are instances of SQLJoin class?!
	protected int fromTablesCount;
	protected SQLJoin[] joins;
	protected int joinsCount;
	protected SQLWhereClause whereClause;
	protected SQLGroupByClause groupByClause;
	protected SQLHavingClause havingClause;
	protected SQLOrderByClause orderByClause;
	
	protected long limit;
	protected long offset;
	
	protected double topN;
	protected boolean topNIsPercent;
	
	protected int outputColumnsCount;
	
	protected SQLSelectStmt parentSelectStmt;
	protected SQLSelectTbl/*SQLSelectTable*/ parentSelectTbl;
	
	protected boolean withInfaJoins;
	
	/**
	* Constructor.<br>
	*/
	SQLSelectStmt(final boolean selectDistinct) {
		super();
		this.withStmtsCount = 0;
		this.fromTablesCount = 0;
		this.joinsCount = 0;
		this.columnsCount = 0;
		this.selectDistinct = selectDistinct;
		this.limit = -1;
		this.offset = -1;
		this.outputColumnsCount = 0;
		this.withInfaJoins = false;
		this.topN = Double.NaN;
		this.topNIsPercent = false;
	}
	
	SQLSelectStmt() {
		this(false);
	}
	
	/**
	* {@inheritDoc}
	*
	* @see #SELECT_STATEMENT SELECT_STATEMENT
	* @see #WITH_STATEMENT WITH_STATEMENT
	*/
	public /*final */byte getType() {
		return SELECT_STATEMENT;
	}
	
	/**
	* Tells if the select statement has a LIMIT clause
	*/
	public boolean hasLimit() {return limit > -1; }
	/**
	* @return <code>-1</code> if the select statement does not have a LIMIT clause; the value of the limit, otherwise.
	*/
	public long getLimit() {return limit; }
	
	/**
	* Tells if the select statement has an OFFSET clause
	*/
	public boolean hasOffset() {return offset > -1; }
	/**
	* @return <code>-1</code> if the select statement does not have an OFFSET clause; the value of the offset, otherwise.
	*/
	public long getOffset() {return offset; }
	
	/**
	* Tells if the select statement has a TopN attribute.
	*/
	public boolean hasTopN() {
		return topN != Double.NaN;
	}
	/**
	* Returns the value of the <code>TopN</code> attribute.
	*/
	public double getTopN() {return topN; }
	/**
	* Tells if the value of the <code>TopN</code> attribute is a percentage.
	*/
	public boolean topNIsPercent() {return topNIsPercent; };
	
	/**
	* Returns the data-set operator associated with this select statement.
	*/
	public byte getAssocOperator() {return 0; }
	
	/**
	* Returns the name of the select statement, if any.
	* @return empty string if the select statement does not have a name; the name of the return statement otherwise.
	*/
	public String getName() {
		return EMPTY_STR;
	}
	
	/**
	* Gets the from-table of this select statement, whose alias name is equal to that supplied.
	*/
	public SQLTableRef getTableByAlias(String aliasName) {
		for (int i=0;i<fromTablesCount;i++)
		{
			if (fromTables[i].getAliasName().equalsIgnoreCase(aliasName)) return fromTables[i];
		}
		return null;
	}
	
	/**
	* Recursively gets the from-table of this select statement, whose alias name is equal to that supplied.
	*/
	public SQLTableRef getTableByAliasExt(String tableAlias) {
		SQLTableRef tblRef = getTableByAlias(tableAlias);
		return tblRef != null ? tblRef : parentSelectStmt != null ? parentSelectStmt.getTableByAliasExt(tableAlias) : 
					parentSelectTbl != null && parentSelectTbl.parentSelectStmt != null ? parentSelectTbl.parentSelectStmt.getTableByAliasExt(tableAlias) : null;
	}
	
	/**
	* Returns the number of output columns of this <code>SQLSelectStmt</code>
	*/
	public int getNumberOfOutputColumns() {
		return outputColumnsCount;
	}
	
	/**
	* Returns the number of columns involved in the SELECT part of this <code>SQLSelectStmt</code>
	*/
	public int getNumberOfColumns() {
		return columnsCount;
	}
	
	/**
	* Gets the <code><sup>th</sup></code> column involved in the SELECT part of this <code>SQLSelectStmt</code>
	*/
	public SQLColumn getColumn(int i) {
		return columns[i];
	}
	
	void __addColumn(SQLColumn col) {
		if (parentSelectStmt != null) {
			SQLSelectStmt outputSelectStmt = getOutputSelectStmt();
			if (outputSelectStmt.fromTablesCount < 0) {
				col.outputNumber = outputSelectStmt.outputColumnsCount + 1;
			}
			else if (fromTablesCount < 0) {
				col.localOutputNumber = outputColumnsCount + 1;
			}
		}
		else if (parentSelectTbl != null) {
			SQLSelectStmt outputSelectStmt = parentSelectTbl.getOutputSelectStmt();
			if (outputSelectStmt != null && outputSelectStmt.fromTablesCount < 0) {
				col.outputNumber = outputSelectStmt.outputColumnsCount + 1;
			}
			if (fromTablesCount < 0) {
				col.localOutputNumber = outputColumnsCount + 1;
			}
		}
		else {
			if (fromTablesCount < 0) {
				col.outputNumber = outputColumnsCount + 1;
			}
		}
		
		if (columnsCount == 0) {
			if (columns == null || columns.length == 0) {
				columns = new SQLColumn[12];
			}
			columns[0] = col;
			columnsCount = 1;
			return ;
		}
		if (columnsCount >= columns.length) {
			int newLen = columns.length + (columns.length >>> 1);
			SQLColumn[] cols = new SQLColumn[newLen <= columns.length ? columns.length + 1 : newLen];
			System.arraycopy(columns, 0, cols, 0, columnsCount);
			columns = cols;
		}
		columns[columnsCount++] = col;
	}

	/**
	* Returns the number of from-tables of this select statement.
	*/
	public int getNumOfFromTables() {return fromTablesCount; }
	
	public SQLTableRef getFromTable(int i) {
		return fromTables[i];
	}
	
	/**
	* Tells if this <code>SQLSelectStmt</code> has with-statements associated with it.
	*/
	public boolean hasWithStmts() {
		return withStmtsCount > 0;
	}
	/**
	* Returns the number of <code>with-statements</code> that this <code>SQLSelectStmt</code> has associated with it.<br>
	*/
	public int getNumberOfWithStmts() {
		return withStmtsCount;
	}
	/**
	* Gets the <code>(i + 1)<sup>th</sup></code> that this <code>SQLSelectStmt</code> has associated with it.<br>
	*/
	public SQLSelectStmt getWithStmt (int i) {
		return withStmts[i];
	}
	
	/**
	* Gets the designated with-statement of the with-statements that this <code>SQLSelectStmt</code> has associated with it.<br>
	* @return <code>null</code> if this is associated with no with-statements or none of the associated with-statements has its name equal to the specified name.
	*/
	public SQLSelectStmt getWithStmt(String name) {
		for (int i=0;i<withStmtsCount;i++)
		{
			if (withStmts[i].getName().equalsIgnoreCase(name)) {
				return withStmts[i]; 
			}
		}
		return null;
	}
	
	/**
	* Gets the with-statement for the supplied from-table.<br>
	* @return <code>null</code> this select statement does not have with-statements at all or a matching with-statement for the supplied table-ref.
	*/
	public SQLSelectStmt getWithStmt(SQLTableRef fromTbl) {
		if (fromTbl.isSQLStmtTable()) {
			SQLStmtTable tbl = fromTbl.asSQLStmtTable();
			return getWithStmt(tbl.name);
		}
		else if (fromTbl.isSQLStmtTableAlias()) {
			SQLStmtTableAlias tblAlias = fromTbl.asSQLStmtTableAlias();
			return getWithStmt(tblAlias.of.name);
		}
		return null;
	}
	/**
	* Makes the with-statement ref object for the supplied from-table.<br>
	* @return <code>null</code> this select statement does not have with-statements at all or a matching with-statement for the supplied table-ref.
	*/
	public SQLWithStmtRef makeWithStmtRef(SQLTableRef fromTbl) {
		if (fromTbl.isSQLStmtTable()) {
			SQLStmtTable tbl = fromTbl.asSQLStmtTable();
			SQLSelectStmt withStmt = getWithStmt(tbl.name);
			if (withStmt != null) {
				return new SQLWithStmtRef(withStmt.asSQLWithStmt(), EMPTY_STR/*aliasName*/); 
			}
		}
		else if (fromTbl.isSQLStmtTableAlias()) {
			SQLStmtTableAlias tblAlias = fromTbl.asSQLStmtTableAlias();
			SQLSelectStmt withStmt = getWithStmt(tblAlias.of.name);
			if (withStmt != null) {
				return new SQLWithStmtRef(withStmt.asSQLWithStmt(), tblAlias.aliasName); 
			}
		}
		return null;
	}
	final SQLTableRef __getActualFromTableRef(final SQLTableRef tblRef) {
		if (tblRef.isSQLStmtTable()) {
			SQLStmtTable tbl = tblRef.asSQLStmtTable();
			if (tbl.schema.isEmpty()) {
				SQLSelectStmt withStmt = getWithStmt(tbl.name);
				if (withStmt != null) {
					return new SQLWithStmtRef(withStmt.asSQLWithStmt(), EMPTY_STR);
				}
			}
		}
		else if (tblRef.isSQLStmtTableAlias()) {
			SQLStmtTableAlias tblAlias = tblRef.asSQLStmtTableAlias();
			if (tblAlias.of.schema.isEmpty()) {
				SQLSelectStmt withStmt = getWithStmt(tblAlias.of.name);
				if (withStmt != null) {
					return new SQLWithStmtRef(withStmt.asSQLWithStmt(), tblAlias.aliasName);
				}
			}
		}
		return tblRef;
	}
	void __addFromTable(SQLTableRef fromTbl) {
		fromTbl = __getActualFromTableRef(fromTbl);
		if (fromTablesCount == 0) {
			if (fromTables == null || fromTables.length == 0) {
				fromTables = new SQLTableRef[6];
			}
			fromTables[0] = fromTbl;
			fromTablesCount = 1;
			return ;
		}
		if (fromTablesCount >= fromTables.length) {
			int newLen = fromTables.length + (fromTables.length >>> 1);
			SQLTableRef[] cols = new SQLTableRef[newLen <= fromTables.length ? fromTables.length + 1 : newLen];
			System.arraycopy(fromTables, 0, cols, 0, fromTablesCount);
			fromTables = cols;
		}
		fromTables[fromTablesCount++] = fromTbl;
	}
	
	/**
	* @return getNumberOfWithStmts()</code>
	*/
	public final int getNumOfWithStatements() {
		return getNumberOfWithStmts(); //withStmtsCount; 
	}
	/**
	* @return getWithStmt(i)</code>
	*/
	public final SQLSelectStmt getWithStatement(int i) {
		return getWithStmt(i); //withStmts[i];
	}
	
	void __addWithStatement(SQLSelectStmt col) {
		if (withStmtsCount == 0) {
			if (withStmts == null || withStmts.length == 0) {
				withStmts = new SQLSelectStmt[4];
			}
			withStmts[0] = col;
			withStmtsCount = 1;
			return ;
		}
		if (withStmtsCount >= withStmts.length) {
			int newLen = withStmts.length + (withStmts.length >>> 1);
			SQLSelectStmt[] cols = new SQLSelectStmt[newLen <= withStmts.length ? withStmts.length + 1 : newLen];
			System.arraycopy(withStmts, 0, cols, 0, withStmtsCount);
			withStmts = cols;
		}
		withStmts[withStmtsCount++] = col;
	}
	
	public SQLSelectStmt getParentSelectStmt() {return parentSelectStmt; }
	
	public SQLSelectStmt getOutputSelectStmt() {
		if (parentSelectStmt == null) return this;
		return parentSelectStmt.getOutputSelectStmt();
	}
	
	/**
	* Gets the root select statement.
	*/
	public SQLSelectStmt getRootSelectStmt() {
		if (parentSelectStmt == null) {
			if (parentSelectTbl == null) return this;
			return parentSelectTbl.parentSelectStmt != null ? parentSelectTbl.parentSelectStmt.getRootSelectStmt() : null;
		}
		return parentSelectStmt.getRootSelectStmt();
	}
	
	public int getNumberOfJoins() {return joinsCount; }
	
	public SQLJoin getJoin(int i) {
		return joins[i];
	}
	
	void __addJoin(SQLJoin col) {
		if (joinsCount == 0) {
			if (joins == null || joins.length == 0) {
				joins = new SQLJoin[4];
			}
			joins[0] = col;
			joinsCount = 1;
			return ;
		}
		if (joinsCount >= joins.length) {
			int newLen = joins.length + (joins.length >>> 1);
			SQLJoin[] cols = new SQLJoin[newLen <= joins.length ? joins.length + 1 : newLen];
			System.arraycopy(joins, 0, cols, 0, joinsCount);
			joins = cols;
		}
		joins[joinsCount++] = col;
	}

	public int indexOfColumn(String columnName) {
		for (int i=0;i<columnsCount;i++)
		{
			if (columns[i].getName().equals(columnName)) return -1;
		}
		return -1;
	}
	public int indexOfColumn(String columnName, String ownerTableName) {
		for (int i=0;i<columnsCount;i++)
		{
			SQLColumn col = columns[i];
			if (col.getName().equals(columnName) && col.getTableName().equals(ownerTableName)) return -1;
		}
		return -1;
	}
	
	public int indexOfColumnCI(String columnName) {
		for (int i=0;i<columnsCount;i++)
		{
			if (columns[i].getName().equalsIgnoreCase(columnName)) return -1;
		}
		return -1;
	}
	public int indexOfColumnCI(String columnName, String ownerTableName) {
		for (int i=0;i<columnsCount;i++)
		{
			SQLColumn col = columns[i];
			if (col.getName().equalsIgnoreCase(columnName) && col.getTableName().equalsIgnoreCase(ownerTableName)) return -1;
		}
		return -1;
	}

	
	public int indexOfTable(String tableName, String tableSchema) {
		for (int i=0;i<columnsCount;i++)
		{
			SQLTableRef tbl = fromTables[i];
			if (tbl.getTableName().equals(tableName) && tbl.getSchema().equals(tableSchema)) return -1;
		}
		return -1;
	}
	public int indexOfTableCI(String tableName, String tableSchema) {
		for (int i=0;i<columnsCount;i++)
		{
			SQLTableRef tbl = fromTables[i];
			if (tbl.getTableName().equalsIgnoreCase(tableName) && tbl.getSchema().equalsIgnoreCase(tableSchema)) return -1;
		}
		return -1;
	}
	
	void __trim() {
		if (columns != null && columns.length != columnsCount) {
			SQLColumn[] cols = new SQLColumn[columnsCount];
			System.arraycopy(columns, 0, cols, 0, columnsCount);
			columns = cols;
		}
		if (fromTables != null && fromTables.length != fromTablesCount) {
			SQLTableRef[] cols = new SQLTableRef[fromTablesCount];
			System.arraycopy(fromTables, 0, cols, 0, fromTablesCount);
			fromTables = cols;
		}
		if (joins != null && joins.length != joinsCount) {
			SQLJoin[] cols = new SQLJoin[joinsCount];
			System.arraycopy(joins, 0, cols, 0, joinsCount);
			joins = cols;
		}
	}
	
	protected void __getChars(String tabsIndent, StringBuilder buf) {
		super.__getChars(tabsIndent, buf);
		String tabsIndentP1 = tabsIndent + '\t';
		String tabsIndentP2 = tabsIndentP1 + '\t';
		if (withStmtsCount > 0) {
			buf.append(tabsIndent).append("withStmts: ").append(LN_TERMINATOR);
			for (int i=0;i<withStmtsCount;i++)
			{
				buf.append(tabsIndentP1).append("- ").append(LN_TERMINATOR);
				withStmts[i].__getChars(tabsIndentP2, buf);
			}
		}
		if (selectDistinct) {
			buf.append(tabsIndent).append("distinct: ").append(selectDistinct).append(LN_TERMINATOR);
		}
		if (withInfaJoins) {
			buf.append(tabsIndent).append("withInfaJoins: ").append(withInfaJoins).append(LN_TERMINATOR);
		}
		if (limit > -1) {
			buf.append(tabsIndent).append("limit: ").append(limit).append(LN_TERMINATOR);
			if (offset > -1) {
				buf.append(tabsIndent).append("offset: ").append(offset).append(LN_TERMINATOR);
			}
		}
		buf.append(tabsIndent).append("outputColumnsCount: ").append(outputColumnsCount).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("columns: ").append(LN_TERMINATOR);
		for (int i=0;i<columnsCount;i++)
		{
			buf.append(tabsIndentP1).append("- ").append(LN_TERMINATOR);
			columns[i].__getChars(tabsIndentP2, buf);
		}
		buf.append(tabsIndent).append("fromTables: ").append(LN_TERMINATOR);
		for (int i=0;i<fromTablesCount;i++)
		{
			buf.append(tabsIndentP1).append("- ").append(LN_TERMINATOR);
			fromTables[i].__getChars(tabsIndentP2, buf);
		}
		if (joinsCount > 0) {
			buf.append(tabsIndent).append("joins: ").append(LN_TERMINATOR);
			for (int i=0;i<joinsCount;i++)
			{
				buf.append(tabsIndentP1).append("- ").append(LN_TERMINATOR);
				joins[i].__getChars(tabsIndentP2, buf);
			}
		}
		if (whereClause != null && whereClause.involvedColumnsCount > 0) { //NOTE: in case of INFA joins the SQLWhereClause object may end up having 0 involved columns
			buf.append(tabsIndent).append("whereClause: ").append(LN_TERMINATOR);
			whereClause.__getChars(tabsIndentP1, buf);
		}
		if (groupByClause != null) {
			buf.append(tabsIndent).append("groupByClause: ").append(LN_TERMINATOR);
			groupByClause.__getChars(tabsIndentP1, buf);
		}
		if (havingClause != null) {
			buf.append(tabsIndent).append("havingClause: ").append(LN_TERMINATOR);
			havingClause.__getChars(tabsIndentP1, buf);
		}
		if (orderByClause != null) {
			buf.append(tabsIndent).append("orderByClause: ").append(LN_TERMINATOR);
			orderByClause.__getChars(tabsIndentP1, buf);
		}
	}
	
	public static String getOperatorCode(byte operator) {
//		switch(operator)
//		{
//		case UNION: return "UNION"; 
//		case UNION_ALL: return "UNION_ALL"; 
//		case MINUS: return "MINUS"; 
//		case INTERSECT: return "INTERSECT"; 
//		case EXCEPT: return "EXCEPT"; 
//		}
//		return String.valueOf(operator);
		return SQLStatementCombiningOperators.getOperatorCode(operator);
	}
	
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isSQLSelectStmt() {
		//System.out.println("IN isSQLSelectStmt");
		return true; 
	}
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLSelectStmt asSQLSelectStmt() {
		//System.out.println("IN isSQLSelectStmt");
		return this; 
	}
	/**
	* Gets the designated output column of this <code>SQLColumn</code>.<br>
	*/
	public SQLColumn getColumn(String outputColumnName) {
		for (int i=0;i<columnsCount;i++)
		{
			SQLColumn col = columns[i];
			//System.out.println("\r\nSQLSelectStmt - col: \r\n" + col);
			if (col.aliasName != null && !col.aliasName.isEmpty()) {
				if (col.aliasName.equalsIgnoreCase(outputColumnName)) return col;
			}
			else {
				String colName = col.getName();
				if (colName != null && !colName.isEmpty() && colName.equalsIgnoreCase(outputColumnName)) return col;
			}
		}
		return null;
	} 
	/**
	* Gets the physical column source for the designated output column of this <code>SQLSelectStmt</code>, into the supplied <code>SQLPhysicalColumnSource</code>.<br>
	* If the designated output column is a physical column, this method returns it, else the method recursively looks up for the source physical column, if any.<br>
	* @return <code>false</code> if there's no physical source for this <code>SQLColumn</code>
	*/
	public boolean getPhysicalColumnSourceFor(String outputColumnName, final SQLPhysicalColumnSource output) {
		//System.out.println("SQLSelectStmt - outputColumnName: " + outputColumnName);
		SQLColumn col = getColumn(outputColumnName);
		if (col == null) return false;
		//System.out.println("SQLSelectStmt:::: - outputColumnName: " + outputColumnName + ", col.getLogicalColumnType(): '" + col.getLogicalColumnType() + "'");
		switch (col.getLogicalColumnType())
		{
		case 'N': 
			output.physicalColumn = col;
			output.sourceStmt = this;
			output.physicalColumnLvl++;
			return true;
		case 'T':
			SQLTableRef tblREf = col.getTable();
			if (tblREf == null) {
				throw new IllegalStateException(
				"SQLSelectStmt::getPhysicalColumnSourceFor-1: column marked as being associated with a nested select table but the associated nested select table is not set"
				);
			}
			SQLSelectTable selectTbl = tblREf.isSQLSelectTable() ? tblREf.asSQLSelectTable() : null;
			if (selectTbl == null) {
				if (!tblREf.isSQLStmtTableAlias()) {
					throw new IllegalStateException(
					"SQLSelectStmt::getPhysicalColumnSourceFor-2: column marked as being associated with a nested select table but the associated select table is not a nested select table"
					);
				}
				SQLStmtTableAlias tblAlias = tblREf.asSQLStmtTableAlias();
				if (tblAlias.of == null) {
					throw new IllegalStateException(
					"SQLSelectStmt::getPhysicalColumnSourceFor-3: column marked as being associated with a nested select table but the associated select table is a table alias in illegal state"
					);
				}
				else if (!tblAlias.of.isSQLSelectTable()) {
					throw new IllegalStateException(
					"SQLSelectStmt::getPhysicalColumnSourceFor-4: column marked as being associated with a nested select table but the associated select table is not a nested select table"
					);
				}
				selectTbl = tblAlias.of.asSQLSelectTable();
			}
			//System.out.println("SQLSelectStmt - outputColumnName: " + outputColumnName + ", selectTbl.name: " + selectTbl.name);
			return selectTbl.selectStmt.getPhysicalColumnSourceFor(col.getName(), output);
		case 'W':
			try 
			{
				SQLWithStmtColumnRef withStmtColREf = col.asSQLWithStmtColumnRef();
				switch(withStmtColREf.withStmtColumn.getLogicalColumnType())
				{
				case 'N': 
					output.physicalColumn = withStmtColREf.withStmtColumn;
					output.sourceStmt = this;
					output.physicalColumnLvl++;
					return true;
				case 'Y': 
					return false;		
				}
				return withStmtColREf.withStmt.withStmt.getPhysicalColumnSourceFor(withStmtColREf.withStmtColumn.getName(), output);
			}
			catch(Exception ex)
			{
				throw new IllegalStateException(ex.getMessage() + LN_TERMINATOR + 
				"SQLSelectStmt::getPhysicalColumnSourceFor-5: with-statement's column in illegal state"
				, ex
				);
			}
		case 'Y':
			switch(col.getType())
			{
			case CONSTANT:
			case PSEUDO_COLUMN_REF:
				output.physicalColumn = col;
				output.sourceStmt = this;
				output.physicalColumnLvl++;
				return true;
			}
			return false;
		case 'S': 
			return false;
		}
		return false;
	}
	
	/**
	* Gets the source physical column for the designated output column of this <code>SQLSelectStmt</code>.<br>
	* If the designated output column is a physical column, this method returns it, else the method recursively looks up for the source physical column, if any.<br>
	*/
	public SQLColumn getPhysicalColumnFor(String outputColumnName) {
		//System.out.println("SQLSelectStmt - outputColumnName: " + outputColumnName);
		SQLColumn col = getColumn(outputColumnName);
		if (col == null) return null;
		//System.out.println("SQLSelectStmt:::: - outputColumnName: " + outputColumnName + ", col.getLogicalColumnType(): '" + col.getLogicalColumnType() + "'");
		switch (col.getLogicalColumnType())
		{
		case 'N': return col;
		case 'T':
			SQLTableRef tblREf = col.getTable();
			if (tblREf == null) {
				throw new IllegalStateException(
				"SQLSelectStmt::getPhysicalColumnFor-1: column marked as being associated with a nested select table but the associated nested select table is not set"
				);
			}
			SQLSelectTable selectTbl = tblREf.isSQLSelectTable() ? tblREf.asSQLSelectTable() : null;
			if (selectTbl == null) {
				if (!tblREf.isSQLStmtTableAlias()) {
					throw new IllegalStateException(
					"SQLSelectStmt::getPhysicalColumnFor-2: column marked as being associated with a nested select table but the associated select table is not a nested select table"
					);
				}
				SQLStmtTableAlias tblAlias = tblREf.asSQLStmtTableAlias();
				if (tblAlias.of == null) {
					throw new IllegalStateException(
					"SQLSelectStmt::getPhysicalColumnFor-3: column marked as being associated with a nested select table but the associated select table is a table alias in illegal state"
					);
				}
				else if (!tblAlias.of.isSQLSelectTable()) {
					throw new IllegalStateException(
					"SQLSelectStmt::getPhysicalColumnFor-4: column marked as being associated with a nested select table but the associated select table is not a nested select table"
					);
				}
				selectTbl = tblAlias.of.asSQLSelectTable();
			}
			//System.out.println("SQLSelectStmt - outputColumnName: " + outputColumnName + ", selectTbl.name: " + selectTbl.name);
			return selectTbl.selectStmt.getPhysicalColumnFor(col.getName());
		case 'W':
			try 
			{
				SQLWithStmtColumnRef withStmtColREf = col.asSQLWithStmtColumnRef();
				switch(withStmtColREf.withStmtColumn.getLogicalColumnType())
				{
				case 'N': return withStmtColREf.withStmtColumn;
				case 'Y': return null;		
				}
				return withStmtColREf.withStmt.withStmt.getPhysicalColumnFor(withStmtColREf.withStmtColumn.getName());
			}
			catch(Exception ex)
			{
				throw new IllegalStateException(ex.getMessage() + LN_TERMINATOR + 
				"SQLSelectStmt::getPhysicalColumnFor-5: with-statement's column in illegal state"
				, ex
				);
			}
		case 'Y':
			switch(col.getType())
			{
			case CONSTANT:
			case PSEUDO_COLUMN_REF:
				return col;
			}
			return null;
		case 'S': 
			return null;
		}
		return null;
	}
	/**
	* Tells if this <code>SQLSelectStmt</code> is an instance of class {@link SQLWithStmt SQLWithStmt}.<br>
	*/
	public boolean isSQLWithStmt() {return true; }
	/**
	* @throws ClassCastException if this <code>SQLSelectStmt</code> is not an instance of class {@link SQLWithStmt SQLWithStmt}.<br>
	*/
	public SQLWithStmt asSQLWithStmt() {return (SQLWithStmt)this; }

}