package expr.sql;

/**
* Base class for providing support for SQL widgets.<br>
* This class and its sub-classes are primarily meant for parsing SQL statements and working out list of involved columns in sql statement.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public abstract class SQLWidget implements ISQLWidgetConstants, java.io.Serializable {
//	
//	protected static final String EMPTY_STR = "";
//	
//	/**
//	* Constant for SQL widget type <code>COLUMN_REF</code>
//	*/
//	public static final byte COLUMN_REF = 1;
//	/**
//	* Constant for SQL widget type <code>CONSTANT</code>
//	*/
//	public static final byte CONSTANT = 2;
//	/**
//	* Constant for SQL widget type <code>EXPR</code>
//	*/
//	public static final byte EXPR = 3;
//	/**
//	* Constant for SQL widget type <code>SELECT_STMT_COLUMN</code>
//	*/
//	public static final byte SELECT_STMT_COLUMN = 4;
//	/**
//	* Constant for SQL widget type <code>TABLE_REF</code>
//	*/
//	public static final byte TABLE_REF = 5;
//	/**
//	* Constant for SQL widget type <code>JOIN</code>
//	*/
//	public static final byte JOIN = 6;
//	/**
//	* Constant for SQL widget type <code>WHERE_CLAUSE</code>
//	*/
//	public static final byte WHERE_CLAUSE = 7;
//	/**
//	* Constant for SQL widget type <code>GROUP_BY_CLAUSE</code>
//	*/
//	public static final byte GROUP_BY_CLAUSE = 8;
//	/**
//	* Constant for SQL widget type <code>ORDER_BY_CLAUSE</code>
//	*/
//	public static final byte ORDER_BY_CLAUSE = 9;
//	/**
//	* Constant for SQL widget type <code>WITH_STATEMENT</code>
//	*/
//	public static final byte WITH_STATEMENT = 10;
//	/**
//	* Constant for SQL widget type <code>SELECT_STATEMENT</code>
//	*/
//	public static final byte SELECT_STATEMENT = 11;
//	/**
//	* Constant for SQL widget type <code>SET_STATEMENT</code>
//	*/
//	public static final byte SET_STATEMENT = 12;
//	/**
//	* Constant for SQL widget type <code>WITH_STMT_COLUMN_REF</code>
//	*/
//	public static final byte WITH_STMT_COLUMN_REF = 13;
//	/**
//	* Constant for SQL widget type <code>CASE_EXPR</code>
//	*/
//	public static final byte CASE_EXPR = 14;
//	/**
//	* Constant for SQL widget type <code>FUNC_EXPR</code>
//	*/
//	public static final byte FUNC_EXPR = 15;
//	/**
//	* Constant for SQL widget type <code>PARENTH_EXPR</code>
//	*/
//	public static final byte PARENTH_EXPR = 16;
//	/**
//	* Constant for SQL widget type <code>EXISTS_CONDITION_COLUMN</code>
//	*/
//	public static final byte EXISTS_CONDITION_COLUMN = 17;
//	/**
//	* Constant for SQL widget type <code>IN_SELECT_COLUMN</code>
//	*/
//	public static final byte IN_SELECT_COLUMN = 18;
//	/**
//	* Constant for SQL widget type <code>HAVING_CLAUSE</code>
//	*/
//	public static final byte HAVING_CLAUSE = 19;
//	/**
//	* Constant for SQL widget type <code>PARAMETER_COLUMN</code>
//	*/
//	public static final byte PARAMETER_COLUMN = 20;
//	/**
//	* Constant for SQL widget type <code>PARAMETER_TABLE_REF</code>
//	*/
//	public static final byte PARAMETER_TABLE_REF = 21;
//	/**
//	* Constant for SQL widget type <code>FUNC_ANALYTIC_CLAUSE</code>
//	*/
//	public static final byte FUNC_ANALYTIC_CLAUSE = 22;
//	/**
//	* Constant for SQL widget type <code>SQL_INFA_JOINS</code>
//	*/
//	public static final byte SQL_INFA_JOINS = 23;
//	/**
//	* Constant for SQL widget type <code>PSEUDO_COLUMN_REF</code>
//	*/
//	public static final byte PSEUDO_COLUMN_REF = 24; //e.g. ROWNUM, ROWID, SYSDATE, OBJECT_ID, SYS_NC_OID$(old name for OBJECT_ID), LEVEL, CURRENT_DATE, CURRENT_TIMESTAMP, SESSIONTIMEZONE, USER in oracle
//	/**
//	* Constant for SQL widget type <code>SEQUENCE_VALUE_REF</code>
//	*/
//	public static final byte SEQUENCE_VALUE_REF = 25; //e.g. sequence.CURRVAL, sequence.NEXTVAL, schema.sequence.CURRVAL, schema.sequence.NEXTVAL, schema.sequence.CURRVAL@dblink, schema.sequence.NEXTVAL@dblink
//	
//	public static final byte EXPR_RIGHT_OPERAND = 26;
//	
//	/**
//	* Combination of flags for the widget that hold values.<br>
//	*/
//	public static final int VALUE_WIDGETS = (1 << COLUMN_REF) | 
//												(1 << CONSTANT) | 
//												(1 << EXPR) | 
//												(1 << SELECT_STMT_COLUMN) | 
//												(1 << CASE_EXPR) | 
//												(1 << FUNC_EXPR) | 
//												(1 << PARENTH_EXPR) | 
//												(1 << EXISTS_CONDITION_COLUMN) | 
//												(1 << IN_SELECT_COLUMN) | 
//												(1 << PARAMETER_COLUMN) | 
//												(1 << PSEUDO_COLUMN_REF) | 
//												(1 << SEQUENCE_VALUE_REF);
//	
//	
//	public static final byte SCHEMA_PARAM = 1; 
//	public static final byte NAME_PARAM = 2; 
//	public static final byte SCHEMA_AND_NAME_PARAMS = SCHEMA_PARAM | NAME_PARAM;
//	
//	public static final String FROM_TABLE_GROUP = "FROM_TABLE";
//	
//	public static final String SELECT_GRP_NAME = "SELECT";
//	public static final String FROM_GRP_NAME = "FROM";
//	public static final String WHERE_GRP_NAME = "WHERE";
//	public static final String JOIN_GRP_NAME = "JOIN";
//	public static final String GROUP_BY_GRP_NAME = "GROUP_BY";
//	public static final String HAVING_GRP_NAME = "HAVING";
//	public static final String ORDER_BY_GRP_NAME = "ORDER_BY";
//	public static final String FUNC_ANALYTIC_CLAUSE_SUBGRP_NAME = "ANALYTIC_CLAUSE";
//	public static final String EXPR_SUBGRP_NAME = "EXPR";
//	public static final String CASE_EXPR_SUBGRP_NAME = "CASE_EXPR";
//	public static final String FUNC_EXPR_SUBGRP_NAME = "FUNC_EXPR";
//	public static final String PARENTH_EXPR_SUBGRP_NAME = "PARENTH_EXPR";
//	
//	public static final String SELECT_STMT_COLUMN_SUBGRP_NAME = "SELECT_STMT_COLUMN";
//	public static final String EXISTS_CONDITION_COLUMN_SUBGRP_NAME = "EXISTS_CONDITION_COLUMN";
//	public static final String IN_SELECT_COLUMN_SUBGRP_NAME = "IN_SELECT_COLUMN";
//	
//	/**
//	* tag indicating DESC NULLS LAST sort rule - to be set as the alias name of the order-by column
//	*/
//	public static final String DESC_NL_TAG = "DESC:L"; 
//	/**
//	* tag indicating DESC NULLS FIRST sort rule - to be set as the alias name of the order-by column
//	*/
//	public static final String DESC_NF_TAG = "DESC:F"; 
//	/**
//	* tag indicating ASC NULLS LAST sort rule - to be set as the alias name of the order-by column
//	*/
//	public static final String ASC_NL_TAG = "ASC:L"; 
//	/**
//	* tag indicating ASC NULLS FIRST sort rule - to be set as the alias name of the order-by column
//	*/
//	public static final String ASC_NF_TAG = "ASC:F"; 
//	/**
//	* tag indicating DESC sort rule
//	*/
//	public static final String DESC_TAG = DESC_NL_TAG.substring(0, 4); 
//	/**
//	* tag indicating ASC sort rule
//	*/
//	public static final String ASC_TAG = ASC_NL_TAG.substring(0, 3); 
//	
//	
//	//public static final byte ANALYTIC_FUNC_EXPR = 23;//commented because abandoned as using it requires to replace existing fnction with an instance of this class upon the spotting of an anlytic clause; rather enhance SQLFuncExprColumn for both analytic and non analytic usages of functions.
//	
//	static final String LN_TERMINATOR = java.lang.System.lineSeparator();
//
	/**
	* Constructor.
	*/
	/*public */SQLWidget() {
		super();
	}
	
	/**
	* Returns the type of the SQL widget.
	*/
	public abstract byte getType();
	/**
	* Tells if this SQL widget is a column reference.
	*/
	public abstract boolean isColumnRef();
	/**
	* Tells if this SQL widget is a constant/literal.
	*/
	public abstract boolean isConstant();
	/**
	* Tells if this SQL widget is an expression.
	*/
	public abstract boolean isExpr();
	/**
	* Tells if this SQL widget is a select-statement column.
	*/
	public abstract boolean isSelectStmtColumn();
	/**
	* Tells if this SQL widget is a table reference.
	*/
	public abstract boolean isTableRef();
	/**
	* Tells if this SQL widget is a where clause.
	*/
	public abstract boolean isWhereClause();
	
	public boolean isHavingClause() {return false; }
	
	public boolean isGroupByClause() {return false; }
	
	public boolean isOrderByClause() {return false; }
	/**
	* Tells if this SQL widget is an SQL join.
	*/
	public abstract boolean isJoin();
	/**
	* Tells if this SQL widget is an SQL statement.
	*/
	public abstract boolean isStatement();
	/**
	* Tells if this SQL widget is an SQL statement involving data-set operators (MINUS, UNION, Etc...).
	*/
	public boolean isCombiningStatement() {return false; }
	
	public boolean isParameterColumn() {return false; }
	
	public boolean isParameterTableRef() {return false; }
	
	
	/**
	* Base class for providing support for SQL widgets of kind <code>COLUMN</code>.<br>
	*
	* @author Marc E. KAMGA
	* @version 1.0
	* @copyright Right to use and modify given to <code>Michelin</code> corporation
	*/
	public static abstract class SQLColumn extends SQLWidget { 
		
		protected String aliasName;
		protected int outputNumber;
		protected int localOutputNumber;
		
		
		/*protected */SQLColumn() {
			super();
			this.aliasName = EMPTY_STR;
			this.localOutputNumber = -1;
		}
		/*protected */SQLColumn(String aliasName) {
			super();
			this.aliasName = aliasName == null ? EMPTY_STR : aliasName;
			this.localOutputNumber = -1;
		}
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isTableRef() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isWhereClause() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isStatement() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isJoin() {return false; }
		
		
		boolean isRaw() {return false; }
		
		/**
		* Returns the {@link SQLTableRef SQLTableRef} for this {@link SQLColumn SQLColumn}, if any.
		*/
		public SQLTableRef getTable() {return null; }
		/**
		* Returns the with-statement acting as table, if any.<br>
		* For columns of type {@link #WITH_STMT_COLUMN_REF WITH_STMT_COLUMN_REF}, this method and method {@link #getTable() getTable()} return the same result.<br>
		*/
		public SQLWithStmtRef getWithStmt() {return null; }
		
		/**
		* Returns the name of the physical table this <code>SQLColumn</code> is directly bound to.
		*/
		public String getTableName() {return EMPTY_STR; }
		/**
		* Gets the alias of the table this <code>SQLColumn</code> is directly bound to.
		*/
		public String getTableAlias() {return EMPTY_STR; }
		/**
		* Returns the (physical?) name of the column.
		*/
		public String getName() {return EMPTY_STR; }
		/**
		* Tells if the statement column has an alias name.
		*/
		public boolean hasAliasName() {return !aliasName.isEmpty(); }
		/**
		* Returns the alias name of the column.
		*/
		public String getAliasName() {return aliasName; }
		
		/**
		* Returns the output name of the column.
		* @return <code>aliasName.isEmpty() ? getName() : aliasName</code>
		*/
		public String getOutputName() {return aliasName.isEmpty() ? getName() : aliasName; }
		
		/**
		* Returns the associated/source column of this <code>SQLColumn</code>
		*/
		public SQLColumn getAssociatedColumm() {
			return null;
		}
		
		/**
		* {@inheritDoc}
		*/
		public boolean isLogicalColumn() {
			return false;
		}
		/**
		* {@inheritDoc}
		* @return {@code 'N'} for not-a-logical column; {@code 'W'} for column sourced from/bounded to a with-statement; {@code 'T'} for column sourced from/bounded to a nested select table; {@code 'S'} for select-statement column, in-select column or exists-condition-column; {@code 'Y'} for other types of logical column, otherwise.
		*/
		public char getLogicalColumnType() {
			return 'N';
		}
		/**
		* Gets the (source) physical column, literal-column or pseudo-column for this <code>SQLColumn</code>.<br>
		*/
		public SQLColumn getPhysicalColumn() {
			return null;
		}
		/**
		* Gets the physical column source, literal-column source or pseudo-column source for this <code>SQLColumn</code>.<br>
		* return <code>0</code> if this column is already a physical column; <code>-1</code> if the there's no physical column source for this <code>SQLColumn</code>, <code>0</code>, otherwise.
		*/
		public byte getPhysicalColumnSource(final SQLPhysicalColumnSource output) {
			return -1;
		}
		
		
		/**
		* Tells if this <code>SQLColumn</code> is an instance of {@link SQLWithStmtColumnRef SQLWithStmtColumnRef}.
		*/
		public boolean isSQLWithStmtColumnRef() {return true; }
		/**
		* @throws ClassCastException if this <code>SQLColumn</code> is not an instance of {@link SQLWithStmtColumnRef SQLWithStmtColumnRef}.
		*/
		public SQLWithStmtColumnRef asSQLWithStmtColumnRef() {return (SQLWithStmtColumnRef)this; }
		
		/**
		* Gets the allocated ETL id/number
		*/
		public long getEtlRowWid() {
			return 0x8000000000000000L;
		}
		void __setEtlRowWid(long etlRowWid) {
			//DOES NOTHING BY DEFAULT, NEEDS OVERRIDING BY SUB-CLASSES, IF RELEVANT
		}
		
		/**
		* Returns the default top-expression sub-group of this <code>SQLColumn</code>
		*/
		public String getDefaultTopExprSubGroup() {return EMPTY_STR; }
		
		
		protected void __getChars(String tabsIndent, StringBuilder buf) {
			super.__getChars(tabsIndent, buf);
			buf.append(tabsIndent).append("physicalName: ").append(getName()).append(LN_TERMINATOR);
			buf.append(tabsIndent).append("aliasName: ").append(getAliasName()).append(LN_TERMINATOR);
			buf.append(tabsIndent).append("outputNumber: ").append(outputNumber).append(LN_TERMINATOR);
			if (localOutputNumber > -1) {
				buf.append(tabsIndent).append("localOutputNumber: ").append(localOutputNumber).append(LN_TERMINATOR);
			}
			SQLTableRef table = getTable();
			if (table != null) {
				if (table instanceof SQLSelectTable) {
					buf.append(tabsIndent).append("table: ").append(((SQLSelectTable)table).name).append(LN_TERMINATOR);
				}
				else {
					buf.append(tabsIndent).append("table: ").append(LN_TERMINATOR);
					table.__getChars(tabsIndent + '\t', buf);
				}
			}
			else {
				buf.append(tabsIndent).append("table: ").append(getTableName()).append(LN_TERMINATOR);
			}
			__appendMoreColumnInfo(tabsIndent/* + '\t'*/, buf);
		}
		
		protected void __appendMoreColumnInfo(String tabsIndent, StringBuilder buf) {
			
		}
		
		/**
		* Tells if this <code>SQLColumn</code> is an instance of class {@link SQLParameterColumn SQLParameterColumn}.
		*/
		public boolean isSQLParameterColumn() {return false; }
		
		public SQLParameterColumn asSQLParameterColumn() {return (SQLParameterColumn)this;}
		/**
		* Tells if this <code>SQLIndexedParameterColumn</code> is an instance of class {@link SQLIndexedParameterColumn SQLIndexedParameterColumn}.
		*/
		public boolean isSQLIndexedParameterColumn() {return false; }
		
		public SQLIndexedParameterColumn asSQLIndexedParameterColumn() {return (SQLIndexedParameterColumn)this;}
		/**
		* Tells if this <code>SQLColumn</code> is an instance of class {@link SQLSelectStmtColumn SQLSelectStmtColumn}.
		*/
		public boolean isSQLSelectStmtColumn() {return false; }
		
		public SQLSelectStmtColumn asSQLSelectStmtColumn() {return (SQLSelectStmtColumn)this;}
		/**
		* Tells if this <code>SQLColumn</code> is an instance of class {@link SQLExprColumn SQLExprColumn}.
		*/
		public boolean isSQLExprColumn() {return false; }
		
		public SQLExprColumn asSQLExprColumn() {return (SQLExprColumn)this;}
		
		/**
		* Tells if this <code>SQLColumn</code> is an instance of class {@link SQLExpression SQLExpression}.
		*/
		public boolean isSQLExpression() {return false; }
		
		public SQLExpression asSQLExpression() {return (SQLExpression)this;}
		
		/**
		* Tells if this <code>SQLColumn</code> is an instance of class {@link SQLBooleanExpression SQLBooleanExpression}.
		*/
		public boolean isSQLBooleanExpression() {return false; }
		
		public SQLBooleanExpression asSQLBooleanExpression() {return (SQLBooleanExpression)this;}
		
		/**
		* Tells if this <code>SQLColumn</code> is an instance of class {@link SQLArithmeticExpression SQLArithmeticExpression}.
		*/
		public boolean isSQLArithmeticExpression() {return false; }
		
		public SQLArithmeticExpression asSQLArithmeticExpression() {return (SQLArithmeticExpression)this;}
		
		
		/**
		* Tells if this <code>SQLColumn</code> is an instance of class {@link SQLRawColumnRef SQLRawColumnRef}.
		*/
		public boolean isSQLRawColumnRef() {return false; }
		/**
		* @throws ClassCastException if this <code>SQLColumn</code> is not an instance of class {@link SQLRawColumnRef SQLRawColumnRef}.
		*/
		public SQLRawColumnRef asSQLRawColumnRef() {return (SQLRawColumnRef)this; }
		/**
		* Tells if this <code>SQLColumn</code> is an instance of class {@link SQLColumnRef SQLColumnRef}.
		*/
		public boolean isSQLColumnRef() {return false; }
		/**
		* @throws ClassCastException if this <code>SQLColumn</code> is not an instance of class {@link SQLColumnRef SQLColumnRef}.
		*/
		public SQLColumnRef asSQLColumnRef() {return (SQLColumnRef)this; }
		
		/**
		* @return <code>1</code> if the expression is known to be boolean; <code>-1</code> if the expression is known to not be a boolean expression; <code>0</code>, otherwise, to indicate that it remains to be determined
		*/
		public byte isBooleanExpression() {
			return 0;
		}
		/**
		* @return <code>1</code> if the expression is known to be an arithmetic expression; <code>-1</code> if the expression is known to not be an arithmetic expression; <code>0</code>, otherwise, to indicate that it remains to be determined
		*/
		public byte isArithmeticExpression() {
			return 0;
		}
		
		/**
		* Tells if this <code>SQLColumn</code> is an instance of {@link SQLLiteral SQLLiteral} class.<br>
		*/
		public boolean isSQLLiteral() {
			return false; 
		}
		/**
		* @throws ClassCastException if this <code>SQLColumn</code> is not an instance of {@link SQLLiteral SQLLiteral} class.<br>
		*/
		public SQLLiteral asSQLLiteral() {
			return (SQLLiteral)this; 
		}
		
		
	}
	
	
	/**
	* Base class for providing support for SQL widgets of kind <code>PARAMETER</code>.<br>
	*
	* @author Marc E. KAMGA
	* @version 1.0
	* @copyright Right to use and modify given to <code>Michelin</code> corporation
	*/
	public static abstract class SQLParamColumn extends SQLColumn {
		
		public static final byte INDEXED_PARAMETER = 1;
		
		public static final byte NAMED_PARAMETER = 2;
		
		
		SQLParamColumn() {
			super();
		}
		SQLParamColumn(String aliasName) {
			super(aliasName);
		}
		/**
		* Returns the kind of this parameter
		*/
		public abstract byte getKind();
		
		protected void __prependMoreInfo(String tabsIndent, StringBuilder buf) {
			switch(getKind())
			{
			case INDEXED_PARAMETER: 
				buf.append(tabsIndent).append("kind: ").append("INDEXED_PARAMETER").append(LN_TERMINATOR);
				break ;
			case NAMED_PARAMETER: 
				buf.append(tabsIndent).append("kind: ").append("NAMED_PARAMETER").append(LN_TERMINATOR);
				break ;
			default: 
				buf.append(tabsIndent).append("kind: ").append(getKind()).append(LN_TERMINATOR);
				break ;
			}
		}

		/**
		* {@inheritDoc}
		* @return {@link #PARAMETER_COLUMN PARAMETER_COLUMN}
		*/
		public final byte getType() {
			return PARAMETER_COLUMN;
		}
		/**
		* {@inheritDoc}
		* @return {@code true}
		*/
		public final boolean isParameterColumn() {return true; }
		/**
		* {@inheritDoc}
		* @return {@code false}
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
		
		public final boolean isSelectStmtColumn() {return false; }
		/**
		* {@inheritDoc}
		* @return {@code true}
		*/
		public final boolean isLogicalColumn() {
			return true;
		}	
		
	}
	
	
	/**
	* Base class for providing support for SQL widgets of kind <code>STATEMENT</code>.<br>
	*
	* @author Marc E. KAMGA
	* @version 1.0
	* @copyright Right to use and modify given to <code>Michelin</code> corporation
	*/
	public static abstract class SQLStatement extends SQLWidget {
		
		/**
		* meant to hold the row wid allocated by ETL widget sql statement analyze wizard
		*/
		long etlRowWid;
		
		/*protected */SQLStatement() {
			super();
		}
		
		/**
		* Returns the row wid allocated by ETL widget sql statement analyze wizard
		*/
		public long getEtlRowWid() {
			return etlRowWid;
		}
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isColumnRef() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isConstant() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isExpr() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isSelectStmtColumn() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isTableRef() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isWhereClause() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>true</code>
		*/
		public final boolean isStatement() {return true; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isJoin() {return false; }
		
		/**
		* Tells if this statement is an instance of {@link SQLSelectStmt SQLSelectStmt} class.
		*/
		public boolean isSQLSelectStmt() {
			return false; 
		}
		/**
		* @throws ClassCastException if this statement is not an instance of {@link SQLSelectStmt SQLSelectStmt} class.
		*/
		public SQLSelectStmt asSQLSelectStmt() {
			return (SQLSelectStmt)this; 
		}
		
		
		/**
		* Tells if this statement is an instance of {@link SQLCombiningStatement SQLCombiningStatement} class.
		*/
		public boolean isSQLCombiningStatement() {
			return false; 
		}
		/**
		* @throws ClassCastException if this statement is not an instance of {@link SQLCombiningStatement SQLSelectStmt} class.
		*/
		public SQLCombiningStatement asSQLCombiningStatement() {
			return (SQLCombiningStatement)this; 
		}
		
		
	}
	/**
	* Base class for providing support for SQL widgets of kind <code>TABLE_REF</code>.<br>
	*
	* @author Marc E. KAMGA
	* @version 1.0
	* @copyright Right to use and modify given to <code>Michelin</code> corporation
	*/
	public static abstract class SQLTableRef extends SQLWidget implements ISQLTableRefTypes {
//		
//		
//		public static final byte STMT_TABLE = 1;
//		
//		public static final byte STMT_TABLE_ALIAS = 2;
//		
//		public static final byte SELECT_TABLE = 3;
//		
//		public static final byte WITH_STMT_TABLE_REF = 4;
//		
//		public static final byte PARAM_TABLE_REF = 5;
//		
//		public static final byte JOIN_TABLE_REF = 6;
//		
		protected long etlRowWid;
		
		/**
		* Constructor.<br>
		*/
		SQLTableRef() {
		
		}
		
		/**
		* Tells if this <code>SQLTableRef</code> is/is backed by an SQL query.
		*/
		public abstract boolean isSQLQuery();
		
		public abstract boolean isAlias();
		/**
		* Returns the name of the (physical) schema of this <code>SQLTableRef</code>, if any.
		*/
		public abstract String getSchema();
		/**
		* Returns the (physical?) name of this <code>SQLTableRef</code>, if any.
		*/
		public abstract String getTableName();
		/**
		* Returns the alias name of this <code>SQLTableRef</code>.
		*/
		public String getAliasName() {return getTableName(); }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isColumnRef() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isConstant() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isExpr() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isSelectStmtColumn() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isTableRef() {return true; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isWhereClause() {return false; }
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final boolean isStatement() {return false; }
		/**
		* Tells if this <code>SQLTableRef</code> is a statement-ref.<br>
		*/
		public boolean isWithStmtRef() {return false; }
		/**
		* {@inheritDoc}
		*/
		public /*final */boolean isJoin() {return false; }
		/**
		* {@inheritDoc}
		*/
		public /*final */byte getType() {return TABLE_REF; }
		
		/**
		* Returns the sub-type of this <code>SQLTableRef</code>.<br>
		* @see ISQLTableRefTypes ISQLTableRefTypes
		*/
		public byte getKind() {return 0; }
		
		
		protected void __getChars(String tabsIndent, StringBuilder buf) {
			super.__getChars(tabsIndent, buf);
			buf.append(tabsIndent).append("kind: "); //since 2017-06-19
			SQLTableRefTypes.appendTableRefType(getKind(), buf);
			buf.append(LN_TERMINATOR);
			if (!isSQLQuery()) {
				buf.append(tabsIndent).append("schema: ").append(getSchema()).append(LN_TERMINATOR);
				buf.append(tabsIndent).append("tableName: ").append(getTableName()).append(LN_TERMINATOR);
			}
			buf.append(tabsIndent).append("aliasName: ").append(getAliasName()).append(LN_TERMINATOR);
			__appendMoreTableInfo(tabsIndent/* + '\t'*/, buf);
		}
		
		protected void __appendMoreTableInfo(String tabsIndent, StringBuilder buf) {
			
		}
		/**
		* Tells if this <code>SQLWidget</code> is an instance of {@link SQLStmtTable SQLStmtTable} class.<br>
		*/
		public boolean isSQLStmtTable() {return false; }
		
		public SQLStmtTable asSQLStmtTable() {return (SQLStmtTable)this; }
		
		/**
		* Tells if this <code>SQLWidget</code> is an instance of {@link SQLStmtTableAlias SQLStmtTableAlias} class.<br>
		*/
		public boolean isSQLStmtTableAlias() {return false; }
		/**
		* @throws ClassCastException if this <code>SQLWidget</code> is not an instance of {@link SQLStmtTableAlias SQLStmtTableAlias} class.<br>
		*/
		public SQLStmtTableAlias asSQLStmtTableAlias() {return (SQLStmtTableAlias)this; }
		
		/**
		* Tells if this <code>SQLWidget</code> is an instance of {@link SQLSelectTable SQLSelectTable} class.<br>
		*/
		public boolean isSQLSelectTable() {return false; }
		/**
		* @throws ClassCastException if this <code>SQLWidget</code> is not an instance of {@link SQLSelectTable SQLSelectTable} class.<br>
		*/
		public SQLSelectTable asSQLSelectTable() {return (SQLSelectTable)this; }
		
		/**
		* Tells if this <code>SQLWidget</code> is an instance of {@link SQLSelectTable SQLSelectTable} class.<br>
		*/
		public boolean isSQLWithStmtRef() {return false; }
		/**
		* @throws ClassCastException if this <code>SQLWidget</code> is not an instance of {@link SQLWithStmtRef SQLWithStmtRef} class.<br>
		*/
		public SQLWithStmtRef asSQLWithStmtRef() {return (SQLWithStmtRef)this; }
		
		/**
		* Tells if this <code>SQLWidget</code> is an instance of {@link SQLParameterTableRef SQLParameterTableRef} class.<br>
		*/
		public boolean isSQLParameterTableRef() {return false; }
		/**
		* @throws ClassCastException if this <code>SQLWidget</code> is not an instance of {@link SQLParameterTableRef SQLParameterTableRef} class.<br>
		*/
		public SQLParameterTableRef asSQLParameterTableRef() {return (SQLParameterTableRef)this; }
		
		/**
		* Tells if the table-ref is actually a nested select table or a ref to a nested select table.<br>
		*/
		public boolean isNestedTableRef() {return false; }
		
		/**
		* Tells if this <code>SQLWidget</code> is an instance of {@link SQLCombiningSelectTable SQLCombiningSelectTable} class.<br>
		*/
		public boolean isSQLCombiningSelectTable() {return false; }
		/**
		* @throws ClassCastException if this <code>SQLWidget</code> is not an instance of {@link SQLCombiningSelectTable SQLCombiningSelectTable} class.<br>
		*/
		public SQLCombiningSelectTable asSQLCombiningSelectTable() {return (SQLCombiningSelectTable)this; }
		
		/**
		* Tells if this <code>SQLWidget</code> is an instance of {@link SQLSelectTbl SQLSelectTbl} class.<br>
		*/
		public boolean isSQLSelectTbl() {return false; }
		/**
		* @throws ClassCastException if this <code>SQLWidget</code> is not an instance of {@link SQLSelectTbl SQLSelectTbl} class.<br>
		*/
		public SQLSelectTbl asSQLSelectTbl() {return (SQLSelectTbl)this; }
		
		/**
		* {@inheritDoc}
		* @return <code>true</code>
		*/
		public final boolean isSQLTableRef() {
			return true; 
		} 
		/**
		* {@inheritDoc}
		* @return <code>false</code>
		*/
		public final SQLTableRef asSQLTableRef() {return this; }
	
	}
	
	/**
	* Tells if this <code>SQLWidget</code> is an instance of {@link SQLTableRef SQLTableRef} class.<br>
	*/
	public boolean isSQLTableRef() {return false; }
	/**
	* @throws ClassCastException if this <code>SQLWidget</code> is not an instance of {@link SQLTableRef SQLTableRef} class.<br>
	*/
	public SQLTableRef asSQLTableRef() {return (SQLTableRef)this; }
	
	
	boolean isSQLCaseExprColumn() {
		return false;
	}
	
	void __trim() {}
	
	static final void __appendTabs(int tabsIndents, StringBuilder buf) {
		if (tabsIndents < 1) return ;
		char[] tabs = new char[tabsIndents];
		java.util.Arrays.fill(tabs, '\t');
		buf.append(tabs);
	}
	
	protected void __prependMoreInfo(String tabsIndent, StringBuilder buf) {
		
	}
	
	protected void __getChars(String tabsIndent, StringBuilder buf) {
		buf.append(tabsIndent).append("type: ").append(getTypeCode(getType())).append(LN_TERMINATOR);
		__prependMoreInfo(tabsIndent, buf);
	}
	
	/**
	* Returns the textual representation of this <code>SQLWidget</code>.
	*/
	public String toString() {
		StringBuilder buf = new StringBuilder(50);
		__getChars(EMPTY_STR, buf);
		return buf.toString();
	}
	
	public static final String getTypeCode(byte type) {
//		switch(type)
//		{
//		case COLUMN_REF: return "COLUMN_REF"; 
//		case CONSTANT: return "CONSTANT"; 
//		case EXPR: return EXPR_SUBGRP_NAME; 
//		case SELECT_STMT_COLUMN: return SELECT_STMT_COLUMN_SUBGRP_NAME; //"SELECT_STMT_COLUMN"; 
//		case TABLE_REF: return "TABLE_REF"; 
//		case JOIN: return JOIN_GRP_NAME; //"JOIN"; 
//		case WHERE_CLAUSE: return "WHERE_CLAUSE"; 
//		case GROUP_BY_CLAUSE: return "GROUP_BY_CLAUSE"; 
//		case ORDER_BY_CLAUSE: return "ORDER_BY_CLAUSE"; 
//		case WITH_STATEMENT: return "WITH_STATEMENT"; 
//		case SELECT_STATEMENT: return "SELECT_STATEMENT"; 
//		case SET_STATEMENT: return "SET_STATEMENT"; 
//		case WITH_STMT_COLUMN_REF: return "WITH_STMT_COLUMN_REF"; 
//		case CASE_EXPR: return CASE_EXPR_SUBGRP_NAME; 
//		case FUNC_EXPR: return FUNC_EXPR_SUBGRP_NAME; 
//		case PARENTH_EXPR: return PARENTH_EXPR_SUBGRP_NAME; 
//		case EXISTS_CONDITION_COLUMN: return EXISTS_CONDITION_COLUMN_SUBGRP_NAME; //"EXISTS_CONDITION_COLUMN"; 
//		case IN_SELECT_COLUMN: return IN_SELECT_COLUMN_SUBGRP_NAME; //"IN_SELECT_COLUMN"; 
//		case HAVING_CLAUSE: return "HAVING_CLAUSE"; 
//		case PARAMETER_COLUMN: return "PARAMETER_COLUMN"; 
//		case PARAMETER_TABLE_REF: return "PARAMETER_TABLE_REF"; 
//		case FUNC_ANALYTIC_CLAUSE: return FUNC_ANALYTIC_CLAUSE_SUBGRP_NAME; //"FUNC_ANALYTIC_CLAUSE";
//		//case ANALYTIC_FUNC_EXPR: return "ANALYTIC_FUNC_EXPR"; //commented because abandoned as using it requires to replace existing fnction with an instance of this class upon the spotting of an anlytic clause
//		case SQL_INFA_JOINS: return "SQL_INFA_JOINS";
//		case PSEUDO_COLUMN_REF: return "PSEUDO_COLUMN_REF";
//		case SEQUENCE_VALUE_REF: return "SEQUENCE_VALUE_REF";
//		case EXPR_RIGHT_OPERAND: return "EXPR_RIGHT_OPERAND"; 
//		}
//		return String.valueOf(type);
		return SQLWidgetConstants.getTypeCode(type);
	}


}