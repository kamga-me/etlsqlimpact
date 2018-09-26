package expr.sql;

/**
* Meant to serve in accumulating order-by clauses while parsing.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*
*/
class SQLParseOrderByClause extends SQLParseColumnList/*SQLParseTempResultThing*/ implements IOrderByColumnQualifiers, core.ITrinaryValues {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x020C9D68D1000000L;
	
	/**
	* Transient to use and reuse as much as necessary.<br> 
	*/
	protected transient final SQLParseOrderByColumn tempOrderByColumn = new SQLParseOrderByColumn();

	/**
	* Constructor.<br>
	*/
	SQLParseOrderByClause() {
		super();
		this.columns = new Column[4];
		//this.columnsCount = 0; //DONE BY SUPER CLASS
	}
	SQLParseOrderByClause(final int anticipatedMinNumOfOrderByCols) {
		super();
		this.columns = new Column[anticipatedMinNumOfOrderByCols < 1 ? 4 : anticipatedMinNumOfOrderByCols];
		//this.columnsCount = 0; //DONE BY SUPER CLASS
	}
	
	final void onColumnFinished(final SQLParseOrderByColumn orderByColumn) {
		orderByColumn.__setQualifier();
		__add(orderByColumn.column);
	}
	
	void reset() {
		super.reset();
	}
	
	final boolean isSQLParseOrderByClause() {return true; }
	
	final SQLParseOrderByClause asSQLParseOrderByClause() {return this; }


}