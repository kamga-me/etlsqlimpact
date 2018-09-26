package expr.sql;

/**
* Meant to serve in accumulating group-by columns while parsing.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*
*/
class SQLParseGroupByColumns extends SQLParseColumnList {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x020C9F25B1B6928AL;

	/**
	* Constructor.<br>
	*/
	SQLParseGroupByColumns() {
		super();
	}
	
	final boolean isSQLParseGroupByColumns() {return true; }
	
	final SQLParseGroupByColumns asSQLParseGroupByColumns() {return this; }
	
}