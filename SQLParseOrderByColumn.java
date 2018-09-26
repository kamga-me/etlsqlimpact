package expr.sql;

/**
* Meant to serve to accumulate an order-by column spec while parsing.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*
*/
class SQLParseOrderByColumn extends SQLParseTempResultThing implements ISQLWidgetConstants, IOrderByColumnQualifiers, core.ITrinaryValues {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x020C9CDCE1000000L;
	
	protected Column column;
	protected byte sortDescending;
	protected byte nullsFirst;
	
	/**
	* Constructor.<br>
	*/
	SQLParseOrderByColumn() {
		super();
		this.sortDescending = maybe;
		this.nullsFirst = maybe;
	}
	/**
	* Constructor.<br>
	*/
	SQLParseOrderByColumn(final Column column) {
		this();
		this.column = column;
	}

	void reset() {
		this.column = null;
		this.sortDescending = maybe;
		this.nullsFirst = maybe;
	}
	
	static String getSortQualifier(final byte sortDescending, final byte nullsFirst) {
		if (sortDescending == yes) {
			if (nullsFirst == yes) {
				return DESC_NF_TAG;
			}
			else if (nullsFirst == no) {
				return DESC_NL_TAG;
			}
			return DESC_TAG;
		}
		else if (sortDescending == no) {
			if (nullsFirst == yes) {
				return ASC_NF_TAG;
			}
			else if (nullsFirst == no) {
				return ASC_NL_TAG;
			}
			return ASC_TAG;
		}
		else {
			if (nullsFirst == yes) {
				return NF_TAG;
			}
			else if (nullsFirst == no) {
				return NL_TAG;
			}
		}
		return EMPTY_STR;
	}
	
	final void __setQualifier() {
		column.aliasName = getSortQualifier(sortDescending, nullsFirst); 
	}
	
	final boolean isSQLParseOrderByColumn() {return true; }
	
	final SQLParseOrderByColumn asSQLParseOrderByColumn() {return this; }

}