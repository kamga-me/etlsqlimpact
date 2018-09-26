package expr.sql;

class SQLParseDataTypeLengthSpec extends SQLParseTempResultThing {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x020C97E221B6928AL;
	
	protected int length;
	protected String encodingTag;
	
	SQLParseDataTypeLengthSpec() {
		super();
	}
	SQLParseDataTypeLengthSpec(final int length, String encodingTag) {
		super();
		this.length = length;
		this.encodingTag = encodingTag == null ? ISQLWidgetConstants.EMPTY_STR : encodingTag;
	}
	SQLParseDataTypeLengthSpec(final int length) {
		this(length, ISQLWidgetConstants.EMPTY_STR);
	}
	
	final boolean isSQLParseDataTypeLengthSpec() {return true; }
	
	final SQLParseDataTypeLengthSpec asSQLParseDataTypeLengthSpec() {return this; }


}