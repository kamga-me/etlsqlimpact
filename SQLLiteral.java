package expr.sql;

/**
* Class for providing support for SQL literals.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public class SQLLiteral extends SQLWidget.SQLColumn implements ISQLLiteralTypes {
	
//	public static final byte STRING_LITERAL = 1;
//	public static final byte INTEGER_LITERAL = 2;
//	public static final byte DECIMAL_LITERAL = 3;
//	public static final byte NULL_LITERAL = 4;
//	public static final byte BOOLEAN_LITERAL = 5;
//	public static final byte DATE_LITERAL = 6;
//	public static final byte TIMESTAMP_LITERAL = 7;
//	public static final byte TIME_LITERAL = 8;
//	public static final byte DURATION_LITERAL = 9;
	
	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204A12E61B6928AL;
	
	public static final String NULL_KEYWORD = "NULL";
	
	/**
	* Constant for <code>NULL</code> literal.<br>
	*/
	public static final SQLLiteral NULL = new SQLLiteral(NULL_KEYWORD, NULL_LITERAL);
	
	protected String value;
	protected byte literalType;
	protected boolean isWorkedOutFromParam; //since 2017-06-20

	/**
	* Constructor.<br>
	*/
	SQLLiteral(final String value, final byte literalType) {
		super();
		this.value = value;
		this.literalType = literalType;
		this.isWorkedOutFromParam = literalType == STRING_LITERAL && value.length() > 2 && value.charAt(0) == '$' && value.charAt(1) == '$';
	}
	/**
	* Constructor.<br>
	*/
	SQLLiteral(final String value) {
		this(value, STRING_LITERAL);
	}
	/**
	* Constructor.<br>
	*/
	SQLLiteral(final String value, final byte literalType, final String aliasName) {
		super(aliasName);
		this.value = value;
		this.literalType = literalType;
		this.isWorkedOutFromParam = literalType == STRING_LITERAL && value.length() > 2 && value.charAt(0) == '$' && value.charAt(1) == '$';
	}
	/**
	* Constructor.<br>
	*/
	SQLLiteral(final String value, final String aliasName) {
		this(value, STRING_LITERAL, aliasName);
	}
	
	/**
	*{@inheritDoc}
	* @return {@link #CONSTANT CONSTANT}
	*/
	public final byte getType() {
		return CONSTANT;
	}
	/**
	*{@inheritDoc}
	* @return {@code false}
	*/
	public final boolean isColumnRef() {return false; }
	/**
	*{@inheritDoc}
	* @return {@code true}
	*/
	public final boolean isConstant() {return true; }
	/**
	*{@inheritDoc}
	* @return {@code false}
	*/
	public final boolean isExpr() {return false; }
	/**
	*{@inheritDoc}
	* @return {@code false}
	*/
	public final boolean isSelectStmtColumn() {return false; }
	/**
	* {@inheritDoc}
	* @return {@code true}
	*/
	public final boolean isLogicalColumn() {
		return true;
	}
	/**
	* {@inheritDoc}
	* @return {@code 'N'}
	*/
	public final char getLogicalColumnType() {
		return 'Y';
	}
	/**
	* {@inheritDoc}
	* @return {@code this}
	*/
	public final SQLColumn getPhysicalColumn() {
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
	* Returns the string value of the SQL literal.<br>
	*/
	public String/*Object*/ getValue() {return value; }
	
	public static final String getLiteralTypeCode(final byte literalType) {
//		switch(literalType)
//		{
//		case STRING_LITERAL: return "STRING_LITERAL";
//		case INTEGER_LITERAL: return "INTEGER_LITERAL";
//		case DECIMAL_LITERAL: return "DECIMAL_LITERAL";
//		case BOOLEAN_LITERAL: return "BOOLEAN_LITERAL";
//		case NULL_LITERAL: return "NULL_LITERAL"; 
//		case DATE_LITERAL: return "DATE_LITERAL";  
//		case TIMESTAMP_LITERAL: return "TIMESTAMP_LITERAL"; 
//		case DURATION_LITERAL: return "DURATION_LITERAL"; 
//		}
//		return String.valueOf(literalType);
		return SQLLiteralTypes.getLiteralTypeCode(literalType);
	}
	
	protected void __getChars(String tabsIndent, StringBuilder buf) {
		buf.append(tabsIndent).append("type: ").append(getTypeCode(getType())).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("literalType: ").append(getLiteralTypeCode(literalType)).append(LN_TERMINATOR);
		if (isWorkedOutFromParam) { 
			buf.append(tabsIndent).append("isWorkedOutFromParam: true").append(LN_TERMINATOR);
			buf.append(tabsIndent).append("parameter: ").append(value).append(LN_TERMINATOR);
		}
		else {
			buf.append(tabsIndent).append("value: ").append(value).append(LN_TERMINATOR);
		}
		buf.append(tabsIndent).append("aliasName: ").append(aliasName).append(LN_TERMINATOR);
		buf.append(tabsIndent).append("outputNumber: ").append(outputNumber).append(LN_TERMINATOR);
		if (localOutputNumber > -1) {
			buf.append(tabsIndent).append("localOutputNumber: ").append(localOutputNumber).append(LN_TERMINATOR);
		}
	}
	
	static byte checkLiteralType(String value) {
//		if (value.isEmpty()) return STRING_LITERAL;
//		char ch = value.charAt(0);
//		int len = value.length();
//		if (ch < '0' || ch > '9') {
//			return ch == 'N' || ch == 'n' && len == 4 && 
//					(ch = value.charAt(1)) == 'U' || ch == 'u' && 
//					(ch = value.charAt(2)) == 'L' || ch == 'l' && 
//					(ch = value.charAt(3)) == 'L' || ch == 'l' ? NULL_LITERAL : STRING_LITERAL;
//		}
//		for (int i=1;i<len;i++)
//		{
//			ch = value.charAt(i);
//			if (ch < '0' || ch > '9') {
//				if (ch != '.') return STRING_LITERAL;
//				i++;
//				for (;i<len;i++)
//				{
//					ch = value.charAt(i);
//					if (ch < '0' || ch > '9') return STRING_LITERAL;
//				}
//				return DECIMAL_LITERAL;
//			}
//		}
//		return INTEGER_LITERAL;
		return SQLLiteralTypes.checkLiteralType(value);
	}
	
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isSQLLiteral() {
		return true; 
	}
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLLiteral asSQLLiteral() {
		return this; 
	}
	
	
}