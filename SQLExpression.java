package expr.sql;

/**
* Base class for providing support for SQL expressions.<br>
*
*
* @author Marc E. KAMGA
* @version 1.0
*/
public abstract class SQLExpression extends /*SQLExprColumn*/SQLWidget.SQLColumn implements ISQLOperandPrefixOperators, ISQLExpressionConstants {
	
//	public static final byte ARITHMETIC_EXPRESSION = 1;
//	//public static final byte ARITHMETIC_OPERAND_EXPRESSION = 2; //NOTE: commented out as an operand is not and expression
//	public static final byte ARITHMETIC_OPERATION = 2; //3;
//	public static final byte ORDERED_ARITHMETIC_EXPRESSION = 3; //4; //new
//	public static final byte BOOLEAN_EXPRESSION = 4; //5;
//	public static final byte COMPARISON_EXPRESSION = 5; //6;
//	//public static final byte BOOLEAN_OPERAND_EXPRESSION = 7; //NOTE: commented out as an operand is not and expression
//	public static final byte BOOLEAN_OPERATION = 6; //8;
//	public static final byte ORDERED_BOOLEAN_EXPRESSION = 7; //9;//
//	public static final byte STRING_OPERATION = 8; //10;
//	public static final byte STRING_EXPRESSION = 9; //11;
//	public static final byte OTHER_KIND_OF_EXPRESSION = 0;
//	
//	
//	public static final byte WITHIN_NO_BRACKETS = 0;
//	
//	public static final byte WITHIN_BRACKETS = 1;
//	/**
//	* preceded by minus sign or unary operator <code>NOT</code>.
//	*/
//	public static final byte WITHIN_NEGATED_BRACKETS = 2;
//	/**
//	* preceded by an unary operator other than minus and <code>NOT</code>.
//	*/
//	public static final byte WITHIN_OP_PREFIXED_BRACKETS = 3;
	
	
	public static final SQLExpression[] EMPTY_ARRAY = new SQLExpression[0]; 

	
	SQLExpression() {
		super();
	}
	
	SQLExpression(String aliasName) {
		super(aliasName);
	}
	
	/**
	* Returns the kind of the expression.
	*/
	public byte getKind() {return OTHER_KIND_OF_EXPRESSION/*0*/; }
	
//	/**
//	* Tells if this class implements interface {@link ISQLBooleanExpression ISQLBooleanExpression}.<br>
//	*/
//	public boolean isISQLBooleanExpression() {
//		return false;
//	}
//	/**
//	* @throws ClassCastException if this class does not implement interface {@link ISQLBooleanExpression ISQLBooleanExpression}.<br>
//	*/
//	public ISQLBooleanExpression asISQLBooleanExpression() {
//		throw new ClassCastException(
//		"SQLExpression::asISQLBooleanExpression-1: the class does not implement interface ISQLBooleanExpression"
//		);
//	}

	/**
	* Tells if the expression is within brackets.<br>
	* Please note that for instances of this class that are instances of class {@link SQLParenthExprColumn SQLParenthExprColumn}, this method always returns {@link #WITHIN_BRACKETS WITHIN_BRACKETS}.<br>
	*/
	public byte isWithinBrackets() {
		return WITHIN_NO_BRACKETS;
	}
	
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
	public final boolean isExpr() {return true; }
	/**
	* {@inheritDoc}
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
	* @return {@code 'Y'}
	*/
	public final char getLogicalColumnType() {
		return 'Y';
	}
	
	/**
	* {@inheritDoc}
	*/
	public /*final */String getDefaultTopExprSubGroup() {
		return EXPR_SUBGRP_NAME;
	}

	/**
	* {@inheritDoc}
	*/
	public final boolean isSQLExpression() {return true; }
	/**
	* {@inheritDoc}
	*/
	public final SQLExpression asSQLExpression() {return this; }
	
	
	public static String getKindCode(final byte kind) {
//		switch(kind)
//		{
//		case ARITHMETIC_EXPRESSION: return "ARITHMETIC_EXPRESSION"; 
//		//case ARITHMETIC_OPERAND_EXPRESSION: return "ARITHMETIC_OPERAND_EXPRESSION"; //NOTE: commented out as an operand is not and expression
//		case ARITHMETIC_OPERATION: return "ARITHMETIC_OPERATION"; 
//		case BOOLEAN_EXPRESSION: return "BOOLEAN_EXPRESSION"; 
//		case COMPARISON_EXPRESSION: return "COMPARISON_EXPRESSION"; 
//		//case BOOLEAN_OPERAND_EXPRESSION: return "BOOLEAN_OPERAND_EXPRESSION"; //NOTE: commented out as an operand is not and expression
//		case BOOLEAN_OPERATION: return "BOOLEAN_OPERATION"; 
//		case STRING_OPERATION: return "STRING_OPERATION"; 
//		case STRING_EXPRESSION: return "STRING_EXPRESSION"; 
//		case OTHER_KIND_OF_EXPRESSION: return "OTHER_KIND_OF_EXPRESSION"; 
//		}
//		return String.valueOf(kind);
		return SQLExpressionConstants.getKindCode(kind);
	}
	

}