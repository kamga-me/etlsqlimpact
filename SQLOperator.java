package expr.sql;

import static expr.sql.ISQLWidgetConstants.COMMA_STR;
import static expr.sql.ISQLWidgetConstants.SEMI_COLON_STR;
import static expr.sql.ISQLWidgetConstants.ASTERIX_STR;
import static expr.sql.ISQLWidgetConstants.PIPE_STR;

/**
* Class for providing support for SQL operators.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public final class SQLOperator extends SQLToken implements ISQLOperators, ISQLOperatorCategories, java.io.Serializable, Comparable<SQLOperator> {
	
	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0205442BB1B6928AL;

	public final String symbol;
	public final byte number;
	public final byte precedence;
	public final byte parseOrderNum;
	public final byte flags;
	//public final byte nAry; //now part the flags
	public final byte indexOfFirstSpaceChar;
	
	/**
	* @param nAry 0 for binary, 64 for unary and -128 for ternary
	*/
	SQLOperator(String symbol, final byte number, final byte precedence, final byte parseOrderNum, final byte nAry, final byte indexOfFirstSpaceChar, final byte flags) {
		this.symbol = symbol.equals(ASTERIX_STR) ? ASTERIX_STR : symbol.equals(PIPE_STR) ? PIPE_STR : 
							symbol.equals(COMMA_STR) ? COMMA_STR : symbol.equals(SEMI_COLON_STR) ? SEMI_COLON_STR : symbol;
		this.number = number;
		this.precedence = precedence;
		this.parseOrderNum = parseOrderNum;
		this.flags = (byte)(flags | (nAry & (UNARY | BINARY | TERNARY)));
		//this.nAry = nAry;
		this.indexOfFirstSpaceChar = indexOfFirstSpaceChar;
	}
	/**
	* {@inheritDoc}
	*/
	public final String getToken() {return symbol; }
	
	/**
	* Tells if this operator is a binary operator.
	*/
	public final boolean isBinary() {
		return (flags & UNARY_OR_TERNARY) == 0; //OLD: (flags & BINARY) != 0; 
	}
	/**
	* Tells if this operator is an unary operator.
	*/
	public final boolean isUnary() {return (flags & UNARY) != 0; }
	/**
	* Tells if this operator is a ternary operator.
	*/
	public final boolean isTernary() {return number == BETWEEN_OP || number == NOT_BETWEEN_OP; }
	
	public final boolean isABetween() {return (flags & TERNARY) != 0; }
	/**
	* Tells if this operator has two parts or more.
	*/
	public final boolean isTwoPartsSymbol() {return indexOfFirstSpaceChar != (byte)0; }
	/**
	* Tells if this operator is an arithmetic operator.
	*/
	public final boolean isArithmetic() {return (flags & ARITHMETIC) != 0; }
	/**
	* Tells if this operator is a comparison operator.
	*/
	public final boolean isComparison() {return (flags & COMPARISON) != 0; }
	/**
	* Tells if this operator is a logical operator.
	*/
	public final boolean isLogical() {return (flags & LOGICAL) != 0; }
	/**
	* Tells if this operator is a comparison operator or a logical operator.
	*/
	public final boolean isBoolean() {return (flags & BOOLEAN) != BOOLEAN; }
	/**
	*
	*/
	public final boolean isGrouping() {return (flags & GROUPING) != 0; }
	/**
	* Tells if this operator is a string-operation operator.
	*/
	public final boolean isStringOperation() {return (flags & STRING_OPERATION) != 0; }
	
	/**
	* Tells if this operator shares its symbol with a logical operator.<br>
	*/
	public boolean sameSymbolIsUsedForLogical() {
		return number == CONCAT_OP;
	}
	/**
	* Gets the logical operator which has the same symbol as this's.<br>
	* @return <code>null</code> if none of the logical operators has its symbol equal to that of this <code>SQLOperator</code>
	*/
	public SQLOperator getLogicalOpWithSameSymbol() {
		return number == CONCAT_OP ? COMMON_OPERATORS[ALT_LOGICAL_OR_OP] : null;
	}
	
	/**
	* Compares this <code>SQLOperator</code> and that supplied for order.
	*/
	public final int compareTo(SQLOperator other) {
		int cmp = symbol.compareTo(other.symbol);
		if (cmp != 0) return cmp;
		return number == other.number ? 0 : number < other.number ? -1 : 1;
	}
	/**
	* Compares this <code>SQLOperator</code> and that supplied for equality.
	*/
	public final boolean equals(SQLOperator other) {
		return number == other.number && (symbol == other.symbol || symbol.equals(other.symbol));
	}
	/**
	* Computes the hash code of the operator.<br>
	*/
	public final int hashCode() {
		return symbol.hashCode();
	}
	
	private static final SQLOperator[] COMMON_OPERATORS = new SQLOperator[]{
		null/*serves just to make the index one based*/
		, new SQLOperator("(", LEFT_PARENTHESIS_OP, (byte)30/*precedence*/, (byte)0/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, GROUPING) 
		, new SQLOperator(")", RIGHT_PARENTHESIS_OP, (byte)30/*precedence*/, (byte)0/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, GROUPING) 
		, new SQLOperator("+", PLUS_OP, (byte)10/*precedence*/, (byte)1/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("-", MINUS_OP, (byte)10/*precedence*/, (byte)2/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("*=", TSQL_OUTER_JOIN_EQUAL_OP, (byte)5/*precedence*/, (byte)3/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("*", MULTIPLY_OP, (byte)20/*precedence*/, (byte)4/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("/", DIVIDE_OP, (byte)20/*precedence*/, (byte)5/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("%", MODULUS_OP, (byte)20/*precedence*/, (byte)6/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("^", BITWISE_XOR_OP, (byte)7/*precedence*/, (byte)7/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("&", BITWISE_AND_OP, (byte)7/*precedence*/, (byte)8/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("MOD", MOD_OP, (byte)20/*precedence*/, (byte)9/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("DIV", INT_DIVIDE_OP, (byte)20/*precedence*/, (byte)10/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("POWER", POWER_OP, (byte)25/*precedence*/, (byte)10/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("||", CONCAT_OP, (byte)10/*precedence*/, (byte)11/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, (byte)(STRING_OPERATION | ARITHMETIC | LOGICAL)) 
		, new SQLOperator("|", BITWISE_OR_OP, (byte)7/*precedence*/, (byte)12/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("<>", NOT_EQUAL_OP, (byte)5/*precedence*/, (byte)13/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("!=", C_NOT_EQUAL_OP, (byte)5/*precedence*/, (byte)14/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator(":=", ASSIGNMENT_OP, (byte)0/*precedence*/, (byte)15/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator("=", EQUAL_OP, (byte)5/*precedence*/, (byte)16/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("<=>", EQUIV_OP, (byte)5/*precedence*/, (byte)17/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("<=", LESS_THAN_OR_EQUAL_OP, (byte)5/*precedence*/, (byte)17/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("<<", LEFT_SHIFT_OP, (byte)8/*precedence*/, (byte)17/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("<", LESS_THAN_OP, (byte)5/*precedence*/, (byte)18/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator(">=", GREATER_THAN_OR_EQUAL_OP, (byte)5/*precedence*/, (byte)19/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator(">>>", UNSIGNED_RSHIFT_OP, (byte)8/*precedence*/, (byte)19/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator(">>", SIGNED_RSHIFT_OP, (byte)8/*precedence*/, (byte)19/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator(">", GREATER_THAN_OP, (byte)5/*precedence*/, (byte)20/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("IN", IN_OP, (byte)5/*precedence*/, (byte)21/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("AND", AND_OP, (byte)3/*precedence*/, (byte)22/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, LOGICAL) 
		, new SQLOperator("OR", OR_OP, (byte)1/*precedence*/, (byte)23/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, LOGICAL) 
		, new SQLOperator("~", BITWISE_COMPLEMENT_OP, (byte)30/*precedence*/, (byte)24/*parse order*/, UNARY, (byte)-1/*indexOfFirstSpaceChar*/, ARITHMETIC) 
		, new SQLOperator("{", LEFT_BRACE_PARENTH_OP, (byte)30/*precedence*/, (byte)25/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, GROUPING) 
		, new SQLOperator("}", RIGHT_BRACE_PARENTH_OP, (byte)30/*precedence*/, (byte)25/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, GROUPING) 
		, new SQLOperator("[", LEFT_SQUARE_PARENTH_OP, (byte)30/*precedence*/, (byte)25/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, GROUPING) 
		, new SQLOperator("]", RIGHT_SQUARE_PARENTH_OP, (byte)30/*precedence*/, (byte)25/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, GROUPING) 
		, new SQLOperator("LIKE", LIKE_OP, (byte)5/*precedence*/, (byte)26/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("BETWEEN", BETWEEN_OP, (byte)5/*precedence*/, (byte)26/*parse order*/, TERNARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("REGEXP_LIKE", REGEXP_LIKE_OP, (byte)5/*precedence*/, (byte)26/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("EXISTS", EXISTS_OP, (byte)30/*precedence*/, (byte)26/*parse order*/, UNARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("NOT IN", NOT_IN_OP, (byte)5/*precedence*/, (byte)26/*parse order*/, BINARY, (byte)3/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("NOT LIKE", NOT_LIKE_OP, (byte)5/*precedence*/, (byte)26/*parse order*/, BINARY, (byte)3/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("NOT REGEXP_LIKE", NOT_REGEXP_LIKE_OP, (byte)5/*precedence*/, (byte)26/*parse order*/, BINARY, (byte)3/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("NOT EXISTS", NOT_EXISTS_OP, (byte)5/*precedence*/, (byte)26/*parse order*/, BINARY, (byte)3/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("NOT BETWEEN", NOT_BETWEEN_OP, (byte)5/*precedence*/, (byte)27/*parse order*/, TERNARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("NOT REGEXP", NOT_REGEXP_OP, (byte)5/*precedence*/, (byte)27/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("NOT", NOT_OP, (byte)30/*precedence*/, (byte)28/*parse order*/, UNARY, (byte)-1/*indexOfFirstSpaceChar*/, LOGICAL) 
		, new SQLOperator("!<", NOT_LESS_THAN_OP, (byte)5/*precedence*/, (byte)29/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("!>", NOT_GREATER_THAN_OP, (byte)5/*precedence*/, (byte)30/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("!", ALTERNATIVE_NOT_OP, (byte)30/*precedence*/, (byte)31/*parse order*/, UNARY, (byte)-1/*indexOfFirstSpaceChar*/, LOGICAL) 
		, new SQLOperator("XOR", XOR_OP, (byte)2/*precedence*/, (byte)31/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("REGEXP", REGEXP_OP, (byte)5/*precedence*/, (byte)31/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("RLIKE", RLIKE_OP, (byte)5/*precedence*/, (byte)31/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("&&", ALT_LOGICAL_AND_OP, (byte)3/*precedence*/, (byte)31/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("SOUNDS LIKE", SOUNDS_LIKE_OP, (byte)5/*precedence*/, (byte)31/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("ILIKE", ILIKE_OP, (byte)5/*precedence*/, (byte)32/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("IS DISTINCT FROM", IS_DISTINCT_FROM_OP, (byte)5/*precedence*/, (byte)33/*parse order*/, BINARY, (byte)2/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("IS NOT DISTINCT FROM", IS_NOT_DISTINCT_FROM_OP, (byte)5/*precedence*/, (byte)33/*parse order*/, BINARY, (byte)2/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("IS NOT", IS_NOT_OP, (byte)5/*precedence*/, (byte)33/*parse order*/, BINARY, (byte)2/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("IS", IS_OP, (byte)5/*precedence*/, (byte)33/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("IREGEXP", IREGEXP_OP, (byte)5/*precedence*/, (byte)33/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("CONTAINS", CONTAINS_OP, (byte)5/*precedence*/, (byte)34/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("CONTAINSALL", CONTAINSALL_OP, (byte)5/*precedence*/, (byte)34/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("CONTAINSANY", CONTAINSANY_OP, (byte)5/*precedence*/, (byte)34/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("||", ALT_LOGICAL_OR_OP, (byte)1/*precedence*/, (byte)35/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, LOGICAL) 
		, new SQLOperator("|=", ORACLE_JOIN_LEFT_OUTER_EQUAL_OP, (byte)5/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("=|", ORACLE_JOIN_RIGHT_OUTER_EQUAL_OP, (byte)5/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
		, new SQLOperator("|=|", ORACLE_JOIN_FULL_OUTER_EQUAL_OP, (byte)5/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, COMPARISON) 
	};
	
	private static final SQLOperator[] ASSIGNMENT_OPERATORS = new SQLOperator[]{
		COMMON_OPERATORS[ASSIGNMENT_OP] /*:=*/
		, new SQLOperator(COMMON_OPERATORS[EQUAL_OP].symbol/*"="*/, EQUAL_ASSIGNMENT_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator("+=", PLUS_EQUAL_ASSIGNMENT_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator("-=", MINUS_EQUAL_ASSIGNMENT_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator(COMMON_OPERATORS[TSQL_OUTER_JOIN_EQUAL_OP].symbol/*"*="*/, TIMES_EQUAL_ASSIGNMENT_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator("/=", DIVIDE_EQUAL_ASSIGNMENT_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator("%=", MODULUS_EQUAL_ASSIGNMENT_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator("^=", BITWISE_XOR_EQUAL_ASSIGN_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator("&=", BITWISE_AND_EQUAL_ASSIGN_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator(COMMON_OPERATORS[ORACLE_JOIN_LEFT_OUTER_EQUAL_OP].symbol/*"*="*/, BITWISE_OR_EQUAL_ASSIGN_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT)
		, new SQLOperator(">>=", BIT_RSHIFT_EQUAL_ASSIGN_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator(">>>=", BIT_URSHIFT_EQUAL_ASSIGN_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT) 
		, new SQLOperator("<<=", BIT_LSHIFT_EQUAL_ASSIGN_OP, (byte)0/*precedence*/, (byte)127/*parse order*/, BINARY, (byte)-1/*indexOfFirstSpaceChar*/, ASSIGNMENT)  
	};
	
	public final boolean isOracleJoinOuterEqualOp() {
		switch(number)
		{
		case ORACLE_JOIN_LEFT_OUTER_EQUAL_OP:
		case ORACLE_JOIN_RIGHT_OUTER_EQUAL_OP: 
		case ORACLE_JOIN_FULL_OUTER_EQUAL_OP: 
			return true;
		}
		return false;
	}
	
	/**
	* Gets the (common) operator whose symbol is equal, case insensitive wise, to the supplied string.<br>
	* <b>NOTE</b>: all the assignment operators are ignored/discarded except one ({@link #ASSIGNMENT_OP ASSIGNMENT_OP}).<br>
	*/
	public static final SQLOperator getOperator(final String op) {
		//NOTE: the length of he one-based array is 67 instead of 64 because of the dummy operators
		//      So, the extra search price is on the dummies ==> it is accepted to not go for a binary search on a sorted array as the true number of operators is 64!!!
		for (int i=1;i<COMMON_OPERATORS.length;i++)
		{
			if (COMMON_OPERATORS[i].symbol.equalsIgnoreCase(op)) return COMMON_OPERATORS[i];
		}
		return null;
	}
	
	/**
	* Gets the (common) operator whose symbol is equal, case sensitive wise, to the supplied string.<br>
	* <b>NOTE</b>: all the assignment operators are ignored/discarded except one ({@link #ASSIGNMENT_OP ASSIGNMENT_OP}).<br>
	*/
	public static final SQLOperator getOperatorCS(final String op) {
		//NOTE: the length of he one-based array is 67 instead of 64 because of the dummy operators
		//      So, the extra search price is on the dummies ==> it is accepted to not go for a binary search on a sorted array as the true number of operators is 64!!!
		for (int i=1;i<COMMON_OPERATORS.length;i++)
		{
			if (COMMON_OPERATORS[i].symbol.equals(op)) return COMMON_OPERATORS[i];
		}
		return null;
	}
	/**
	* Gets the (common) operator whose number is equal to that supplied.<br>
	* @throws IndexOutOfBoundsException if <code>operator</code> is negative or greater than {@link #NUMBER_OF_COMMON_OPERATORS NUMBER_OF_COMMON_OPERATORS}.
	*/
	public static final SQLOperator get_Operator(final int/*byte*/ number) {
		return COMMON_OPERATORS[number];
	}
	/**
	* Gets the {@link SQLOperator SQLOperator} for <code>+</code> operator.<br>
	*/
	public static final SQLOperator getPlusOperator() {return COMMON_OPERATORS[PLUS_OP]; }
	/**
	* Gets the {@link SQLOperator SQLOperator} for <code>-</code> operator.<br>
	*/
	public static final SQLOperator getMinusOperator() {return COMMON_OPERATORS[MINUS_OP]; }
	/**
	* Gets the {@link SQLOperator SQLOperator} for <code>*</code> operator.<br>
	*/
	public static final SQLOperator getMultiplyOperator() {return COMMON_OPERATORS[MULTIPLY_OP]; }
	/**
	* Gets the {@link SQLOperator SQLOperator} for <code>/</code> operator.<br>
	*/
	public static final SQLOperator getDivideOperator() {return COMMON_OPERATORS[DIVIDE_OP]; }
	/**
	* Gets the {@link SQLOperator SQLOperator} for <code>DIV</code> operator.<br>
	*/
	public static final SQLOperator getIntDivideOperator() {return COMMON_OPERATORS[INT_DIVIDE_OP]; }
	/**
	* Gets the {@link SQLOperator SQLOperator} for <code>%</code> operator.<br>
	*/
	public static final SQLOperator getModulusOperator() {return COMMON_OPERATORS[MODULUS_OP]; }
	/**
	* Gets the {@link SQLOperator SQLOperator} for <code>MOD</code> operator.<br>
	*/
	public static final SQLOperator getModuloOperator() {return COMMON_OPERATORS[MOD_OP]; }
	/**
	* Gets the {@link SQLOperator SQLOperator} for <code>POWER</code> operator.<br>
	*/
	public static final SQLOperator getPowerOperator() {return COMMON_OPERATORS[POWER_OP]; }
	
	/**
	* Gets the {@link SQLOperator SQLOperator} for <code>||</code> operator.<br>
	*/
	public static final SQLOperator getConcatOperator() {return COMMON_OPERATORS[CONCAT_OP]; }
	
	public static final SQLOperator getOracleJoinLeftOuterEqualOp() {return COMMON_OPERATORS[ORACLE_JOIN_LEFT_OUTER_EQUAL_OP]; }
	
	public static final SQLOperator getOracleJoinRightOuterEqualOp() {return COMMON_OPERATORS[ORACLE_JOIN_RIGHT_OUTER_EQUAL_OP]; }
	
	public static final SQLOperator getOracleJoinFullOuterEqualOp() {return COMMON_OPERATORS[ORACLE_JOIN_FULL_OUTER_EQUAL_OP]; }
	
	/**
	* Gets the {@code SQLOperator} for the assignment operator that shares its symbol with <code>SQLOperator</code>, if any.<br>
	*/
	public SQLOperator getAssignmentOpWithSameSymbol() {
		switch(number)
		{
		case EQUAL_OP: 
			return ASSIGNMENT_OPERATORS[EQUAL_ASSIGNMENT_OP - NUMBER_OF_COMMON_OPERATORS]; 
		case TSQL_OUTER_JOIN_EQUAL_OP: 
			return ASSIGNMENT_OPERATORS[TIMES_EQUAL_ASSIGNMENT_OP - NUMBER_OF_COMMON_OPERATORS]; 
		case ORACLE_JOIN_LEFT_OUTER_EQUAL_OP: 
			return ASSIGNMENT_OPERATORS[BITWISE_OR_EQUAL_ASSIGN_OP - NUMBER_OF_COMMON_OPERATORS]; 
		case ASSIGNMENT_OP:
			return this; 
		}
		return number >= SECOND_ASSIGNMENT_OP_NUMBER && number <= LAST_ASSIGNMENT_OP_NUMBER ? this : null;
	}
	
	/**
	* Gets the {@link SQLOperator SQLOperator} for the assignment operator whose symbol is equal to that supplied, if any.<br>
	*/
	public static SQLOperator getAssignmentOperator(final String symbol) {
		for (int i=SECOND_ASSIGNMENT_OP_NUMBER;i<=LAST_ASSIGNMENT_OP_NUMBER;i++)
		{
			if (ASSIGNMENT_OPERATORS[i].symbol.equals(symbol)) return ASSIGNMENT_OPERATORS[i];
		}
		return COMMON_OPERATORS[ASSIGNMENT_OP].equals(symbol) ? COMMON_OPERATORS[ASSIGNMENT_OP] : null;
	}
	/**
	* Gets the {@link SQLOperator SQLOperator} for the assignment operator whose number is equal to that supplied.<br>
	*/
	public static SQLOperator get_AssignmentOperator(final int assignmentOperatorNumber) {
		if (assignmentOperatorNumber < SECOND_ASSIGNMENT_OP_NUMBER) {
			return COMMON_OPERATORS[ASSIGNMENT_OP];
		}
		return ASSIGNMENT_OPERATORS[assignmentOperatorNumber - SECOND_ASSIGNMENT_OP_NUMBER_M1];
	}
	
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isSQLOperator() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLOperator asSQLOperator() {return this; }
	
	
	/**
	* Base class for providing support for sets of operators.<br>
	*
	* @author Marc E. KAMGA
	* @version 1.0
	*
	*/
	public static abstract class SQLOperatorSet implements java.io.Serializable {
		
		/**
		* Constructor.
		*/
		protected SQLOperatorSet() {
			super();
		}
		
		public abstract int size();
		
		public abstract SQLOperator getOperator(String symbol);
		
		public abstract java.util.Iterator<? extends SQLOperator> iterator();
		
	}
	

}