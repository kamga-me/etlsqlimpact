package expr.sql;

/**
* Class for providing support for SQL case-expression columns.<br>
* This class is more suited for just getting the involved columns rather than getting parenthesized expressions that can be run.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public class SQLCaseExprColumn extends SQLExprColumn {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204A03A31B6928AL;
	
	/**
	* Constant for <code>WHEN</code> block type.<br>
	*/
	public static final byte WHEN = 1;
	/**
	* Constant for <code>THEN</code> block type.<br>
	*/
	public static final byte THEN = 2;
	/**
	* Constant for <code>ELSE</code> block type.<br>
	*/
	public static final byte ELSE = 3;
	/**
	* Signals that the end of the case expression is reached.<br>
	*/
	public static final byte END = 4;
	
	transient byte blockType; //

	/**
	* Constructor.<br>
	*/
	SQLCaseExprColumn() {
		super();
		this.blockType = 0;
	}
	/**
	* Constructor.<br>
	*/
	SQLCaseExprColumn(String aliasName) {
		super(aliasName);
		this.blockType = 0;
	}
	
	/**
	* {@inheritDoc}
	*/
	public final byte getType() {
		return CASE_EXPR;
	}
	
	final boolean isSQLCaseExprColumn() {
		return true;
	}
	/**
	* {@inheritDoc}
	*/
	public String getDefaultTopExprSubGroup() {
		return CASE_EXPR_SUBGRP_NAME;
	}
	
	
}