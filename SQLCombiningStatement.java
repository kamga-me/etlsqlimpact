package expr.sql;

import expr.sql.SQLWidget.SQLStatement;

/**
* Class for providing support for combining SELECT statements.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public class SQLCombiningStatement extends SQLWidget.SQLStatement {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0205652B91B6928AL;
	
	protected SQLStatement/*OLD: SQLSelectStmt*/[] items; //NOTE: changed type on 2017-06-20
	protected int itemsCount;
	
	protected SQLCombiningSelectTable parentSelectTbl; //since 2017-06-19 - 

	/**
	* Constructor.<br>
	*/
	SQLCombiningStatement() {
		super();
		this.itemsCount = 0;
		this.parentSelectTbl = parentSelectTbl; //since 2017-06-19 - 
	}
	
	/**
	* {@inheritDoc}
	*/
	public final byte getType() {
		return SET_STATEMENT;
	}
	/**
	* {@inheritDoc}
	*/
	public final boolean isSetStatement() {return true; }
	/**
	* {@inheritDoc}
	*/
	public boolean isEmpty() {return itemsCount == 0; }
	/**
	* {@inheritDoc}
	*/
	public SQLStatement/*OLD: SQLSelectStmt*/ get(int i) {
		return items[i];
	}
	/**
	* Gets the last statement item.<br>
	*/
	public SQLStatement/*OLD: SQLSelectStmt*/ getLast() {
		return items[itemsCount - 1];
	}
	/**
	* Gets the last select statement.<br>
	*/
	public SQLSelectStmt getLastSelect() {
		SQLStatement/*OLD: SQLSelectStmt*/ last = items[itemsCount - 1];
		return last.isSQLSelectStmt() ? last.asSQLSelectStmt() : last.asSQLCombiningStatement().getLastSelect();
	}
	/**
	* Gets the driving/master select statement.<br>
	*/
	public SQLSelectStmt getDrivingStmt() {
		//return items[0]; 
		SQLStatement/*OLD: SelectStmt*/ first = items[0];
		return first.isSQLSelectStmt() ? first.asSQLSelectStmt() : first.asSQLCombiningStatement().getDrivingStmt();
	}
	
	final byte __getAssocOperator() {
		SQLStatement first = items[0];
		return first.isSQLSelectStmt() ? first.asSQLSelectStmt().getAssocOperator() : first.asSQLCombiningStatement().__getAssocOperator();
	}
	
	void __add(SQLStatement/*OLD: SQLSelectStmt*/ itm) {
		if (itemsCount == 0) {
			if (items == null || items.length == 0) {
				items = new SQLStatement/*OLD: SQLSelectStmt*/[4];
			}
			items[0] = itm;
			itemsCount = 1;
			return ;
		}
		if (itemsCount >= items.length) {
			int newLen = items.length + (items.length >>> 1);
			SQLStatement/*OLD: SQLSelectStmt*/[] stmts = new SQLStatement/*OLD: SQLSelectStmt*/[newLen <= items.length ? items.length + 1 : newLen];
			System.arraycopy(items, 0, stmts, 0, itemsCount);
			items = stmts;
		}
		items[itemsCount++] = itm;
	}
	
	void __trim() {
		if (items != null && items.length != itemsCount) {
			SQLStatement/*OLD: SQLSelectStmt*/[] stmts = new SQLStatement/*OLD: SQLSelectStmt*/[itemsCount];
			System.arraycopy(items, 0, stmts, 0, itemsCount);
			items = stmts;
		}
	}

	protected void __getChars(String tabsIndent, StringBuilder buf) {
		super.__getChars(tabsIndent, buf);
		buf.append(tabsIndent).append("selectStmtItems: ").append(LN_TERMINATOR);
		if (itemsCount != 0) {
			String tabsIndentP1 = tabsIndent + '\t';
			String tabsIndentP2 = tabsIndentP1 + '\t';
			buf.append(tabsIndentP1).append('-').append(LN_TERMINATOR);
			items[0].__getChars(tabsIndentP2, buf);
			for (int i=1;i<itemsCount;i++)
			{
				buf.append(LN_TERMINATOR).append(tabsIndentP1).append('-').append(LN_TERMINATOR);
				items[i].__getChars(tabsIndentP2, buf);
			}
		}
	}
	/**
	* Tells if this <code>SQLCombiningStatement</code> is the statement for a combining select table.<br>
	*/
	public final boolean isStatementForCombiningSelectTbl() {
		return parentSelectTbl != null;
	}
	
	public final SQLCombiningSelectTable getParentSelectTable() {
		return parentSelectTbl;
	}
		
	/**
	* {@inheritDoc}
	* @return <code>true</code>
	*/
	public final boolean isSQLCombiningStatement() {
		return true; 
	}
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLCombiningStatement asSQLCombiningStatement() {
		return this; 
	}

	
	final SQLSelectStmt __parentSelectStmt() {
		return parentSelectTbl != null ? parentSelectTbl.parentSelectStmt : null;
	}
	
	
}