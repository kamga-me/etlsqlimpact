package expr.sql;

/**
* Class for providing support for SQL where clauses.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public class SQLWhereClause extends SQLWidget {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204C8C571000000L;
	
	protected SQLColumn[] involvedColumns;
	protected int involvedColumnsCount;
	
	protected SQLSelectStmt of; //added to be able to easily set ouput column numbers on columns
	
	SQLWhereClause() {
		super();
	}
	
	
	public final byte getType() {return WHERE_CLAUSE; }
	
	public final boolean isColumnRef() {return false; }
	
	public final boolean isConstant() {return false; }
	
	public final boolean isExpr() {return false; }
	
	public final boolean isSelectStmtColumn() {return false; }
	
	public final boolean isTableRef() {return false; }
	
	public final boolean isWhereClause() {return true; }
	
	public final boolean isJoin() {return false; }
	
	public final boolean isStatement() {return false; }
		
	public int getNumOfInvolvedColumns() {return involvedColumnsCount; }
	
	public SQLColumn getInvolvedColumn(int i) {
		return involvedColumns[i];
	}
	
	boolean __mustSetOutputNumber() {
		if (of == null) return false;
		SQLSelectStmt outputSelectStmt = of.getOutputSelectStmt();
		return (outputSelectStmt.fromTablesCount < 0);
	}
	
	void __addInvolvedColumn(SQLColumn col) {
		if (__mustSetOutputNumber()) {
			col.outputNumber = of.getOutputSelectStmt().outputColumnsCount;
		}
		else {
			col.outputNumber = -1;
		}
		if (involvedColumnsCount == 0) {
			if (involvedColumns == null || involvedColumns.length == 0) {
				involvedColumns = new SQLColumn[6];
			}
			involvedColumns[0] = col;
			involvedColumnsCount = 1;
			return ;
		}
		if (involvedColumnsCount >= involvedColumns.length) {
			int newLen = involvedColumns.length + (involvedColumns.length >>> 1);
			SQLColumn[] cols = new SQLColumn[newLen <= involvedColumns.length ? involvedColumns.length + 1 : newLen];
			System.arraycopy(involvedColumns, 0, cols, 0, involvedColumnsCount);
			involvedColumns = cols;
		}
		involvedColumns[involvedColumnsCount++] = col;
	}
	
	void __trim() {
		if (involvedColumns != null && involvedColumns.length > involvedColumnsCount) {
			SQLColumn[] cols = new SQLColumn[involvedColumnsCount];
			System.arraycopy(involvedColumns, 0, cols, 0, involvedColumnsCount);
			involvedColumns = cols;
		}
	}
	
	protected void __getChars(String tabsIndent, StringBuilder buf) {
		super.__getChars(tabsIndent, buf);
		String tabsIndentP1 = tabsIndent + '\t';
		String tabsIndentP2 = tabsIndentP1 + '\t';
		buf.append(tabsIndent).append("involvedColumns: ").append(LN_TERMINATOR);
		for (int i=0;i<involvedColumnsCount;i++)
		{
			buf.append(tabsIndentP1).append("- ").append(LN_TERMINATOR);
			involvedColumns[i].__getChars(tabsIndentP2, buf);
		}
	}


}