package expr.sql;

/**
* Class for providing support for SQL widgets of kind <code>WITH_STATEMENT</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Right to use and modify given to <code>Michelin</code> corporation
*/
public class SQLWithStmt extends SQLSelectStmt {

	/**The class's serial version UID. */
	public static final long serialVersionUID = 0x0204C7ECC1000000L;
	
	protected String name;

	/**
	* Constructor.<br>
	*/
	SQLWithStmt(String name) {
		super();
		this.name = name;
	}
	
	/**
	* {@inheritDoc}
	* @return {@link #WITH_STATEMENT WITH_STATEMENT}
	*/
	public final byte getType() {
		return WITH_STATEMENT;
	}
	/**
	* Returns the name of this <code>with-statement</code>.<br>
	*/
	public final String getName() {
		return name;
	}
	
	protected void __prependMoreInfo(String tabsIndent, StringBuilder buf) {
		buf.append(tabsIndent).append("name: ").append(name).append(LN_TERMINATOR);
	}
	
	
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final boolean isSQLWithStmt() {return true; }
	/**
	* {@inheritDoc}
	* @return <code>this</code>
	*/
	public final SQLWithStmt asSQLWithStmt() {return this; }
	

}