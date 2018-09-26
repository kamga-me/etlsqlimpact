package expr.sql;

/**
* Base class for providing support for (from) tables of kind <code>select</code>.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public abstract class SQLSelectTbl extends SQLWidget.SQLTableRef {
	
	protected String name; //since 2017-06-20
	protected SQLSelectStmt parentSelectStmt; //since 2017-06-20

	/**
	* Constructor.<br>
	*/
	SQLSelectTbl() {
		super();
		this.name = EMPTY_STR;
	}
	
	/**
	* {@inheritDoc}
	* @return {@link #SELECT_TABLE SELECT_TABLE} or {@link #COMBINING_SELECT_TABLE COMBINING_SELECT_TABLE}
	*/
	public abstract byte getKind();
	
	/**
	* Gets the output select statement.<br>
	*/
	public abstract SQLSelectStmt getOutputSelectStmt();
	
	/**
	* {@inheritDoc}
	*/
	public final boolean isSQLSelectTbl() {
		return true; 
	}
	/**
	* {@inheritDoc}
	*/
	public final SQLSelectTbl asSQLSelectTbl() {return this; }



}