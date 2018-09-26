package expr.sql;

import java.util.regex.Pattern;
import java.util.regex.Matcher;

import expr.sql.SQLWidget.SQLStatement;

import java.io.InputStreamReader;
import java.io.FileReader;
import java.io.File;
import java.io.InputStream;
import java.io.FileInputStream;


import static expr.sql.SQLWidget.EMPTY_STR;
import static expr.sql.SQLWidget.SELECT_GRP_NAME;
import static expr.sql.SQLWidget.WHERE_GRP_NAME;
import static expr.sql.SQLWidget.GROUP_BY_GRP_NAME;
import static expr.sql.SQLWidget.HAVING_GRP_NAME;
import static expr.sql.SQLWidget.ORDER_BY_GRP_NAME;
import static expr.sql.SQLWidget.FUNC_ANALYTIC_CLAUSE_SUBGRP_NAME;
import static expr.sql.SQLWidget.EXPR_SUBGRP_NAME;
import static expr.sql.SQLWidget.CASE_EXPR_SUBGRP_NAME;
import static expr.sql.SQLWidget.FUNC_EXPR_SUBGRP_NAME;
import static expr.sql.SQLWidget.PARENTH_EXPR_SUBGRP_NAME;
import static expr.sql.SQLWidget.JOIN_GRP_NAME;
import static expr.sql.SQLWidget.FROM_GRP_NAME;

import expr.sql.SQLWidget.SQLColumn;
import expr.sql.SQLWidget.SQLTableRef;
import expr.sql.SQLWidget.SQLStatement;

import static expr.sql.SQLWidget.SCHEMA_PARAM;
import static expr.sql.SQLWidget.NAME_PARAM;
import static expr.sql.SQLWidget.SCHEMA_AND_NAME_PARAMS;

/**
* Utility class for parsing SELECT sql statements.<br>
*
* <br><b>TODO - URGENT</b>: make sure SQLPseudoColumnRef is used every needed.<br>
* <b>TODO - URGENT</b>: handle the case for an unnecessary column being added to previous block when a aliased nested table is followed by WHERE, ORDER_BY, GROUP_BY or HAVING clause - see comment in the code.<br>
*
* @author Marc E. KAMGA
* @version 1.0
* @copyright Marc E. KAMGA
*/
public class SQLSelectStmtParseUtil {

	//WITH[\r\n\s]+([A-Za-z_0-9]+)[\r\n\s]+AS[\r\n\s]+|\(?[\r\n\s]*SELECT[\r\n\s]+|FROM[\r\n\s]+|CASE[\r\n\s]+|WHEN[\r\n\s]+|THEN[\r\n\s]+|ELSE[\r\n\s]+|AND[\r\n\s]+|OR[\r\n\s]+|WHERE[\r\n\s]+|(?:(?:LEFT[\r\n\s]+)?OUTER[\r\n\s]+JOIN|(?:INNER[\r\n\s]+)?JOIN)[\r\n\s]+([A-Za-z_0-9]+)(\.[A-Za-z_0-9]+)?(?:[\r\n\s]+([A-Za-z_0-9]+))?[\r\n\s]+ON(?:[\r\n\s]*\()?[\r\n\s]+|(?:(?:LEFT[\r\n\s]+)?OUTER[\r\n\s]+JOIN[\r\n\s]+\([\r\n\s]+|(?:INNER[\r\n\s]+)?JOIN[\r\n\s]+\([\r\n\s]+)SELECT[\r\n\s]+|GROUP[\r\n\s]+BY[\r\n\s]+|ORDER[\r\n\s]+BY[\r\n\s]+|UNION([\r\n\s]+ALL)?[\r\n\s]+|'[^']*'|\-\-[^\r\n]*|([A-Za-z_0-9]+)(?:\.([A-Za-z_0-9]+))?[\r\n\s]*(?:\([\r\n\s]*|(?:[\r\n\s]*AS[\r\n\s]+)?([A-Za-z_0-9]+)[\r\n\s]*)?|\)?[\r\n\s]*AS[\r\n\s]+([A-Za-z_0-9]+)[\r\n\s]*|\)[\r\n\s]*|\([\r\n\s]*|/\*|\*/

	//BUG-FIX-2017-06-19 - comparison operators now captured, the non capturing was causing the parsing of BBB_OZF_SDE_OffserDimension's SQL to fail; may need to handle the captured comparison operators as alias name in case in WHERE CLAUSE!!!
	static final String PARSE_PTRN_STR = "WITH[\\r\\n\\s]+([A-Za-z_0-9]+)[\\r\\n\\s]+AS[\\r\\n\\s]+|\\(?[\\r\\n\\s]*SELECT(?:[\\r\\n\\s]+\\$\\$Hint\\d*)?(?:[\\r\\n\\s]+DISTINCT|ALL)?[\\r\\n\\s]+|FROM[\\r\\n\\s]+|CASE[\\r\\n\\s]+|WHEN[\\r\\n\\s]+|THEN[\\r\\n\\s]+|ELSE[\\r\\n\\s]+|AND[\\r\\n\\s]+|OR[\\r\\n\\s]+|WHERE[\\r\\n\\s]+|HAVING[\\r\\n\\s]+|MOD[\\r\\n\\s]+|DIV[\\r\\n\\s]+|BY[\\r\\n\\s]+|NOT[\\r\\n\\s]+|OVER[\\r\\n\\s]+|(?:LIMIT|OFFSET)[\\r\\n\\s]+\\d+[\\r\\n\\s]*|(?:(?:(?:LEFT|RIGHT|FULL)[\\r\\n\\s]+)?(?:OUTER[\\r\\n\\s]+)?JOIN|(?:INNER[\\r\\n\\s]+)?JOIN)[\\r\\n\\s]+([A-Za-z_0-9]+)(?:\\.([A-Za-z_0-9]+))?(?:[\\r\\n\\s]+([A-Za-z_0-9]+))?[\\r\\n\\s]+ON(?:[\\r\\n\\s]*\\()?[\\r\\n\\s]+|(?:(?:(?:LEFT|RIGHT|FULL)[\\r\\n\\s]+)?(?:OUTER[\\r\\n\\s]+)?JOIN[\\r\\n\\s]*\\([\\r\\n\\s]*|(?:INNER[\\r\\n\\s]+)?JOIN[\\r\\n\\s]+\\([\\r\\n\\s]*)SELECT(?:[\\r\\n\\s]+\\$\\$Hint\\d*)?(?:[\\r\\n\\s]+DISTINCT|ALL)?[\\r\\n\\s]+|GROUP[\\r\\n\\s]+BY[\\r\\n\\s]+|ORDER[\\r\\n\\s]+BY[\\r\\n\\s]+|\\([\\r\\n\\s]*PARTITION[\\r\\n\\s]+BY[\\r\\n\\s]+|UNION(?:[\\r\\n\\s]+ALL)?[\\r\\n\\s]+|ALL[\\r\\n\\s]+|EXISTS[\\r\\n\\s]*\\([\\r\\n\\s]*SELECT(?:[\\r\\n\\s]+\\$\\$Hint\\d*)?(?:[\\r\\n\\s]+DISTINCT|ALL)?|IN[\\r\\n\\s]*\\([\\r\\n\\s]*SELECT(?:[\\r\\n\\s]+\\$\\$Hint\\d*)?(?:[\\r\\n\\s]+DISTINCT|ALL)?|IN[\\r\\n\\s]+|'[^']*'[\\r\\n\\s]*|\\=|\\<\\>|\\<\\=|\\>\\=|\\<|\\>|\\-\\-[^\\r\\n]*|(?:\\$\\$|\\-|\\+)?([A-Za-z_0-9]+)(?:\\.(?:\\$\\$)?([A-Za-z_0-9]+))?(?:\\.(?:CURRVAL|NEXTVAL)(?:@[A-Za-z0-9_]+)?)?[\\r\\n\\s]*(?:(?:\\(\\s*\\+\\s*\\)|\\()[\\r\\n\\s]*|(?:[\\r\\n\\s]*(AS)[\\r\\n\\s]+)?([A-Za-z_0-9]+|\"[^\"\\r\\n]+\")[\\r\\n\\s]*)?|\\)[\\r\\n\\s]*([A-Za-z_0-9]+)[\\r\\n\\s]+ON[\\r\\n\\s]+|\\)[\\r\\n\\s]*|\\([\\r\\n\\s]*|/\\*[\\r\\n\\s]*|\\*/[\\r\\n\\s]*|\\+[\\r\\n\\s]*|\\-[\\r\\n\\s]*|\\*[\\r\\n\\s]*|/[\\r\\n\\s]*|\\<(?:\\>|\\=)[\\r\\n\\s]*|\\=[\\r\\n\\s]*|\\!\\=[\\r\\n\\s]*|\\|\\|[\\r\\n\\s]*|\\>(?:\\=)[\\r\\n\\s]*|\\,[\\r\\n\\s]*|\\{[\\r\\n\\s]*[A-Za-z0-9_]*[\\r\\n\\s]+|\\}[\\r\\n\\s]*";
	static final Pattern PARSE_PTRN = Pattern.compile(PARSE_PTRN_STR, Pattern.CASE_INSENSITIVE);

	static final String SELECT_KWORD = SELECT_GRP_NAME; //"SELECT";
	static final String CASE_KWORD = "CASE";
	static final String WHEN_KWORD = "WHEN";
	static final String THEN_KWORD = "THEN";
	static final String ELSE_KWORD = "ELSE";
	static final String FROM_KWORD = FROM_GRP_NAME; //"FROM";
	static final String WHERE_KWORD = WHERE_GRP_NAME; //"WHERE";
	static final String GROUP_KWORD = "GROUP";
	static final String ORDER_KWORD = "ORDER";
	static final String UNION_KWORD = "UNION";
	static final String MINUS_KWORD = "MINUS";
	static final String INTERSECT_KWORD = "INTERSECT";
	static final String EXCEPT_KWORD = "EXCEPT";
	static final String ALL_KWORD = "ALL";
	static final String LPARENT_KWORD = "(";
	static final String RPARENT_KWORD = ")";
	static final String LEFT_KWORD = "LEFT";
	static final String RIGHT_KWORD = "RIGHT";
	static final String INNER_KWORD = "INNER";
	static final String FULL_KWORD = "FULL";
	static final String OUTER_KWORD = "OUTER";
	static final String JOIN_KWORD = "JOIN";
	static final String END_KWORD = "END";
	static final String AS_KWORD = "AS";
	static final String EXISTS_KWORD = "EXISTS";
	static final String IN_KWORD = "IN";
	static final String HAVING_KWORD = HAVING_GRP_NAME; //"HAVING";
	static final String LIMIT_KWORD = "LIMIT";
	static final String OFFSET_KWORD = "OFFSET";
	static final String DISTINCT_KWORD = "DISTINCT";
	static final String MOD_KWORD = "MOD";
	static final String DIV_KWORD = "DIV";
	static final String AND_KWORD = "AND";
	static final String OR_KWORD = "OR";
	static final String BETWEEN_KWORD = "BETWEEN";
	static final String NOT_KWORD = "NOT";
	static final String IS_KWORD = "IS";
	static final String OVER_KWORD = "OVER";
	static final String PARTITION_KWORD = "PARTITION";
	static final String BY_KWORD = "BY";
	static final String TOP_KWORD = "TOP";

	static final String LAST_UPDATE_DATE_COLNAME = "LAST_UPDATE_DATE";
	static final String STATUS_COLNAME = "STATUS";
	static final String ROW_WID_COLNAME = "ROW_WID";
	static final String INTEGRATION_ID_COLNAME = "INTEGRATION_ID";

//	public static final String NULL_KWORD = "NULL";

	static final String ON_KWORD = "ON";


	static final String ORACLE_OLD_STYLE_OUTER_JOIN_OP = "(+)";

	private static final int skipWs(String sqlQueryText, int from, final int sqlQueryTextEnd) {
		for (;from<sqlQueryTextEnd;from++)
		{
			char ch = sqlQueryText.charAt(from);
			if (ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n') continue;
			return from;
		}
		return from;
	}
	private static final int moveToNextWs(String sqlQueryText, int from, final int sqlQueryTextEnd) {
		char ch = sqlQueryText.charAt(from);
		while (ch != ' ' && ch != '\t' && ch != '\r' && ch != '\n')
		{
			from++;
			if (from < sqlQueryTextEnd) {
				ch = sqlQueryText.charAt(from);
				continue ;
			}
		}
		return from;
	}

	private static final boolean is_ws_char(final char ch) {
		return ch == ' ' || ch == '\t' || ch == '\r' || ch == '\n';
	}

	private static final boolean zone_equals_ci(String str, int from, int end, String substr) {
		final int substrLen = substr.length();
		if (end - from/*maxLen*/ < substrLen) return false;
		for (int i=0;i<substrLen;from++, i++)
		{
			char ch1 = str.charAt(from);
			char ch2 = substr.charAt(i);
			if (ch1 == ch2) continue;
			ch1 = Character.toLowerCase(ch1);
			ch2 = Character.toLowerCase(ch2);
			if (ch1 == ch2) continue;
			ch1 = Character.toUpperCase(ch1);
			ch2 = Character.toUpperCase(ch2);
			if (ch1 == ch2) continue;
			return false;
		}
		return true;
	}

	private static final SQLWidget[] ensureCanAddOneMoreStackElt(SQLWidget[] stack, int stackSz) {
		if (stack == null) {
			return new SQLWidget[stackSz < 4 ? 4 : stackSz];
		}
		if (stackSz >= stack.length) {
			int newLen = stack.length + (stack.length >>> 1);
			SQLWidget[] arr =new SQLWidget[newLen <= stack.length ? stack.length + 1 : newLen];
			System.arraycopy(stack, 0, arr, 0, stackSz);
			return arr;
		}
		return stack;
	}

	private static final String __getRefString(String str, java.util.TreeMap<String, String> allStrings) {
		String refStr = allStrings.get(str);
		if (refStr != null) return refStr;
		allStrings.put(str, str);
		return str;
	}

	private static final boolean ends_with_left_parenth(String str, int start, int end) {
		end--;
		for (;end>=start;end--)
		{
			char ch = str.charAt(end);
			if (!is_ws_char(ch)) {
				return (ch == '(');
			}
		}
		return false;
	}

	private static final long __parseLimit(String sqlSelectStmt, int start, int end) {
		end--;
		do
		{
			if (!is_ws_char(sqlSelectStmt.charAt(end))) break;
			end--;
			if (end <= start) return 0x8000000000000000L;
			continue;
		} while (true);
		end++;
		end -= start; //length
		if (end/*length*/ > 19) return 0x8000000000000000L;
		boolean mayOverflow = end/*length*/ == 19;
		if (mayOverflow) {
			end--;
		}
		char ch = 0;
		long limit = 0;
		for (;start<end;start++)
		{
			ch = sqlSelectStmt.charAt(start);
			limit = (limit << 3) + (limit << 1);
			limit += ch - '0';
		}
		if (mayOverflow) {
			ch = sqlSelectStmt.charAt(start);
			if (limit > 0x7FFFFFFFFFFFFFFFL / 10 || (limit == 0x7FFFFFFFFFFFFFFFL / 10 && ch > '7')) return 0x8000000000000000L;
			limit = (limit << 3) + (limit << 1);
			limit += ch - '0';
		}
		return limit;
	}

	static final boolean isDecimalDigit(final char ch) {
		return ch >= '0' && ch <= '9';
	}

	static final byte NO_WITH_STMT = 0;
	static final byte WITH_STMT_STARTED = 1;
	static final byte WITH_STMT_NEXT_STARTED = 2;
	static final byte WITH_STMT_NEXT_SELECT = 3;
	static final byte WITH_STMT_JUST_ENDED = 4;
	static final byte WITH_STMT_ENDED = 5;

	static boolean checkOracleOldStyleOuterJoinIndic(String sqlSelectStmt, int from, final int end) {
		if (from >= end) return false;
		char ch;
		boolean alreadyPlusSign = false, alreadyLeftParenth = false;
		__main_loop:
		do
		{
			ch = sqlSelectStmt.charAt(from);
			switch(ch)
			{
			case ' ':
			case '\t':
			case '\r':
			case '\n':
				from++;
				if (from == end) return false;
				continue __main_loop;
			default:
				if (alreadyPlusSign) {
					return ch == ')';
				}
				else if (ch != '(' || (alreadyLeftParenth && ch == '(')) return false;
				alreadyLeftParenth = true;
				from++;
				if (from == end) return false;
				ch = sqlSelectStmt.charAt(from);
				switch(ch)
				{
				case ' ':
				case '\t':
				case '\r':
				case '\n':
					from++;
					if (from == end) return false;
					continue __main_loop;
				case '+':
					from++;
					if (from == end) return false;
					alreadyPlusSign = true;
					ch = sqlSelectStmt.charAt(from);
					switch(ch)
					{
					case ')':
						return true;
					case ' ':
					case '\t':
					case '\r':
					case '\n':
						from++;
						if (from == end) return false;
						continue __main_loop;
					}
					return false;
				}
				return false;
			}
		} while (true); //end __main_loop
	}

	static java.lang.String getWithStmtStatusCode(final byte withStmtStatus) {
		switch(withStmtStatus)
		{
		case NO_WITH_STMT: return "NO_WITH_STMT";
		case WITH_STMT_STARTED: return "WITH_STMT_STARTED";
		case WITH_STMT_NEXT_STARTED: return "WITH_STMT_NEXT_STARTED";
		case WITH_STMT_NEXT_SELECT: return "WITH_STMT_NEXT_SELECT";
		case WITH_STMT_JUST_ENDED: return "WITH_STMT_JUST_ENDED";
		case WITH_STMT_ENDED: return "WITH_STMT_ENDED";
		}
		return java.lang.Integer.toString(withStmtStatus);
	}

	private static final boolean canFollowBlockEndInExpr(final char ch) {
		switch(ch)
		{
		case ' ':
		case '\t':
		case ')':
		case ']':
		case '}':
		case '\r':
		case '\n':
		case '+':
		case '-':
		case '*':
		case '%':
		case '/':
		case '^':
			return true;
		}
		return false;
	}

	static final SQLOperator check_if_comparison_op_symbol(final CharSequence str, final int start, final int end) {
		switch(end - start)
		{
		case 1:
			switch(str.charAt(start))
			{
			case '=': return SQLOperator.get_Operator(ISQLOperators.EQUAL_OP);
			case '<': return SQLOperator.get_Operator(ISQLOperators.LESS_THAN_OP);
			case '>': return SQLOperator.get_Operator(ISQLOperators.GREATER_THAN_OP);
			}
			return null;
		case 2:
			switch(str.charAt(start))
			{
			case '<':
				switch(str.charAt(start + 1))
				{
				case '>': return SQLOperator.get_Operator(ISQLOperators.NOT_EQUAL_OP);
				case '=': return SQLOperator.get_Operator(ISQLOperators.LESS_THAN_OR_EQUAL_OP);
				}
				return null;
			case '>':
				return str.charAt(start + 1) == '=' ? SQLOperator.get_Operator(ISQLOperators.GREATER_THAN_OR_EQUAL_OP) : null;
			case '!':
				return str.charAt(start + 1) == '=' ? SQLOperator.get_Operator(ISQLOperators.C_NOT_EQUAL_OP) : null;
			}
			return null;
		}
		return null;
	}

	//MUST RECURSIVELY SEARCH TABLE BY ALIAS FROM PARENT SELECT IN CASE NOT PRESENT IN NESTED SELECT
	/*private */static final SQLStatement parseSQLStatement(String sqlSelectStmt) {
		Matcher matcher = PARSE_PTRN.matcher(sqlSelectStmt);
		//if (!matcher.find()) throw new SQLParseException(0)/*return null*/;
		SQLSelectStmt selectStmt = null; //new SQLSelectStmt();
		//SQLStatement stmt = selectStmt;
		String schema = null, tableName = null, columnName = null, aliasName = null;
		SQLSelectStmt currentSelect = null;
		SQLExprColumn currentExprCol = null;
		SQLJoin currentJoin = null;
		SQLWidget[] stack = null;
		int stackSzM1 = -1;
		final int sqlSelectStmtLen = sqlSelectStmt.length();
		boolean asIndicatorIsPresent = false;
		byte previousEndedWith = -1;
		boolean isExistsStmt = false;
		byte operatorType = -1;
		SQLTableRef tblREf = null;
		SQLTableRef previousTblOrJoin = null;
		SQLCombiningStatement setStmt = null;
		SQLStatement returnStmt = null;
		SQLWithStmt[] withStmts = null;
		int withStmtsCount = 0;
		java.util.TreeMap<SQLStmtTable, SQLStmtTable> allTables = new java.util.TreeMap<SQLStmtTable, SQLStmtTable>();
		java.util.TreeMap<String, String> allStrings = new java.util.TreeMap<String, String>();
		int start = 0;
		int end = -1;
		SQLFuncExprColumn maybeAnalyticFuncExpr = null;
		boolean prevMatchIsClosingParenth = false;
		boolean nextMayBeAllSelect = false;
		boolean paramIndicPresent = false;
		SQLLiteral curStringLiteral = null;
		boolean nextMayBeStringLiteralAlias = false;
		byte parametarizedParts = 0;
		SQLSelectTbl/*OLD: SQLSelectTable*/ prevClosingParenthForNestedTbl = null; //changed type on 2017-06-20
		SQLColumn prevClosingParenthForCol = null; //may also be used in aliasIsActualyENDKWord is equal to true
		boolean aliasIsActualyENDKWord = false;
		boolean numberSign = false, mustBeNumberLiteral = false, decimalLiteral = false;
		String numberStr = null;
		byte withStmtStatus = NO_WITH_STMT;
		main_loop:
		while (matcher.find())
		{
//			System.out.println("matcher.group(): '" + matcher.group() + "', matcher.group(5): '" + matcher.group(5) + "', matcher.group(8): " + matcher.group(8) + ", matcher.group(2): " + matcher.group(2));
			start = matcher.start();
			end = matcher.end();

			if (withStmtStatus == WITH_STMT_JUST_ENDED) {
				if (sqlSelectStmt.charAt(matcher.start()) == ',') {
					end = matcher.end();
					if (!matcher.find()) {
						throw new SQLParseException("parse error - with-statement name followed by AS keyword expected (" + end +  ")", end)/*return null*/;
					}
					else if (matcher.start(8) >= matcher.end(8) || !zone_equals_ci(sqlSelectStmt, matcher.start(8), matcher.end(8), AS_KWORD)) {
						throw new SQLParseException("parse error - with-statement name followed by AS keyword expected (" + matcher.start() +  ")", matcher.start())/*return null*/;
					}
					else if (matcher.start(7) < matcher.end(7)) { //case there's an AS indicator
						throw new SQLParseException("parse error - with-statement name followed twice by AS keyword (" + matcher.start() +  ")", matcher.start())/*return null*/;
					}
					else if (matcher.start(6) < matcher.end(6)) { //case there's an AS indicator
						throw new SQLParseException("parse error - unexpected qualified with-statement name (" + matcher.start() +  ")", matcher.start())/*return null*/;
					}
					//zone_equals_ci(sqlSelectStmt, matcher.start(8), matcher.end(8), END_KWORD)
					aliasName = matcher.group(5); //2017-04-06 - was missing because matcher.group(5) was wrongly used below, where the select is being handled
					withStmtStatus = WITH_STMT_NEXT_STARTED;
					if (prevMatchIsClosingParenth) {
						prevMatchIsClosingParenth = false;
					}
					continue main_loop;
				}
				else {
					withStmtStatus = WITH_STMT_ENDED;
				}
			}
			if (prevMatchIsClosingParenth) {
				prevMatchIsClosingParenth = false;
				aliasName = matcher.group(5);
				if (aliasName != null) {
//					System.out.println(":::::::::::::::aliasName: " + aliasName + ", matcher.group(8):  "  + matcher.group(8) + ", matcher.group(7): " + matcher.group(7));

					if (ends_with_left_parenth(sqlSelectStmt, matcher.start(), matcher.end())) { //BUG-FIX-2017-06-20 - was not handling the combining select table item?!
						if (stackSzM1 < 0) {
							throw new SQLParseException("parse error - orphan child select statement: combining select table expected (" + matcher.start() +  ")", matcher.start())/*return null*/;
						}
						boolean isRootCombiningStmt = false;
						SQLTableRef tblRef = null;
						if (stack[stackSzM1].getType() != SQLWidget.TABLE_REF) {
							if (stackSzM1 != 0 || stack[0].getType() != SQLWidget.SET_STATEMENT) { //BUG-FIX-2017-06-20 - was not handling the case for root combining statement
								throw new SQLParseException("parse error - expected child select statement: the parent must be a combining select table (stack[stackSzM1].class=" + stack[stackSzM1].getClass().getName() + ", offset=" + matcher.start() +  ")", matcher.start())/*return null*/;
							}
							isRootCombiningStmt = true;
						}
						else {
							tblRef = stack[stackSzM1].asSQLTableRef();
							if (!tblRef.isSQLCombiningSelectTable()) {
								throw new SQLParseException("parse error - expected child select statement: the parent must be a combining select table (offset=" + matcher.start() +  ")", matcher.start())/*return null*/;
							}
						}
						operatorType = aliasName.equalsIgnoreCase(UNION_KWORD) ? SQLSelectStmt.UNION :
													aliasName.equalsIgnoreCase(MINUS_KWORD) ? SQLSelectStmt.MINUS :
													aliasName.equalsIgnoreCase(INTERSECT_KWORD) ? SQLSelectStmt.INTERSECT :
													aliasName.equalsIgnoreCase(EXCEPT_KWORD) ? SQLSelectStmt.EXCEPT : (byte)-1;
						if (operatorType > (byte)-1) { //BUG-FIX-2017-06-19 - was not handling the case for combining select table
							prevClosingParenthForNestedTbl = null;
							if (!matcher.find()) {
								throw new SQLParseException("parse error - SELECT keyword expected (" + end + ")", end)/*return null*/;
							}
							start = matcher.start();
							end = matcher.end();
							if (!zone_equals_ci(sqlSelectStmt, start, end, SELECT_KWORD)) {
								throw new SQLParseException("parse error - SELECT keyword expected (" + end + ")", end)/*return null*/;
							}
							selectStmt = new SQLCombiningSelectStmtItem(operatorType);
							start = skipWs(sqlSelectStmt, start + 6, end);
							if (start < end) {
								if (sqlSelectStmt.charAt(start) == '$') { //$$Hint
									start = moveToNextWs(sqlSelectStmt, start + 6, end);
									if (start < end) {
										start = skipWs(sqlSelectStmt, start, end);
									}
								}
							}
							if (start < end) {
								selectStmt.selectDistinct = zone_equals_ci(sqlSelectStmt, start, end, DISTINCT_KWORD);
							}
							selectStmt.fromTablesCount = -1; //indicate that FROM keyword is not yet reached
							currentSelect = selectStmt;
							SQLCombiningStatement combiningSelect = new SQLCombiningStatement();
							combiningSelect.__add(selectStmt);
							SQLCombiningSelectTable nestedCombiningSelectTbl = new SQLCombiningSelectTable(combiningSelect, true/*withinBrackets*/);
							if (isRootCombiningStmt) {
								((SQLCombiningStatement)stack[0]).__add(combiningSelect);
							}
							else {
								nestedCombiningSelectTbl.parentSelectStmt = tblRef.asSQLCombiningSelectTable().parentSelectStmt;
								tblRef.asSQLCombiningSelectTable().selectStmt.__add(combiningSelect);
							}
							stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
							stack[stackSzM1] = nestedCombiningSelectTbl;
							stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
							stack[stackSzM1] = selectStmt;
							operatorType = -1; //reset to not get error "two operator for sets in a row" next time
//							System.out.println(":::::::::::::::::::::::::::stackSzM1: " + stackSzM1);
							continue main_loop;
						}
						throw new SQLParseException("parse error - unknown/invalid combining operator: UNION, INSERSECT, MINUS or EXCEPT is expected (" + matcher.start() + ")", matcher.start())/*return null*/;
					}
//					System.out.println("????!!!!!!???? - prevClosingParenthForNestedTbl: " + (prevClosingParenthForNestedTbl != null));
					if (aliasName.equalsIgnoreCase(AS_KWORD)) {
						if (matcher.group(8) != null && matcher.group(7) == null && matcher.group(6) == null) {
							//System.out.println("????AS????");
							aliasName = matcher.group(8);
							prevClosingParenthForCol/*OLD BUG: currentExprCol*/.aliasName = aliasName;
							continue main_loop;
						}
					}
					else if (matcher.end(8) - matcher.start(8) == 4 && zone_equals_ci(sqlSelectStmt, matcher.start(8), matcher.end(8), FROM_KWORD)) { //BUG-FIX-2017-06-20 - fix bug with mplt_BC_ORA_UOMConversionGeneral_IntraClass_SQ_QUAL
//						System.out.println(":::::::::::::::******************************+++++++++++++++++++++++=YEAH!!!");
						if (prevClosingParenthForCol != null) {
							prevClosingParenthForCol.aliasName = aliasName;
							prevClosingParenthForCol = null;
						}
						else if (prevClosingParenthForNestedTbl != null) {
							throw new SQLParseException("parse error - unexpected FROM keyword (offset=" + matcher.start(8) + ")", matcher.start(8))/*return null*/;
						}
						currentSelect.fromTablesCount = 0;
						continue main_loop;
					}
					else if (prevClosingParenthForNestedTbl != null) {
//						System.out.println("::::::::::::::::::::::::prevClosingParenthForNestedTbl!!!");
						if (matcher.start(6) >= matcher.end(6) && matcher.start(7) >= matcher.end(7)) {
							if (matcher.start(8) >= matcher.end(8)) {
								prevClosingParenthForNestedTbl.name = aliasName;
								prevClosingParenthForNestedTbl = null;
								continue main_loop;
							}
							String grp8 = matcher.group(8);
							if (grp8.equalsIgnoreCase(WHERE_KWORD) || grp8.equalsIgnoreCase(GROUP_KWORD) ||
											grp8.equalsIgnoreCase(ORDER_KWORD) || grp8.equalsIgnoreCase(HAVING_KWORD)) {
								prevClosingParenthForNestedTbl.name = aliasName;
								//NOTE: don't call continue main_loop to let the clause to be handled below - with current code the will be a column added to the previous block unnecessarily, MUST BE FIXED ASAP, ESPECIALLY IF IT CAUSES SOME ISSUES LATER ON
							}
						}
						prevClosingParenthForNestedTbl = null;
					}
					else if (!aliasName.equalsIgnoreCase(END_KWORD)) { //the alis name must not be equal to ENd_KWORD
						String grp8 = matcher.group(8);
						if (matcher.group(6) == null && grp8 == null && matcher.group(7) == null) {
							prevClosingParenthForCol/*OLD BUG: currentExprCol*/.aliasName = aliasName;
							continue main_loop;
						}
						else if (grp8 != null && FROM_KWORD.equalsIgnoreCase(grp8)) {
							prevMatchIsClosingParenth = true; //leave prevMatchIsClosingParenth to true for the alias to be set below when handling the case for FROM keyword equal to group(8), which gives the alias
						}
					}
					else if (currentExprCol == null || currentExprCol.getType() != SQLWidget.CASE_EXPR) {
						throw new SQLParseException(matcher.start())/*return null*/;
					}
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, IN_KWORD)) { //BUG-FIX-2017-06-19 was not handling the case when closing parenthesis was followed by "IN" keyword, with "IN" keyword followed by blankspace and no parenthesis
					int j = skipWs(sqlSelectStmt, start + 2, end);
					if (j >= matcher.end()) {
//						System.out.println("CAUGHT UP!!!!");
						prevClosingParenthForCol.aliasName = IN_KWORD;
						continue main_loop;
					}
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, BETWEEN_KWORD)) { //BUG-FIX-2017-06-19 was not handling the case when closing parenthesis was followed by "IN" keyword, with "IN" keyword followed by blankspace and no parenthesis
					int j = skipWs(sqlSelectStmt, start + 2, end);
					if (j >= matcher.end()) {
//						System.out.println("CAUGHT UP!!!!");
						prevClosingParenthForCol.aliasName = BETWEEN_KWORD;
						continue main_loop;
					}
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, IS_KWORD)) { //BUG-FIX-2017-06-19 was not handling the case when closing parenthesis was followed by "IN" keyword, with "IN" keyword followed by blankspace and no parenthesis
					int j = skipWs(sqlSelectStmt, start + 2, end);
					if (j >= matcher.end()) {
//						System.out.println("CAUGHT UP!!!!");
						prevClosingParenthForCol.aliasName = IS_KWORD;
						continue main_loop;
					}
				}
				else {
					SQLOperator comparisonOp = check_if_comparison_op_symbol(sqlSelectStmt, start, end);
					if (comparisonOp != null) {
						prevClosingParenthForCol.aliasName = comparisonOp.symbol;
						continue main_loop;
					}
				}
			}
			else if (nextMayBeAllSelect) {
				nextMayBeAllSelect = false;
				if (matcher.start(1) >= matcher.end(1) && matcher.start(5) >= matcher.end(5) && matcher.start(9) >= matcher.end(9) && matcher.start(4) >= matcher.end(4) && matcher.start(3) >= matcher.end(3) && matcher.start(2) >= matcher.end(2)) {
					if (zone_equals_ci(sqlSelectStmt, start, end, ALL_KWORD)) {
						start = skipWs(sqlSelectStmt, start + 3, end);
						if (start >= end) {
							operatorType = SQLSelectStmt.UNION_ALL;
							withStmtStatus = NO_WITH_STMT; //reset
							//System.out.println("????UNION ALL????");
							continue main_loop;
						}
					}
				}
			}
			else if (nextMayBeStringLiteralAlias) {
//				System.out.println("matcher.group(): " + matcher.group() + " - " + nextMayBeStringLiteralAlias);
				nextMayBeStringLiteralAlias = false;
				if (matcher.start(5) < matcher.end(5) && matcher.start(7) >= matcher.end(7) && matcher.start(6) >= matcher.end(6)) { //BUG-FIX-2017-06-19 - was testing "matcher.start(5) <= matcher.end(5)" in lieu of "matcher.start(5) < matcher.end(5)"
					if (matcher.start(8) >= matcher.end(8)) { //case group 8 is missing
						curStringLiteral.aliasName = matcher.group(5);
						continue main_loop;
					}
					else if (matcher.group(5).equalsIgnoreCase(AS_KWORD)) {
						curStringLiteral.aliasName = matcher.group(8);
						continue main_loop;
					}
				}
			}
			else if (aliasIsActualyENDKWord) { //case previously captured in group 8 was keyword END which marked the end of a CASE-WHEN
				if (!zone_equals_ci(sqlSelectStmt, matcher.start(), sqlSelectStmtLen, "/*")) {
					//System.out.println("aliasName for previous CASE-WHEN ??? - matcher.group(): " + matcher.group());
					aliasIsActualyENDKWord = false;
					if (matcher.start(5) < matcher.end(5) && matcher.start(7) >= matcher.end(7) && matcher.start(6) >= matcher.end(6)) {
						if (matcher.start(8) < matcher.end(8)) {
							if (matcher.group(5).equalsIgnoreCase(AS_KWORD)) {
								//System.out.println("aliasName of previous CASE-WHEN captured!!!!");
								prevClosingParenthForCol.aliasName = matcher.group(8);
								prevClosingParenthForCol = null;
								continue main_loop;
							}
							String grp8 = matcher.group(8);
							if (grp8.equalsIgnoreCase(FROM_KWORD) || grp8.equalsIgnoreCase(WHERE_KWORD) || grp8.equalsIgnoreCase(GROUP_KWORD) || grp8.equalsIgnoreCase(ORDER_KWORD) || grp8.equalsIgnoreCase(HAVING_KWORD) || grp8.equalsIgnoreCase(UNION_KWORD) || grp8.equalsIgnoreCase(MINUS_KWORD) || grp8.equalsIgnoreCase(INTERSECT_KWORD) || grp8.equalsIgnoreCase(EXCEPT_KWORD)) {
								//System.out.println("aliasName of previous CASE-WHEN captured even without AS keyword and followed by " + grp8 + "!!!!");
								prevClosingParenthForCol.aliasName = matcher.group(5);
								prevClosingParenthForCol = null;
								aliasIsActualyENDKWord = true; //restore it for use below for WHERE, ORDER BY, GROUP BY, or HAVING CLAUSE
								//continue main_loop; //BUG!! MUST LEt THE ITERATION CONTINUE!!!
							}
						}
						else { //case: if (matcher.start(8) >= matcher.end(8)) {
							//System.out.println("aliasName of previous CASE-WHEN captured even without AS keyword!!!!");
							prevClosingParenthForCol.aliasName = matcher.group(5);
							prevClosingParenthForCol = null;
							continue main_loop;
						}
					}
					prevClosingParenthForCol = null;
				}
			}
			//System.out.println("SQLSelectStmtParseUtil - withStmtStatus: " + getWithStmtStatusCode(withStmtStatus));
			mustBeNumberLiteral = false;
			String grp = withStmtStatus == WITH_STMT_NEXT_STARTED ? aliasName/*OLD KO: matcher.group(5)*/ : matcher.group(1);
			if (grp != null) { //case with statement
				if (withStmtStatus != WITH_STMT_NEXT_STARTED && withStmtStatus != NO_WITH_STMT) {
					throw new SQLParseException("second occurrence of WITH keyword found", start)/*return null*/;
				}
				if (withStmtStatus != WITH_STMT_NEXT_STARTED) {
					if (!matcher.find()) throw new SQLParseException(end)/*return null*/;
					start = matcher.start();
					end = matcher.end();
					//System.out.println("matcher.group(): '" + matcher.group() + "'");
					if (sqlSelectStmt.charAt(start) != '(') throw new SQLParseException(start)/*return null*/; //'(SELECT' is expected
				}
				start = skipWs(sqlSelectStmt, start + 1, sqlSelectStmtLen);
				//System.out.println("sqlSelectStmt.substring(start, end): " + sqlSelectStmt.substring(start, end));
				if (!zone_equals_ci(sqlSelectStmt, start, sqlSelectStmtLen, SELECT_KWORD)) throw new SQLParseException(matcher.start())/*return null*/;
				SQLWithStmt withStmt = new SQLWithStmt(grp);
				withStmt.fromTablesCount = -1; //indicates that keyword 'FROM' is not yet reached
				start = skipWs(sqlSelectStmt, start + 6, end);
				if (start < end) {
					if (sqlSelectStmt.charAt(start) == '$') { //$$Hint
						start = moveToNextWs(sqlSelectStmt, start + 6, end);
						if (start < end) {
							start = skipWs(sqlSelectStmt, start, end);
						}
					}
				}
				if (start < end) {
					withStmt.selectDistinct = zone_equals_ci(sqlSelectStmt, start, end, DISTINCT_KWORD);
				}
				stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
				stack[stackSzM1] = withStmt;
				if (withStmtsCount == 0) {
					withStmts = new SQLWithStmt[4];
					withStmts[0] = withStmt;
					withStmtsCount = 1;
				}
				else {
					if (withStmtsCount >= withStmts.length) {
						int newLen = withStmts.length + (withStmts.length >>> 1);
						SQLWithStmt[] arr = new SQLWithStmt[newLen <= withStmtsCount ? withStmtsCount + 1 : newLen];
						System.arraycopy(withStmts, 0, arr, 0, withStmtsCount);
						withStmts = arr;
					}
					withStmts[withStmtsCount++] = withStmt;
				}
				currentSelect = withStmt;
				previousTblOrJoin = null;
				previousEndedWith = -1;
				withStmtStatus = withStmtStatus == WITH_STMT_NEXT_STARTED ? WITH_STMT_NEXT_SELECT : WITH_STMT_STARTED;
				continue main_loop;
			} //end with statement
			else if ((grp = matcher.group(2)) != null) { //case join
				if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
				__switch_block:
				switch(stack[stackSzM1].getType())
				{
				case SQLWidget.SELECT_STATEMENT:
				case SQLWidget.WITH_STATEMENT:
				case SQLWidget.SELECT_STMT_COLUMN:
				case SQLWidget.EXISTS_CONDITION_COLUMN:
				case SQLWidget.IN_SELECT_COLUMN:
				case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
					break;
				case SQLWidget.JOIN: //join must be ended by new join
					if (currentJoin.joinColumnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
					stack[stackSzM1--] = null;
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
					case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
						break __switch_block;
					case SQLWidget.SQL_INFA_JOINS:
						if (currentSelect.withInfaJoins) {
							throw new SQLParseException("parse error - cannot have more than one INFA join (" + matcher.start() + ")", matcher.start())/*return null*/;
						}
						break __switch_block;
					default:
						throw new SQLParseException(matcher.start())/*return null*/;
					}
				case SQLWidget.SQL_INFA_JOINS:
					if (currentSelect.withInfaJoins) {
						throw new SQLParseException("parse error - cannot have more than one INFA join (" + matcher.start() + ")", matcher.start())/*return null*/;
					}
					break __switch_block;
				default: //unexpected parent's type
					throw new SQLParseException("parse error - " + stack[stackSzM1].getClass().getName() + "-" + matcher.start(), matcher.start())/*return null*/;
				}
				if (currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/; //a join cannot occur before keyword 'FROM' or before the first table
				SQLTableRef joinedTbl = null;
				if (stack[stackSzM1].getType() == SQLWidget.SQL_INFA_JOINS) {
					if (matcher.group(3) != null || matcher.group(4) != null) {
						throw new SQLParseException(matcher.start())/*return null*/; //tableAlias only and not not qualified table name is expected as all the tables are declared before infa joins block
					}
					joinedTbl = currentSelect.getTableByAliasExt(grp);
					if (joinedTbl == null) {
						throw new SQLParseException("unknown table alias - forward declaration of table expected with INFA joins (" + matcher.start() + ")", matcher.start())/*return null*/;
					}
					currentSelect.withInfaJoins = true;
				}
				else {
					tableName = matcher.group(3);
					if (tableName == null) {
						tableName = grp;
						schema = EMPTY_STR; //null
					}
					else {
						schema = __getRefString(grp, allStrings);
					}
					tableName = __getRefString(tableName, allStrings);
					SQLStmtTable tbl = new SQLStmtTable(schema, tableName);
					SQLStmtTable existingTbl = allTables.get(tbl);
					if (existingTbl != null) {
						tbl = existingTbl;
					}
					else {
						allTables.put(tbl, tbl);
					}
					aliasName = matcher.group(4);

					if (aliasName != null) {
						if (aliasName.equals(tableName)) {
							aliasName = tableName;
						}
						else {
							aliasName = __getRefString(aliasName, allStrings);
						}
						joinedTbl = new SQLStmtTableAlias(aliasName, tbl);
					}
					else {
						joinedTbl = tbl;
					}
				}
				start = skipWs(sqlSelectStmt, start, end);
				if (zone_equals_ci(sqlSelectStmt, start, end, INNER_KWORD)) {
					currentJoin = new SQLJoin(SQLJoin.INNER_JOIN, joinedTbl);
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, LEFT_KWORD)) {
					currentJoin = new SQLJoin(SQLJoin.LEFT_OUTER_JOIN, joinedTbl);
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, RIGHT_KWORD)) {
					currentJoin = new SQLJoin(SQLJoin.RIGHT_OUTER_JOIN, joinedTbl);
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, FULL_KWORD)) {
					currentJoin = new SQLJoin(SQLJoin.FULL_OUTER_JOIN, joinedTbl);
				}
				else {
					switch(previousEndedWith)
					{
					case SQLJoin.LEFT_OUTER_JOIN:
						currentJoin = new SQLJoin(SQLJoin.LEFT_OUTER_JOIN, joinedTbl);
						break ;
					case SQLJoin.RIGHT_OUTER_JOIN:
						currentJoin = new SQLJoin(SQLJoin.RIGHT_OUTER_JOIN, joinedTbl);
						break ;
					case SQLJoin.FULL_OUTER_JOIN:
						currentJoin = new SQLJoin(SQLJoin.RIGHT_OUTER_JOIN, joinedTbl);
						break ;
					default:
						currentJoin = new SQLJoin(SQLJoin.INNER_JOIN, joinedTbl);
						break ;
					}
					previousEndedWith = -1;
				}
				if (stack[stackSzM1].getType() == SQLWidget.SQL_INFA_JOINS) {
					((SQLInfaJoins)stack[stackSzM1]).__addJoin(currentJoin);
				}
				else {
					currentSelect.__addFromTable(joinedTbl);
					currentSelect.__addJoin(currentJoin);
				}
				stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
				stack[stackSzM1] = currentJoin;
				currentJoin.tbl = previousTblOrJoin;
				previousTblOrJoin = currentJoin;
				continue main_loop;
			} //end case join
			else if ((grp = matcher.group(5)) != null) { //case column or from-table
				//System.out.println("matcher.group(): '" + matcher.group() + "', matcher.group(8): " + matcher.group(8) + "', matcher.group(5): " + matcher.group(5));
				int grpStart = matcher.start(5);
				if (grpStart > 0) {
					char ch = sqlSelectStmt.charAt(grpStart - 1);
					parametarizedParts = ch == '$' ? SCHEMA_PARAM : (byte)0;
					numberSign = ch == '-' || ch == '+';
				}
				else {
					parametarizedParts = 0;
					numberSign = false;
				}
				mustBeNumberLiteral = isDecimalDigit(sqlSelectStmt.charAt(grpStart));
				decimalLiteral = false;
				int grpLen = grp.length();
				if (mustBeNumberLiteral) {
					for (int i=0;i<grpLen;i++)
					{
						if (!(isDecimalDigit(grp.charAt(i)))) {
							throw new SQLParseException(matcher.start())/*return null*/;
						}
					}
				}
				else if (numberSign) {
					throw new SQLParseException(matcher.start())/*return null*/;
				}
				grpLen = matcher.end(6); //grpEnd
				grpStart = matcher.start(6);
				if (grpStart/*matcher.start(6)*/ < grpLen/*matcher.end(6)*/) { //case suffix part is present
					if (sqlSelectStmt.charAt(grpStart - 1) == '$') {
						if (numberSign || mustBeNumberLiteral) {
							throw new SQLParseException(matcher.start())/*return null*/; //cannot combine number sign and "$$" sign
						}
						parametarizedParts = SCHEMA_AND_NAME_PARAMS;
					}
					else if (mustBeNumberLiteral) {
						for (;grpStart<grpLen/*grpEnd*/;grpStart++)
						{
							if (!(isDecimalDigit(sqlSelectStmt.charAt(grpStart)))) {
								throw new SQLParseException(matcher.start())/*return null*/;
							}
						}
						numberStr = numberSign ? sqlSelectStmt.substring(matcher.start(5) - 1, matcher.end(6)) :
													sqlSelectStmt.substring(matcher.start(5), matcher.end(6));
						decimalLiteral = true;
					}
				} //end case suffix part is present
				else if (mustBeNumberLiteral) {
					numberStr = numberSign ? sqlSelectStmt.substring(matcher.start(5) - 1, matcher.end(5)) : grp;
					//System.out.println("matcher.group(): " + matcher.group() + ", numberSign: " + numberSign + ", numberStr: " + numberStr);
				}
				else if (parametarizedParts == SCHEMA_PARAM) {
					parametarizedParts = NAME_PARAM;
				}
				else if (END_KWORD.equalsIgnoreCase(grp) && matcher.start(8) >= matcher.end(8)) { //BUG-FIX-2017-06-28 - was not handling the of 'END' being a next match
//					System.out.println("ZZZZZZZZZZZZZZZZZZZZZZZZZUUUUUUUUUUUUUUUUTTTTTTTTTT");
					if (matcher.start(7) < matcher.end(7)) { //case AS keyword is present ==> fire exception
						throw new SQLParseException("parse error - unexpected AS keyword after END keyword (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
					}
					else if (stackSzM1 < 0 || stack[stackSzM1].getType() != SQLWidget.CASE_EXPR) {
						throw new SQLParseException("parse error - orphan END keyword (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
					}
					SQLCaseExprColumn caseCol = (SQLCaseExprColumn)stack[stackSzM1];
					switch(caseCol.blockType)
					{
					case SQLCaseExprColumn.THEN:
					case SQLCaseExprColumn.ELSE:
						break ;
					default:
						throw new SQLParseException("parse error - unexpected END keyword (caseExprStage=" + caseCol.blockType + ", offset=" + matcher.start() + ")", matcher.start())/*return null*/;
					}
					stack[stackSzM1--] = null; //remove case expression from stack
					if (stackSzM1 > -1) { //BUG-FIX-2017-06-28 - was not setting currentSelect
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
							currentSelect = (SQLSelectStmt)stack[stackSzM1];
						}
					}
					continue main_loop;
				}
				else if (matcher.end(8) - matcher.start(8) == 4 && zone_equals_ci(sqlSelectStmt, matcher.start(8), matcher.end(8), THEN_KWORD)) { //BUG-FIX-2017-06-28 - was not handling this case
					System.out.println("THEN, THEN, THEN, THEN");
					if (stackSzM1 < 0 || stack[stackSzM1].getType() != SQLWidget.CASE_EXPR) {
						throw new SQLParseException("parse error - orphan THEN keyword (offset=" + matcher.start(8) + ")", matcher.start())/*return null*/;
					}
					else if (matcher.start(7) < matcher.end(7)) { //case AS keyword is present ==> fire exception
						throw new SQLParseException("parse error - unexpected AS keyword after THEN keyword (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
					}
					SQLCaseExprColumn caseCol = (SQLCaseExprColumn)stack[stackSzM1];
					switch(caseCol.blockType)
					{
					case SQLCaseExprColumn.WHEN:
						break ;
					default:
						throw new SQLParseException("parse error - unexpected THEN keyword (caseExprStage=" + caseCol.blockType + ", offset=" + matcher.start() + ")", matcher.start())/*return null*/;
					}
					SQLColumn col;
					if (SQLLiteral.NULL_KEYWORD/*NULL_KWORD*/.equalsIgnoreCase(grp)) {
						col = SQLLiteral.NULL;
					}
					else {
						String pseudoColName = SQLPseudoColumnNames.checkPseudoColName(grp);
						if (pseudoColName != null) {
							col = new SQLPseudoColumnRef(pseudoColName, EMPTY_STR);
						}
						else {
							col = new SQLRawColumnRef(EMPTY_STR/*tableName*/, grp/*columnName*/, EMPTY_STR);
						}
					}
					caseCol.__addInvolvedColumn(col);
					caseCol.blockType = SQLCaseExprColumn.THEN;
//					System.out.println("BYE BYE THEN - caseCol.blockType: " + caseCol.blockType);
					continue main_loop;
				}
				if (!aliasIsActualyENDKWord) { //case we get here not just after the handling of the alias name that follows previous occurrence of ENd keyword
					 //BUG-FIX - 2017-05-17 - made the check of matcher.group(8) more robust to avoid that an alias name starting with keyword end gets handled as the end of a case statement
					 int _start = matcher.start(8);
					 int _end = matcher.end(8);
					 if (zone_equals_ci(sqlSelectStmt, _start, _end, END_KWORD)) {
						 //System.out.println("sqlSelectStmt.charAt(_end - 1): '" + sqlSelectStmt.charAt(_end - 1) + "', sqlSelectStmt.charAt(_start + 3): '" + sqlSelectStmt.charAt(_start + 3) + "', canFollowBlockEndInExpr(sqlSelectStmt.charAt(_start + 3)): " + canFollowBlockEndInExpr(sqlSelectStmt.charAt(_start + 3)));
						/*boolean */aliasIsActualyENDKWord = ((_end < sqlSelectStmtLen &&
																	canFollowBlockEndInExpr(sqlSelectStmt.charAt(_start + 3))) ||
																		(_end >= sqlSelectStmtLen));
					}
					_start = matcher.start(5);
					_end = matcher.end(5);
					paramIndicPresent =  sqlSelectStmt.charAt(matcher.start()) == '$';
					if ((zone_equals_ci(sqlSelectStmt, _start, _end, END_KWORD) && ((_end < sqlSelectStmtLen && canFollowBlockEndInExpr(sqlSelectStmt.charAt(_start + 3))) || (_end >= sqlSelectStmtLen))) || aliasIsActualyENDKWord) { //BUG-FIX - 2017-05-17 - made the check more robust to avoid that a column name starting with keyword end gets handled as the end of a case statement
						//System.out.println("END keyword trying to fake as column alias:" + aliasIsActualyENDKWord + ", matcher.group(): '" + matcher.group() + "'");
						if (!aliasIsActualyENDKWord && matcher.group(6) != null) throw new SQLParseException("parse error " + matcher.start() + ", group: " + matcher.group(), matcher.start())/*return null*/;
						if (stackSzM1 < 0 || stack[stackSzM1].getType() != SQLWidget.CASE_EXPR) throw new SQLParseException("parse error: END keyword without counterpart CASE keyword - offset: " + matcher.start() + ", group: " + matcher.group() + ", group(5): " + matcher.group(5) + ", aliasIsActualyENDKWord: " + aliasIsActualyENDKWord + ", stack.top.class: " + stack[stackSzM1].getClass().getName(), matcher.start())/*return null*/;
						aliasName = matcher.group(8);
						if (aliasName != null) {
							if (!aliasIsActualyENDKWord) {
								currentExprCol.aliasName = aliasName;
							}
							asIndicatorIsPresent = matcher.group(7) != null;
							if (aliasIsActualyENDKWord && asIndicatorIsPresent) {
								throw new SQLParseException(matcher.start())/*return null*/;
							}
						}
						else {
							asIndicatorIsPresent = false;
						}
						if (aliasIsActualyENDKWord) {
							columnName = matcher.group(6);
							SQLColumn col;
							if (mustBeNumberLiteral) {
								col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
							}
							else if (columnName == null) {
								columnName = matcher.group(5);
								byte literalType = SQLLiteral.checkLiteralType(columnName);
								if (literalType != SQLLiteral.STRING_LITERAL) {
									col = new SQLLiteral(columnName/*value*/, literalType, EMPTY_STR);
								}
								else if (parametarizedParts != (byte)0) {
									col = new SQLParameterColumn(EMPTY_STR/*ownerSchema*/, columnName, parametarizedParts, EMPTY_STR);
								}
								else {
									String pseudoColName = SQLPseudoColumnNames.checkPseudoColName(columnName);
									col = pseudoColName != null ? new SQLPseudoColumnRef(pseudoColName, EMPTY_STR) : new SQLColumnRef(SQLStmtTable.NO_TABLE, columnName, EMPTY_STR);
								}
							}
							else if (mustBeNumberLiteral) {
								col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
							}
							else {
								String tableAlias = matcher.group(5);
								if (parametarizedParts != (byte)0) {
									col = new SQLParameterColumn(tableAlias/*ownerSchema*/, columnName, parametarizedParts, EMPTY_STR);
								}
								else if (currentSelect.fromTablesCount > 0) {
									SQLTableRef tblRef = currentSelect.getTableByAliasExt(tableAlias);
									if (tblRef != null) {
										col = new SQLColumnRef(tblRef, columnName, EMPTY_STR);
									}
									else {
										col = new SQLRawColumnRef(tableAlias, columnName, EMPTY_STR);
									}
								}
								else {
									col = new SQLRawColumnRef(tableAlias, columnName, EMPTY_STR);
								}
							}
							currentExprCol/*stack[stackSzM1]*/.__addInvolvedColumn(col);
							prevClosingParenthForCol = currentExprCol; //
						}
						SQLExprColumn exprCol = currentExprCol;
						currentExprCol = null;
						stack[stackSzM1--] = null; //remove case col from stack
						SQLWidget topWidget = stack[stackSzM1];
						switch (topWidget.getType())
						{
						case SQLWidget.EXPR:
						case SQLWidget.CASE_EXPR:
						case SQLWidget.FUNC_EXPR:
						case SQLWidget.PARENTH_EXPR:
						case SQLWidget.FUNC_ANALYTIC_CLAUSE:
							currentExprCol = (SQLExprColumn)topWidget;
							//currentExprCol.__addInvolvedColumn(exprCol); //commented out because added when CASE keyword is handled
							break ;
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
							currentSelect = (SQLSelectStmt)topWidget;
							//currentSelect.__addColumn(exprCol); //commented out because added when CASE keyword is handled
							break ;
						case SQLWidget.SET_STATEMENT:
							SQLCombiningStatement setStmt_ = (SQLCombiningStatement)topWidget;
							if (setStmt_.isEmpty()) throw new SQLParseException(matcher.start())/*return null*/;
							currentSelect = setStmt_.getLast().asSQLSelectStmt();
							//currentSelect.__addColumn(exprCol); //commented out because added when CASE keyword is handled
							break;
						case SQLWidget.JOIN:
							currentJoin = (SQLJoin)topWidget;
							//currentJoin.__addJoinColumn(exprCol); //commented out because added when CASE keyword is handled
							previousTblOrJoin = currentJoin;
							break ;
						case SQLWidget.SELECT_STMT_COLUMN:
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
							currentSelect = ((SQLSelectStmtColumn)topWidget).selectStmt;
							//currentSelect.__addColumn(exprCol); //commented out because added when CASE keyword is handled
							break ;
						case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
							currentSelect = ((SQLSelectTable)topWidget).selectStmt;
							//currentSelect.__addColumn(exprCol); //commented out because added when CASE keyword is handled
							break ;
						}
						continue main_loop;
					}
				}
				if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
				asIndicatorIsPresent = matcher.start(7) < matcher.end(7); //OLD STYLE BUT COSTLY!!!?: matcher.group(7) != null;
				boolean oracleOldStyleOuterJoinOp = false;
				//the Oracle old style outer join indicator is not an alternative to outer join, it is intead an alternative to function argument list start indicator ==> COMMENT OUT THE BELOW LINES
				//if (asIndicatorIsPresent) { //case AS keyword or ORacle old style outer join operator
				//	if (sqlSelectStmt.charAt(matcher.start(7)) == '(') {
				//		if (matcher.start(8) < matcher.end(8)) { //case "(+)" is followed by an alias name
				//			throw new SQLParseException("parse error - Oracle old style outer join operator not followed by a comparison operator", matcher.start(7))/*return null*/;
				//		}
				//		asIndicatorIsPresent = false;
				//		oracleOldStyleOuterJoinOp = true;
				//	}
				//}
				columnName = matcher.group(6);
				if (columnName == null) {
					columnName = grp;
					tableName = EMPTY_STR; //null;
					//if (!asIndicatorIsPresent) {
					//	start = skipWs(sqlSelectStmt, matcher.end(5) + 1, matcher.end());
					//}
					oracleOldStyleOuterJoinOp = checkOracleOldStyleOuterJoinIndic(sqlSelectStmt, matcher.end(5), matcher.end());
				}
				else {
					tableName = grp;
					//if (!asIndicatorIsPresent) {
					//	start = skipWs(sqlSelectStmt, matcher.end(5) + 1, matcher.end());
					//}
					oracleOldStyleOuterJoinOp = checkOracleOldStyleOuterJoinIndic(sqlSelectStmt, matcher.end(6), matcher.end());
				}
				if (oracleOldStyleOuterJoinOp) {
					if (asIndicatorIsPresent || matcher.start(8) < matcher.end(8)) {
						throw new SQLParseException("parse error - unexpected AS keyword or alias name after Oracle old style outer join indicator ("  + matcher.start() + ")", matcher.start())/*return null*/;
					}
				}
				if (LAST_UPDATE_DATE_COLNAME.equals(columnName)) {
					columnName = LAST_UPDATE_DATE_COLNAME;
				}
				else if (STATUS_COLNAME.equals(columnName)) {
					columnName = STATUS_COLNAME;
				}
				else if (INTEGRATION_ID_COLNAME.equals(columnName)) {
					columnName = INTEGRATION_ID_COLNAME;
				}
				else if (ROW_WID_COLNAME.equals(columnName)) {
					columnName = ROW_WID_COLNAME;
				}
				tableName = __getRefString(tableName, allStrings);
				boolean aliasNameIsWhereOrFromKeyword = false;
				aliasName = oracleOldStyleOuterJoinOp ? ORACLE_OLD_STYLE_OUTER_JOIN_OP : matcher.group(8);
				//System.out.println("matcher.group(): '" + matcher.group() + "', aliasName: '" + aliasName + "', asIndicatorIsPresent: " + asIndicatorIsPresent);
				if (aliasName != null) {
					if (aliasName.equals(columnName)) {
						aliasName = columnName;
					}
					if (!asIndicatorIsPresent) {
						byte operatorType_ = aliasName.equalsIgnoreCase(UNION_KWORD) ? SQLSelectStmt.UNION :
												aliasName.equalsIgnoreCase(MINUS_KWORD) ? SQLSelectStmt.MINUS :
												aliasName.equalsIgnoreCase(INTERSECT_KWORD) ? SQLSelectStmt.INTERSECT :
												aliasName.equalsIgnoreCase(EXCEPT_KWORD) ? SQLSelectStmt.EXCEPT : (byte)-1;
						if (operatorType_ > -1) {
							if (operatorType > -1) {
								throw new SQLParseException(matcher.start())/*return null*/;
							}
							operatorType = operatorType_;
							nextMayBeAllSelect = operatorType_ == SQLSelectStmt.UNION;
							withStmtStatus = NO_WITH_STMT; //reset
						}
						if (aliasName.equalsIgnoreCase(FROM_KWORD) || operatorType_ > (byte)-1) {
//							System.out.println("aliasName.equalsIgnoreCase(FROM_KWORD) || operatorType_ > (byte)-1 - type: " + SQLWidget.getTypeCode(stack[stackSzM1].getType()) + ", operatorType_: " + operatorType_ + ", aliasIsActualyENDKWord: " + aliasIsActualyENDKWord);
							//check if column is addable
							//must add column and then handle the FROM
							switch (stack[stackSzM1].getType())
							{
							case SQLWidget.SELECT_STATEMENT:
							case SQLWidget.WITH_STATEMENT:
							case SQLWidget.SELECT_STMT_COLUMN:
							case SQLWidget.EXISTS_CONDITION_COLUMN:
							case SQLWidget.IN_SELECT_COLUMN:
							case SQLWidget.TABLE_REF:
								if (operatorType_ < 0) {
									if (currentSelect.fromTablesCount > -1) throw new SQLParseException(matcher.start())/*return null*/;
									if (prevMatchIsClosingParenth) { //RECALL: prevMatchIsClosingParenth is left equal to true only if the alias is equal to 'FROM' and the name is not qualified
										prevMatchIsClosingParenth = false;
										prevClosingParenthForCol/*OLD BUG: currentExprCol*/.aliasName = columnName;
									}
									else if (aliasIsActualyENDKWord) { //case the alias just next t previous ENd keyword is followed by eyword FROM
										//NOTE: the alias is already set above ==> don't set it again here...
										//System.out.println("alias following ENd keyword definitely marked as handled, YEAH!!!");
										aliasIsActualyENDKWord = false;
									}
									if (mustBeNumberLiteral) {
										SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
										currentSelect.__addColumn(col);
									}
									else if (parametarizedParts != (byte)0) {
										currentSelect.__addColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR/*aliasName*/));
									}
									else {
										String pseudoColName = !tableName.isEmpty() ? null : SQLPseudoColumnNames.checkPseudoColName(columnName);
										SQLColumn col = pseudoColName != null ? new SQLPseudoColumnRef(columnName, EMPTY_STR/*aliasName*/):
															new SQLRawColumnRef(tableName, columnName, EMPTY_STR/*aliasName*/);
										currentSelect.__addColumn(col);
									}
									currentSelect.fromTablesCount = 0;
									currentSelect.outputColumnsCount++;
									continue main_loop;
								}
								else {
									switch (stack[stackSzM1].getType())
									{
									case SQLWidget.WITH_STATEMENT:
									case SQLWidget.SELECT_STMT_COLUMN:
									case SQLWidget.EXISTS_CONDITION_COLUMN:
									case SQLWidget.IN_SELECT_COLUMN:
									case SQLWidget.TABLE_REF:
										throw new SQLParseException("parse error - set operator not supported for nested select statement (" + matcher.start() + ")", matcher.start())/*return null*/;
									case SQLWidget.SELECT_STATEMENT:
										//if (currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
										if (mustBeNumberLiteral) {
											throw new SQLParseException(matcher.start())/*return null*/;
										}
										else if (parametarizedParts != (byte)0) {
											currentSelect.__addFromTable(new SQLParameterTableRef(tableName/*schema*/, columnName/*tableName*/, parametarizedParts, EMPTY_STR));
										}
										else {
											currentSelect.__addFromTable(new SQLStmtTable(tableName/*schema*/, columnName/*tableName*/));
										}
										break;
									case SQLWidget.WHERE_CLAUSE:
										if (aliasIsActualyENDKWord) { //case the alias just next t previous ENd keyword is followed by eyword FROM
											//NOTE: the alias is already set above ==> don't set it again here...
											aliasIsActualyENDKWord = false;
										}
										else if (mustBeNumberLiteral) {
											SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
											currentSelect.whereClause.__addInvolvedColumn(col);
										}
										else if (parametarizedParts != (byte)0) {
											currentSelect.whereClause.__addInvolvedColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR/*aliasName*/));
										}
										else {
											String pseudoColName = !tableName.isEmpty() ? null : SQLPseudoColumnNames.checkPseudoColName(columnName);
											SQLColumn col = pseudoColName != null ? new SQLPseudoColumnRef(columnName, EMPTY_STR/*aliasName*/):
																new SQLRawColumnRef(tableName, columnName, EMPTY_STR/*aliasName*/);
											currentSelect.whereClause.__addInvolvedColumn(col);
										}
										break;
									case SQLWidget.GROUP_BY_CLAUSE:
										if (aliasIsActualyENDKWord) { //case the alias just next t previous ENd keyword is followed by eyword FROM
											//NOTE: the alias is already set above ==> don't set it again here...
											aliasIsActualyENDKWord = false;
										}
										else if (mustBeNumberLiteral) {
											SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
											currentSelect.groupByClause.__addColumn(col);
										}
										else if (parametarizedParts != (byte)0) {
											currentSelect.groupByClause.__addColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR/*aliasName*/));
										}
										else {
											String pseudoColName = !tableName.isEmpty() ? null : SQLPseudoColumnNames.checkPseudoColName(columnName);
											SQLColumn col = pseudoColName != null ? new SQLPseudoColumnRef(columnName, EMPTY_STR/*aliasName*/):
																new SQLRawColumnRef(tableName, columnName, EMPTY_STR/*aliasName*/);
											currentSelect.groupByClause.__addColumn(col);
										}
										break;
									case SQLWidget.HAVING_CLAUSE:
										if (aliasIsActualyENDKWord) { //case the alias just next t previous ENd keyword is followed by eyword FROM
											//NOTE: the alias is already set above ==> don't set it again here...
											aliasIsActualyENDKWord = false;
										}
										else if (mustBeNumberLiteral) {
											SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
											currentSelect.havingClause.__addInvolvedColumn(col);
										}
										else if (parametarizedParts != (byte)0) {
											currentSelect.havingClause.__addInvolvedColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR/*aliasName*/));
										}
										else {
											String pseudoColName = !tableName.isEmpty() ? null : SQLPseudoColumnNames.checkPseudoColName(columnName);
											SQLColumn col = pseudoColName != null ? new SQLPseudoColumnRef(columnName, EMPTY_STR/*aliasName*/):
																new SQLRawColumnRef(tableName, columnName, EMPTY_STR/*aliasName*/);
											currentSelect.havingClause.__addInvolvedColumn(col);
										}
										break;
									case SQLWidget.ORDER_BY_CLAUSE:
										if (aliasIsActualyENDKWord) { //case the alias just next t previous ENd keyword is followed by eyword FROM
											//NOTE: the alias is already set above ==> don't set it again here...
											aliasIsActualyENDKWord = false;
										}
										else if (mustBeNumberLiteral) {
											SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
											currentSelect.orderByClause.__addColumn(col);
										}
										else if (parametarizedParts != (byte)0) {
											currentSelect.orderByClause.__addColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR/*aliasName*/));
										}
										else {
											String pseudoColName = !tableName.isEmpty() ? null : SQLPseudoColumnNames.checkPseudoColName(columnName);
											SQLColumn col = pseudoColName != null ? new SQLPseudoColumnRef(columnName, EMPTY_STR/*aliasName*/):
																new SQLRawColumnRef(tableName, columnName, EMPTY_STR/*aliasName*/);
											currentSelect.orderByClause.__addColumn(col);
										}
										break;
									}
								}
								//currentSelect.outputColumnsCount++; //BUG!!!
								break ; //continue main_loop;
							case SQLWidget.JOIN:
								if (operatorType_ < 0) { //case the stopper is FROM keyword
									throw new SQLParseException(matcher.start())/*return null*/;
								}
								else if (mustBeNumberLiteral) {
									SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
									currentJoin.__addJoinColumn(col);
								}
								else if (parametarizedParts != (byte)0) {
									currentJoin.__addJoinColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR/*aliasName*/));
								}
								else {
									SQLColumn col;
									String pseudoColName = tableName.isEmpty() ? SQLPseudoColumnNames.checkPseudoColName(columnName) : null;
									if (pseudoColName != null) {
										col = new SQLPseudoColumnRef(pseudoColName, EMPTY_STR/*aliasName*/);
									}
									else {
										tblREf = tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentJoin.joinedTbl.getAliasName().equals(tableName) ? currentJoin.joinedTbl : currentSelect.getTableByAliasExt(tableName);
										if (tblREf == null) throw new SQLParseException(matcher.start())/*return null*/; //unknown table alias
										col = new SQLColumnRef(tblREf, columnName, EMPTY_STR/*aliasName*/);
									}
									currentJoin.__addJoinColumn(col);
								}
								stack[stackSzM1--] = null; //remove join from stack
								switch (stack[stackSzM1].getType())
								{
								case SQLWidget.SELECT_STATEMENT:
								case SQLWidget.WITH_STATEMENT:
									currentSelect = (SQLSelectStmt)stack[stackSzM1];
									break;
								case SQLWidget.EXISTS_CONDITION_COLUMN:
								case SQLWidget.IN_SELECT_COLUMN:
								case SQLWidget.SELECT_STMT_COLUMN:
									currentSelect = ((SQLSelectStmtColumn)stack[stackSzM1]).selectStmt;
									break;
								case SQLWidget.TABLE_REF:
									currentSelect = ((SQLSelectTable)stack[stackSzM1]).selectStmt;
									break;
								default:
									throw new SQLParseException(matcher.start())/*return null*/;
								}
								//currentSelect.fromTablesCount = 0; //BUG!!!
								//currentSelect.outputColumnsCount++; //BUG
								stack[stackSzM1].__trim(); //remove select statement from stack
								break ; //continue main_loop;
							case SQLWidget.WHERE_CLAUSE:
								if (operatorType_ < 0) { //case the stopper is FROM keyword
									throw new SQLParseException(matcher.start())/*return null*/;
								}
								else if (mustBeNumberLiteral) {
									SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
									((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(col);
								}
								else if (parametarizedParts != (byte)0) {
									((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR/*aliasName*/));
								}
								else {
									((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(new SQLRawColumnRef(tableName, columnName, EMPTY_STR/*aliasName*/));
								}
								stack[stackSzM1].__trim();
								stack[stackSzM1--] = null; //remove from stack
								stack[stackSzM1].__trim(); //remove select statement from stack
								break ; //continue main_loop;
							case SQLWidget.GROUP_BY_CLAUSE:
								if (operatorType_ < 0) { //case the stopper is FROM keyword
									throw new SQLParseException(matcher.start())/*return null*/;
								}
								else if (mustBeNumberLiteral) {
									SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
									((SQLGroupByClause)stack[stackSzM1]).__addColumn(col);
								}
								else if (parametarizedParts != (byte)0) {
									((SQLGroupByClause)stack[stackSzM1]).__addColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR/*aliasName*/));
								}
								else {
									((SQLGroupByClause)stack[stackSzM1]).__addColumn(new SQLRawColumnRef(tableName, columnName, EMPTY_STR/*aliasName*/));
								}
								stack[stackSzM1].__trim();
								stack[stackSzM1--] = null; //remove from stack
								stack[stackSzM1].__trim(); //remove select statement from stack
								break ; //continue main_loop;
							case SQLWidget.HAVING_CLAUSE:
								if (operatorType_ < 0) { //case the stopper is FROM keyword
									throw new SQLParseException(matcher.start())/*return null*/;
								}
								else if (mustBeNumberLiteral) {
									SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
									((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(col);
								}
								else if (parametarizedParts != (byte)0) {
									((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR/*aliasName*/));
								}
								else {
									((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(new SQLRawColumnRef(tableName, columnName, EMPTY_STR/*aliasName*/));
								}
								stack[stackSzM1].__trim();
								stack[stackSzM1--] = null; //remove from stack
								stack[stackSzM1].__trim(); //remove select statement from stack
								break; //continue main_loop;
							case SQLWidget.ORDER_BY_CLAUSE:
								if (operatorType_ < 0) { //case the stopper is FROM keyword
									throw new SQLParseException(matcher.start())/*return null*/;
								}
								else if (mustBeNumberLiteral) {
									SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
									((SQLOrderByClause)stack[stackSzM1]).__addColumn(col);
								}
								else if (parametarizedParts != (byte)0) {
									((SQLOrderByClause)stack[stackSzM1]).__addColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR/*aliasName*/));
								}
								else {
									((SQLOrderByClause)stack[stackSzM1]).__addColumn(new SQLRawColumnRef(tableName, columnName, EMPTY_STR/*aliasName*/));
								}
								stack[stackSzM1].__trim();
								stack[stackSzM1--] = null; //remove from stack
								stack[stackSzM1].__trim(); //remove select statement from stack
								break; //continue main_loop;
							default:
								throw new SQLParseException("parse error - " + stack[stackSzM1].getClass().getName() + "-" + matcher.start(), matcher.start())/*return null*/;
							}
//							System.out.println("YEAH, YEAH, YEAH!!! - stackSzM1: " + stackSzM1);
							if (stackSzM1 == 0) {
								setStmt = new SQLCombiningStatement();
								setStmt.__add(currentSelect); //add the first select statement
								stack[0] = setStmt;
								returnStmt = setStmt;
							}
							else {
								stack[stackSzM1--] = null; //remove the select statement from the stack
							}
							continue main_loop;
						}
						else if (aliasName.equalsIgnoreCase(WHERE_KWORD)) {
							__switch_type_block:
							switch (stack[stackSzM1].getType())
							{
							case SQLWidget.SELECT_STATEMENT:
							case SQLWidget.WITH_STATEMENT:
							case SQLWidget.SELECT_STMT_COLUMN:
							case SQLWidget.EXISTS_CONDITION_COLUMN:
							case SQLWidget.IN_SELECT_COLUMN:
							case SQLWidget.TABLE_REF:
								if (currentSelect.columnsCount < 1 || currentSelect.whereClause != null || currentSelect.limit > -1) {
									throw new SQLParseException(matcher.start())/*return null*/; //unexpected where clause
								}
								else if (mustBeNumberLiteral) {
									throw new SQLParseException(matcher.start())/*return null*/; //unexpected number literal for a table name
								}
								else if (parametarizedParts != (byte)0) {
									currentSelect.__addFromTable(new SQLParameterTableRef(tableName/*schema*/, columnName/*tableName*/, parametarizedParts, EMPTY_STR));
								}
								else {
									currentSelect.__addFromTable(new SQLStmtTable(tableName/*schema*/, columnName/*tableName*/));
								}
								currentSelect.whereClause = new SQLWhereClause();
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = currentSelect.whereClause;
								continue main_loop;
							case SQLWidget.JOIN: //join must be ended by new WHERE clause
								if (currentJoin.joinColumnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
								stack[stackSzM1--] = null; //remove join from stack
								switch(stack[stackSzM1].getType())
								{
								case SQLWidget.SELECT_STATEMENT:
								case SQLWidget.WITH_STATEMENT:
								case SQLWidget.SELECT_STMT_COLUMN:
								case SQLWidget.EXISTS_CONDITION_COLUMN:
								case SQLWidget.IN_SELECT_COLUMN:
								case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
									if (currentJoin.joinColumnsCount > 0) {
										SQLColumn col;
										if (mustBeNumberLiteral) {
											col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
										}
										else if (parametarizedParts != (byte)0) {
											col = new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR);
										}
										else {
											if (tableName.isEmpty()) {
												byte literalType = SQLLiteral.checkLiteralType(columnName);
												if (literalType != SQLLiteral.STRING_LITERAL) {
													col = new SQLLiteral(columnName/*value*/, literalType, EMPTY_STR);
												}
												else {
													col = new SQLColumnRef(SQLStmtTable.NO_TABLE, columnName, EMPTY_STR);
												}
											}
											else {
												SQLTableRef tblRef = currentSelect.getTableByAliasExt(tableName);
												if (tblRef != null) {
													col = new SQLColumnRef(tblRef, columnName, EMPTY_STR);
												}
												else {
													col = new SQLRawColumnRef(tableName, columnName, EMPTY_STR);
												}
											}
										}
										currentJoin.__addJoinColumn(col);
									}
									else {
										if (mustBeNumberLiteral) {
											throw new SQLParseException(matcher.start()); //unexpected number literal for a table name
										}
										else if (parametarizedParts != (byte)0) {
											currentSelect.__addFromTable(new SQLParameterTableRef(tableName/*schema*/, columnName/*tableName*/, parametarizedParts, EMPTY_STR));
										}
										else {
											currentSelect.__addFromTable(new SQLStmtTable(tableName/*schema*/, columnName/*tableName*/));
										}
									}
									currentSelect.whereClause = new SQLWhereClause();
									stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
									stack[stackSzM1] = currentSelect.whereClause;
									continue main_loop;
								default:
									throw new SQLParseException(matcher.start())/*return null*/;
								}
							default :
								throw new SQLParseException(matcher.start())/*return null*/;
							}
						}
						else if (aliasName.equalsIgnoreCase(WHEN_KWORD)) {
							if (stackSzM1 < 0 || matcher.group(7) != null/*keywordASIsPresent*/ || stack[stackSzM1].getType() != SQLWidget.CASE_EXPR) throw new SQLParseException(matcher.start())/*return null*/;
							SQLCaseExprColumn caseCol = (SQLCaseExprColumn)stack[stackSzM1];
							switch(caseCol.blockType)
							{
							case SQLCaseExprColumn.WHEN:
							case SQLCaseExprColumn.ELSE:
							case SQLCaseExprColumn.END:
								throw new SQLParseException(matcher.start())/*return null*/;
							}
							SQLColumn col;
							if (mustBeNumberLiteral) {
								col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
							}
							else if (tableName.isEmpty()) {
								byte literalType = SQLLiteral.checkLiteralType(columnName);
								if (literalType != SQLLiteral.STRING_LITERAL) {
									col = new SQLLiteral(columnName/*value*/, literalType, EMPTY_STR);
								}
								else if (parametarizedParts != (byte)0) {
									col = new SQLParameterColumn(EMPTY_STR, columnName, parametarizedParts, EMPTY_STR);
								}
								else {
									col = new SQLColumnRef(SQLStmtTable.NO_TABLE, columnName, EMPTY_STR);
								}
							}
							else {
								if (currentSelect.fromTablesCount > 0) {
									if (parametarizedParts != (byte)0) {
										col = new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR);
									}
									else {
										SQLTableRef tblRef = currentSelect.getTableByAliasExt(tableName);
										if (tblRef != null) {
											col = new SQLColumnRef(tblRef, columnName, EMPTY_STR);
										}
										else {
											col = new SQLRawColumnRef(tableName, columnName, EMPTY_STR);
										}
									}
								}
								else if (parametarizedParts != (byte)0) {
									col = new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR);
								}
								else {
									col = new SQLRawColumnRef(tableName, columnName, EMPTY_STR);
								}
							}
							caseCol.__addInvolvedColumn(col);
							caseCol.blockType = SQLCaseExprColumn.WHEN;
							continue main_loop;
						}
						else if (aliasName.equalsIgnoreCase(THEN_KWORD)) {
							//System.out.println("2 and THEN");
							if (stackSzM1 < 0 || matcher.group(7) != null/*keywordASIsPresent*/ || stack[stackSzM1].getType() != SQLWidget.CASE_EXPR) throw new SQLParseException(matcher.start())/*return null*/;
							SQLCaseExprColumn caseCol = (SQLCaseExprColumn)stack[stackSzM1];
							switch(caseCol.blockType)
							{
							case SQLCaseExprColumn.THEN:
							case SQLCaseExprColumn.ELSE:
							case SQLCaseExprColumn.END:
								throw new SQLParseException(matcher.start())/*return null*/;
							}
							SQLColumn col;
							if (mustBeNumberLiteral) {
								col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
							}
							else if (tableName.isEmpty()) {
								byte literalType = SQLLiteral.checkLiteralType(columnName);
								//System.out.println(" - literalType: " + literalType);
								if (literalType != SQLLiteral.STRING_LITERAL) {
									col = new SQLLiteral(columnName/*value*/, literalType, EMPTY_STR);
									//System.out.println(" - col: " + col);
								}
								else if (parametarizedParts != (byte)0) {
									col = new SQLParameterColumn(EMPTY_STR, columnName, parametarizedParts, EMPTY_STR);
								}
								else {
									col = new SQLColumnRef(SQLStmtTable.NO_TABLE, columnName, EMPTY_STR);
								}
							}
							else {
								if (currentSelect.fromTablesCount > 0) {
									if (parametarizedParts != (byte)0) {
										col = new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR);
									}
									else {
										SQLTableRef tblRef = currentSelect.getTableByAliasExt(tableName);
										if (tblRef != null) {
											col = new SQLColumnRef(tblRef, columnName, EMPTY_STR);
										}
										else {
											col = new SQLRawColumnRef(tableName, columnName, EMPTY_STR);
										}
									}
								}
								else if (parametarizedParts != (byte)0) {
									col = new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR);
								}
								else {
									col = new SQLRawColumnRef(tableName, columnName, EMPTY_STR);
								}
							}
							caseCol.__addInvolvedColumn(col);
							caseCol.blockType = SQLCaseExprColumn.THEN;
							continue main_loop;
						}
						else if (aliasName.equalsIgnoreCase(ELSE_KWORD)) { //case ELSE keyword
							if (stackSzM1 < 0 || matcher.group(7) != null/*keywordASIsPresent*/ || stack[stackSzM1].getType() != SQLWidget.CASE_EXPR) throw new SQLParseException(matcher.start())/*return null*/;
							SQLCaseExprColumn caseCol = (SQLCaseExprColumn)stack[stackSzM1];
							switch(caseCol.blockType)
							{
							case SQLCaseExprColumn.ELSE:
							case SQLCaseExprColumn.WHEN:
							case SQLCaseExprColumn.END:
								throw new SQLParseException(matcher.start())/*return null*/;
							}
							SQLColumn col ;
							if (mustBeNumberLiteral) {
								col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
							}
							else {
								col = parametarizedParts != (byte)0 ? new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR) :
															__newColumn(tableName/*tableAlias*/, columnName, currentSelect);
							}
							caseCol.__addInvolvedColumn(col);
							caseCol.blockType = SQLCaseExprColumn.ELSE; //BUG-FIX-2017-06-28 - was mistakenly setting stage/blokcType to SQLCaseExprColumn.WHEN
							continue main_loop;
						} //end case ELSE keyword
						else if (aliasName.equalsIgnoreCase(ORDER_KWORD)) { //
							if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
							start = skipWs(sqlSelectStmt, matcher.end(), sqlSelectStmtLen);
							switch(stack[stackSzM1].getType())
							{
							case SQLWidget.SELECT_STATEMENT:
							case SQLWidget.WITH_STATEMENT:
							case SQLWidget.SELECT_STMT_COLUMN:
							case SQLWidget.EXISTS_CONDITION_COLUMN:
							case SQLWidget.IN_SELECT_COLUMN:
							case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
								if (currentSelect.orderByClause != null) throw new SQLParseException(matcher.start())/*return null*/;
								SQLColumn col;
								if (mustBeNumberLiteral) {
									col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
								}
								else {
									col = parametarizedParts != (byte)0 ? new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR) :
												__newColumn(tableName/*tableAlias*/, columnName, currentSelect);
								}
								currentSelect.__addColumn(col); //NOTE: do not remove the select from stack after adding the colum to it!!!
								currentSelect.orderByClause = new SQLOrderByClause();
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = currentSelect.orderByClause;
								continue main_loop;
							case SQLWidget.JOIN: //join ended by keyword ORDER BY
								switch(stack[stackSzM1 - 1].getType())
								{
								case SQLWidget.SELECT_STATEMENT:
								case SQLWidget.WITH_STATEMENT:
								case SQLWidget.SELECT_STMT_COLUMN:
								case SQLWidget.EXISTS_CONDITION_COLUMN:
								case SQLWidget.IN_SELECT_COLUMN:
									if (mustBeNumberLiteral) {
										col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
									}
									else {
										col = parametarizedParts != (byte)0 ? new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR) :
												__newColumn(tableName/*tableAlias*/, columnName, currentSelect);
									}
									((SQLJoin)stack[stackSzM1]).__addJoinColumn(col);
									currentSelect.orderByClause = new SQLOrderByClause();
									stack[stackSzM1] = currentSelect.orderByClause; //replace join with order by clause in the stack
									continue main_loop;
								default:
									throw new SQLParseException(matcher.start())/*return null*/;
								}
							case SQLWidget.WHERE_CLAUSE:
								if (mustBeNumberLiteral) {
									col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
								}
								else {
									col = parametarizedParts != (byte)0 ? new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR) :
												__newColumn(tableName/*tableAlias*/, columnName, currentSelect);
								}
								((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(col);
								currentSelect.orderByClause = new SQLOrderByClause();
								stack[stackSzM1] = currentSelect.orderByClause; //replace where clause with order by clause in the stack
								continue main_loop;
							case SQLWidget.HAVING_CLAUSE:
								if (mustBeNumberLiteral) {
									col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
								}
								else {
									col = parametarizedParts != (byte)0 ? new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR) :
												__newColumn(tableName/*tableAlias*/, columnName, currentSelect);
								}
								((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(col);
								currentSelect.orderByClause = new SQLOrderByClause();
								stack[stackSzM1] = currentSelect.orderByClause; //replace having clause with order by clause in the stack
								continue main_loop;
							case SQLWidget.GROUP_BY_CLAUSE:
								if (mustBeNumberLiteral) {
									col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
								}
								else {
									col = parametarizedParts != (byte)0 ? new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR) :
												__newColumn(tableName/*tableAlias*/, columnName, currentSelect);
								}
								((SQLGroupByClause)stack[stackSzM1]).__addColumn(col);
								currentSelect.orderByClause = new SQLOrderByClause();
								stack[stackSzM1] = currentSelect.orderByClause; //replace join with order by clause in the stack
								continue main_loop;
							case SQLWidget.FUNC_ANALYTIC_CLAUSE:
								SQLFunctionAnalyticClause analyticClause = ((SQLFunctionAnalyticClause)stack[stackSzM1]);
								if (analyticClause.startedOrderBy) {
									throw new SQLParseException(matcher.start())/*return null*/;
								}
								else if (mustBeNumberLiteral) {
									col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
								}
								else {
									col = parametarizedParts != (byte)0 ? new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR) :
												__newColumn(tableName/*tableAlias*/, columnName, currentSelect);
								}
								analyticClause.__addInvolvedColumn(col);
								analyticClause.startedOrderBy = true;
								analyticClause.orderByColsOffset = analyticClause.involvedColumnsCount;
								continue main_loop;
							default:
								throw new SQLParseException(matcher.start())/*return null*/;
							}
						}
						else if (aliasName.equalsIgnoreCase(GROUP_KWORD)) {
							if (asIndicatorIsPresent || stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
							else if (!matcher.find()) throw new SQLParseException(matcher.start())/*return null*/;
							start = matcher.start();
							if (!zone_equals_ci(sqlSelectStmt, start, matcher.end(), BY_KWORD)) throw new SQLParseException(matcher.start())/*return null*/;
							switch(stack[stackSzM1].getType())
							{
							case SQLWidget.SELECT_STATEMENT:
							case SQLWidget.WITH_STATEMENT:
							case SQLWidget.SELECT_STMT_COLUMN:
							case SQLWidget.EXISTS_CONDITION_COLUMN:
							case SQLWidget.IN_SELECT_COLUMN:
								if (currentSelect.groupByClause != null || currentSelect.orderByClause != null) throw new SQLParseException(matcher.start())/*return null*/;
								currentSelect.groupByClause = new SQLGroupByClause();
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = currentSelect.groupByClause;
								continue main_loop;
							case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
								if (!(stack[stackSzM1] instanceof SQLSelectTable) || currentSelect.groupByClause != null || currentSelect.orderByClause != null) throw new SQLParseException(matcher.start())/*return null*/;
								currentSelect.groupByClause = new SQLGroupByClause();
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = currentSelect.groupByClause;
								continue main_loop;
							case SQLWidget.JOIN: //join ended by keyword GROUP BY
								switch(stack[stackSzM1].getType())
								{
								case SQLWidget.SELECT_STATEMENT:
								case SQLWidget.WITH_STATEMENT:
								case SQLWidget.SELECT_STMT_COLUMN:
								case SQLWidget.EXISTS_CONDITION_COLUMN:
								case SQLWidget.IN_SELECT_COLUMN:
									currentSelect.groupByClause = new SQLGroupByClause();
									//stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1); //commented out because the join must be removed before the group by is added ==> replace join with group by in stack
									stack[stackSzM1] = currentSelect.groupByClause;
									continue main_loop;
								default:
									throw new SQLParseException(matcher.start())/*return null*/;
								}
							case SQLWidget.WHERE_CLAUSE:
								currentSelect.groupByClause = new SQLGroupByClause();
								//stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);  //commented out because the where clause must be removed before the group by is added ==> replace join with group by in stack
								stack[stackSzM1] = currentSelect.groupByClause;
								continue main_loop;
							default:
								throw new SQLParseException(matcher.start())/*return null*/;
							}
						}
						else if (aliasName.equalsIgnoreCase(HAVING_KWORD)) {
							if (asIndicatorIsPresent || stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
							switch(stack[stackSzM1].getType())
							{
							case SQLWidget.SELECT_STATEMENT:
							case SQLWidget.WITH_STATEMENT:
							case SQLWidget.SELECT_STMT_COLUMN:
							case SQLWidget.EXISTS_CONDITION_COLUMN:
							case SQLWidget.IN_SELECT_COLUMN:
								if (currentSelect.havingClause != null || currentSelect.groupByClause == null || currentSelect.limit > -1 || currentSelect.fromTablesCount < 0) {
									throw new SQLParseException(matcher.start())/*return null*/;
								}
								else if (mustBeNumberLiteral) {
									throw new SQLParseException(matcher.start());
								}
								else if (parametarizedParts != (byte)0) {
									currentSelect.__addFromTable(new SQLParameterTableRef(tableName/*schema*/, columnName/*tableName*/, parametarizedParts, EMPTY_STR));
								}
								else {
									currentSelect.__addFromTable(new SQLStmtTable(tableName/*schema*/, columnName/*tableName*/));
								}
								currentSelect.havingClause = new SQLHavingClause();
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = currentSelect.havingClause;
								continue main_loop;
							case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
								if (!(stack[stackSzM1] instanceof SQLSelectTable) || currentSelect.havingClause != null || currentSelect.groupByClause == null || currentSelect.limit > -1 || currentSelect.fromTablesCount < 0) {
									throw new SQLParseException(matcher.start())/*return null*/;
								}
								else if (mustBeNumberLiteral) {
									throw new SQLParseException(matcher.start());
								}
								else if (parametarizedParts != (byte)0) {
									currentSelect.__addFromTable(new SQLParameterTableRef(tableName/*schema*/, columnName/*tableName*/, parametarizedParts, EMPTY_STR));
								}
								else {
									currentSelect.__addFromTable(new SQLStmtTable(tableName/*schema*/, columnName/*tableName*/));
								}
								currentSelect.havingClause = new SQLHavingClause();
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = currentSelect.havingClause;
								continue main_loop;
							case SQLWidget.WHERE_CLAUSE:
								SQLColumn col;
								if (mustBeNumberLiteral) {
									col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
								}
								else {
									col = parametarizedParts != (byte)0 ? new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR) :
												__newColumn(tableName, columnName, currentSelect);
								}
								((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(col);
								currentSelect.havingClause = new SQLHavingClause();
								//stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1); //commented out to just replace instead of removing WHERE or ORDER BY and then adding the new HAVING to the stack
								stack[stackSzM1] = currentSelect.havingClause;
								continue main_loop;
							case SQLWidget.GROUP_BY_CLAUSE:
								if (mustBeNumberLiteral) {
									col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
								}
								else {
									col = parametarizedParts != (byte)0 ? new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR) :
											__newColumn(tableName, columnName, currentSelect);
								}
								((SQLGroupByClause)stack[stackSzM1]).__addColumn(col);
								currentSelect.havingClause = new SQLHavingClause();
								//stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1); //commented out to just replace instead of removing WHERE or ORDER BY and then adding the new HAVING to the stack
								stack[stackSzM1] = currentSelect.havingClause;
								continue main_loop;
							default:
								throw new SQLParseException("parse error - " + stack[stackSzM1].getClass().getName() + "-" + matcher.start(), matcher.start())/*return null*/;
							}

						}
						else if (aliasName.equalsIgnoreCase(JOIN_KWORD)) { //BUG-FIX-2017-06-28 - was not handled
							System.out.println("JOIN keyword as ALIAS!!!");
							if (asIndicatorIsPresent || stackSzM1 < 0) {
								throw new SQLParseException("parse error (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
							}
							else if (currentSelect == null || currentSelect.fromTablesCount < 0) {
								throw new SQLParseException("parse error - unexpected JOIN keyword (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
							}
							int start_ = skipWs(sqlSelectStmt, matcher.end(), sqlSelectStmtLen);
							if (start_ >= sqlSelectStmtLen) {
								throw new SQLParseException("parse error - end of SQL statement reached unexpectedly (offset=" + sqlSelectStmtLen + ")", sqlSelectStmtLen)/*return null*/;
							}
							boolean isLefetParenth = sqlSelectStmt.charAt(start_) == '(';
							if (isLefetParenth) {
								matcher.find(); //get the cursor to move to the position of the left parenthesis that follows JOIN keyword
							}
							byte joinType = columnName.equalsIgnoreCase(LEFT_KWORD) ? SQLJoin.LEFT_OUTER_JOIN :
											columnName.equalsIgnoreCase(INNER_KWORD) ? SQLJoin.INNER_JOIN :
											columnName.equalsIgnoreCase(RIGHT_KWORD) ? SQLJoin.RIGHT_OUTER_JOIN :
											columnName.equalsIgnoreCase(FULL_KWORD) ? SQLJoin.FULL_OUTER_JOIN : (byte)-1;
							if (currentSelect.fromTablesCount ==  0) {
								if (joinType < (byte)0) {
									SQLStmtTable fromTbl = new SQLStmtTable(tableName/*schema*/, columnName/*tableName*/);
									currentSelect.__addFromTable(fromTbl);
									previousTblOrJoin = fromTbl;
									joinType = SQLJoin.INNER_JOIN;
								}
								else {
									throw new SQLParseException("parse error - join driving table is missing (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
								}
							}
							else {
								switch(stack[stackSzM1].getType())
								{
								case SQLWidget.JOIN:
									SQLJoin join = (SQLJoin)stack[stackSzM1];
									if (join.joinedTbl == null) { //case the current join must be ended by closing parenthesis
										throw new SQLParseException("parse error - closing parenthesis expected (offset=" + matcher.start(8) + ")", matcher.start(8))/*return null*/;
									}
									previousTblOrJoin = join;
									if (joinType < (byte)0) {
										SQLColumn joinCol;
										if (tableName.isEmpty()) {
											if (SQLLiteral.NULL_KEYWORD.equals(columnName)) {
												joinCol = SQLLiteral.NULL;
											}
											else {
												byte literalType = SQLLiteral.checkLiteralType(columnName);
												if (literalType > (byte)-1) {
													joinCol = new SQLLiteral(columnName/*value*/, literalType, EMPTY_STR);
												}
												else {
													String pseudoColName = SQLPseudoColumnNames.checkPseudoColName(columnName);
													if (pseudoColName != null) {
														joinCol = new SQLPseudoColumnRef(pseudoColName, EMPTY_STR);
													}
													else {
														joinCol = new SQLColumnRef(SQLStmtTable.NO_TABLE, columnName, EMPTY_STR);
													}
												}
											}
										}
										else {
											SQLTableRef tblRef = currentSelect.getTableByAliasExt(tableName);
											joinCol = tblRef != null ? new SQLColumnRef(tblRef, columnName, EMPTY_STR) : new SQLRawColumnRef(tableName, columnName, EMPTY_STR);
										}
										join.__addJoinColumn(joinCol);
										joinType = SQLJoin.INNER_JOIN;
									}
									stack[stackSzM1--] = null; //remove join from stack
									break ;
								case SQLWidget.SELECT_STATEMENT:
								case SQLWidget.WITH_STATEMENT:
									if (joinType < (byte)0) {
										SQLStmtTable fromTbl = new SQLStmtTable(tableName/*schema*/, columnName/*tableName*/);
										currentSelect.__addFromTable(fromTbl);
										previousTblOrJoin = fromTbl;
										joinType = SQLJoin.INNER_JOIN;
									}
									else if (currentSelect.fromTablesCount == 0) {
										throw new SQLParseException("parse error - join driving table is missing (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
									}
									else {
										previousTblOrJoin = currentSelect.fromTables[currentSelect.fromTablesCount - 1];
									}
									break ;
								default:
									throw new SQLParseException("parse error - unexpected JOIN keyword (offset=" + matcher.start(8) + ")", matcher.start(8))/*return null*/;
								}
							}
							if (!isLefetParenth) { //case JOIN keyword is not followed by left parenthesis
								if (!matcher.find()) {
									throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly JOIN keyword", sqlSelectStmtLen)/*return null*/;
								}
								else if (matcher.start(5) >= matcher.end(5)) {
									throw new SQLParseException("parse error - table reference expected after JOIN keyword (offset=" + matcher.start(8) + ")", matcher.start(8))/*return null*/;
								}
								else if (matcher.start(7) < matcher.end(7)) {
									throw new SQLParseException("parse error - unexpected AS keyword after JOIN keyword (offset=" + matcher.start(8) + ")", matcher.start(8))/*return null*/;
								}
								else if (matcher.start(6) >= matcher.end(6)) { //case the table ref is a simple name
									grp = EMPTY_STR; //this actually represents the schema name
									tableName = matcher.group(5);
								}
								else {
									grp = matcher.group(5); //this actually represents the schema name
									tableName = matcher.group(6);
								}
								if (matcher.start(8) < matcher.end(8)) {
									aliasName = matcher.group(8);
								}
								else {
									aliasName = EMPTY_STR;
								}
								if (!ON_KWORD.equals(aliasName)) {
									if (!matcher.find()) {
										throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly JOIN keyword", sqlSelectStmtLen)/*return null*/;
									}
									else if (matcher.start(5) >= matcher.end(5)) {
										throw new SQLParseException("parse error - ON keyword expected (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
									}
									else if (matcher.start(6) < matcher.end(6)) {
										throw new SQLParseException("parse error - unexpected qualified name (offset=" + matcher.start(6) + ")", matcher.start(6))/*return null*/;
									}
									else if (matcher.start(7) < matcher.end(7)) {
										throw new SQLParseException("parse error - unexpected AS keyword (offset=" + matcher.start(7) + ")", matcher.start(7))/*return null*/;
									}
									else if (matcher.start(8) < matcher.end(8)) {
										throw new SQLParseException("parse error - unexpected alias name (offset=" + matcher.start(8) + ")", matcher.start(8))/*return null*/;
									}
									else if (matcher.end(5) - matcher.start(5) != 2 || !zone_equals_ci(sqlSelectStmt, matcher.start(5), matcher.end(5), ON_KWORD)) {
										throw new SQLParseException("parse error - ON keyword expected (offset=" + matcher.start(5) + ")", matcher.start(5))/*return null*/;
									}
								}
								else {
									aliasName = EMPTY_STR;
								}
								start_ = skipWs(sqlSelectStmt, matcher.end(), sqlSelectStmtLen);
								if (start_ >= sqlSelectStmtLen) {
									throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly", sqlSelectStmtLen)/*return null*/;
								}
								else if (sqlSelectStmt.charAt(start_) == '(') {
									if (!matcher.find() || matcher.start() != start_) {
										throw new SQLParseException(
										"System in illegal state - left parenthesis character at offset " + start_ + " not captured"
										, start_
										);
									}
									start_ = skipWs(sqlSelectStmt, start_ + 1, sqlSelectStmtLen);
									if (start_ >= sqlSelectStmtLen) {
										throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly JOIN keyword", sqlSelectStmtLen)/*return null*/;
									}
									else if (zone_equals_ci(sqlSelectStmt, start_, sqlSelectStmtLen, SELECT_KWORD)) {
										throw new SQLParseException("parse error - unexpected SELECT keyword (offset=" + start_ + ")",start_)/*return null*/;
									}
								}
								SQLTableRef tbl = aliasName.isEmpty() ? new SQLStmtTable(grp, tableName) : new SQLStmtTableAlias(aliasName, new SQLStmtTable(grp, tableName));
								currentSelect.__addFromTable(tbl);
								currentJoin = new SQLJoin(joinType/*SQLJoin.INNER_JOIN*/, tbl);
								currentSelect.__addJoin(currentJoin);
								currentJoin.tbl = previousTblOrJoin;
								previousTblOrJoin = currentJoin;
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = currentJoin;
							} //end case JOIN keyword is not followed by left parenthesis
							else {
								start_ = skipWs(sqlSelectStmt, matcher.end(), sqlSelectStmtLen);
								if (!zone_equals_ci(sqlSelectStmt, start_, sqlSelectStmtLen, SELECT_KWORD)) {
									throw new SQLParseException("parse error - SELECT keyword expected (offset=" + start_ + ")",start_)/*return null*/;
								}
								start_ += 6;
								if (start_ >= sqlSelectStmtLen) {
									throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly", sqlSelectStmtLen)/*return null*/;
								}
								else if (!is_ws_char(sqlSelectStmt.charAt(start_))) {
									start_ -= 6;
									throw new SQLParseException("parse error - SELECT keyword expected (offset=" + start_ + ")",start_)/*return null*/;
								}
								start_ -= 6; //restore start_
								if (!matcher.find() || matcher.start() != start_) {
									throw new SQLParseException("System in illegal state - SELECT keyword at offset " + start_ + " is not captured", sqlSelectStmtLen)/*return null*/;
								}
								selectStmt = new SQLSelectStmt();
								SQLSelectTable tbl = new SQLSelectTable(selectStmt);
								tbl.parentSelectStmt = currentSelect; //OLD KO: currentSelect != null ? currentSelect.parentSelectStmt : null;
								currentSelect.__addFromTable(tbl);
								selectStmt.fromTablesCount = -1;
								currentJoin = new SQLJoin(joinType/*SQLJoin.INNER_JOIN*/);
								currentSelect.__addJoin(currentJoin);
								currentJoin.tbl = previousTblOrJoin;
								currentJoin.joinedTbl = null; //ensure it is set to null for it to be set when the closing
								previousTblOrJoin = tbl;
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = currentJoin;
								currentSelect = selectStmt;
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = tbl;
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = selectStmt;
							}
							continue main_loop;
						} //end if (aliasName.equalsIgnoreCase(JOIN_KWORD))
						else if (columnName.equalsIgnoreCase(JOIN_KWORD) || aliasName.equalsIgnoreCase(OUTER_KWORD)) { //case of JOIN that is hiding!?
							System.out.println("!!!!!!!!!!!found a join that tried to hide (" + tableName + "::" + columnName + "::" + aliasName + ")");
							if (stackSzM1 < 0 || (stack[stackSzM1].getType() != SQLWidget.SELECT_STATEMENT && stack[stackSzM1].getType() != SQLWidget.WITH_STATEMENT)) {
								throw new SQLParseException("parse error - unexpected JOIN related keyword (offset=" + matcher.start() + ")",matcher.start())/*return null*/;
							}
							if (matcher.start(7) < matcher.end(7)) { //case AS key word is present!!!
								throw new SQLParseException("parse error - unexpected AS keyword (offset=" + matcher.start(7) + ")",matcher.start(7))/*return null*/;
							}
							byte joinType = -1;
							if (!tableName.isEmpty()) {
//								NOTE: WAS KO - IT Is REPLACED WITH CODE JUST BELOW
//								if (currentSelect.fromTablesCount == 0) {
//									currentSelect.__addFromTable(new SQLStmtTable(EMPTY_STR/*schema*/, tableName));
//								}
//								else {
//									SQLTableRef tbl = currentSelect.fromTables[currentSelect.fromTablesCount - 1];
//									String tblAlias = null;
//									if (tbl.isSQLStmtTableAlias()) {
//										SQLStmtTableAlias tbl_ = tbl.asSQLStmtTableAlias();
//										if (tbl_.aliasName != null && !tbl_.aliasName.isEmpty()) {
//											throw new SQLParseException("parse error - unexpected token related keyword (offset=" + matcher.start() + ")",matcher.start())/*return null*/;
//										}
//										tbl_.aliasName = aliasName;
//									}
//									else if (tbl.isSQLSelectTbl()) {
//										SQLSelectTbl tbl_ = tbl.asSQLSelectTbl();
//										if (tbl_.name != null && !tbl_.name.isEmpty()) {
//											throw new SQLParseException("parse error - unexpected token related keyword (offset=" + matcher.start() + ")",matcher.start())/*return null*/;
//										}
//										tbl_.name = aliasName;
//									}
//									else if (tbl.isSQLStmtTable()) {
//										//should normally neer get here!!!
//										throw new SQLParseException("System in illegal state - unexpected token related keyword, alias name should have been captured along with table name (tableName=" + tbl.getTableName() + ", offset=" + matcher.start() + ")",matcher.start())/*return null*/;
//									}
//									else {
//										throw new SQLParseException("System in illegal error - kind of table not yet full handled? (offset=" + matcher.start() + ")",matcher.start())/*return null*/;
//									}
//								}
								if (aliasName.equalsIgnoreCase(OUTER_KWORD)) {
									throw new SQLParseException("parse error - unexpected OUTER keyword (offset=" + matcher.start(8) + ")",matcher.start(8))/*return null*/;
								}
								else if (!tableName.equalsIgnoreCase(INNER_KWORD)) {
									joinType = SQLJoin.INNER_JOIN;
								}
								else if (!tableName.equalsIgnoreCase(LEFT_KWORD)) {
									joinType = SQLJoin.LEFT_OUTER_JOIN;
								}
								else if (!tableName.equalsIgnoreCase(RIGHT_KWORD)) {
									joinType = SQLJoin.RIGHT_OUTER_JOIN;
								}
								else if (!tableName.equalsIgnoreCase(FULL_KWORD)) {
									joinType = SQLJoin.FULL_OUTER_JOIN;
								}
								else {
									throw new SQLParseException("parse error - unexpected JOIN keyword (offset=" + matcher.start(6) + ")",matcher.start(6))/*return null*/;
								}
							}
							byte joinsToNestedTable = 0;
							String joinTblAlias = EMPTY_STR;
							String joinTblSchema = EMPTY_STR;
							boolean tableNameMayBecomeTableAlias = false;
							if (aliasName.equalsIgnoreCase(OUTER_KWORD)) { //case JOIN keyword is expected
								joinType = LEFT_KWORD.equalsIgnoreCase(columnName) ? SQLJoin.LEFT_OUTER_JOIN :
												RIGHT_KWORD.equalsIgnoreCase(columnName) ? SQLJoin.RIGHT_OUTER_JOIN :
												FULL_KWORD.equalsIgnoreCase(columnName) ? SQLJoin.FULL_OUTER_JOIN : (byte)-1;
								if (joinType < (byte)0) {
									throw new SQLParseException("parse error - invalid/unknown join type (keyword=" + columnName + ", offset=" + matcher.start() + ")",matcher.start())/*return null*/;
								}
								__skip_comments_loop:
								do
								{
									if (!matcher.find()) {
										throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly", sqlSelectStmtLen);
									}
									else if (zone_equals_ci(sqlSelectStmt, matcher.start(), matcher.end(), "/*")) {
										do
										{
											if (!matcher.find()) {
												throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly", sqlSelectStmtLen);
											}
											start = matcher.start();
											end = matcher.end();
											if (!zone_equals_ci(sqlSelectStmt, matcher.start(), matcher.end(), "*/")) continue;
											continue __skip_comments_loop;
										} while (true);
									}
									else if (zone_equals_ci(sqlSelectStmt, matcher.start(), matcher.end(), "--")) {
										continue __skip_comments_loop;
									}
									break __skip_comments_loop;
									//"/*"
								} while (true); //end __skip_comments_loop
								if (matcher.end(5) - matcher.start(5) != 4 || !zone_equals_ci(sqlSelectStmt, matcher.start(5), matcher.end(5), JOIN_KWORD)) {
									throw new SQLParseException("parse error - JOIN keyword expected (offset=" + matcher.start() + ")",matcher.start())/*return null*/;
								}
								else if (matcher.start(7) < matcher.end(7)) {
									throw new SQLParseException("parse error - unexpected AS keyword (offset=" + matcher.start(7) + ")",matcher.start(7))/*return null*/;
								}
								else if (matcher.start(6) < matcher.end(6)) {
									throw new SQLParseException("parse error - unexpected token after JOIN keyword (offset=" + matcher.start(6) + ")",matcher.start(6))/*return null*/;
								}
								else if (matcher.start(8) < matcher.end(8)) { //case table name
									System.out.println("joined table spotted (" + matcher.group(8) + ")");
									tableNameMayBecomeTableAlias = true;
									joinsToNestedTable = -1;
								}
								else { //case must be ending with character '(' or followed by qualified table name
									int start_ = skipWs(sqlSelectStmt, matcher.end(5), sqlSelectStmtLen);
									if (start_ >= sqlSelectStmtLen) {
										throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly", sqlSelectStmtLen);
									}
									else if (sqlSelectStmt.charAt(start_) != '(') { //JOIN keyword must be followed by qualified table name
										if (!matcher.find()) {
											throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly", sqlSelectStmtLen);
										}
										else if (matcher.start(5) >= matcher.end(5)/* || matcher.start(6) >= matcher.end(6): KO as the name of the table can be unqualified*/) {
											throw new SQLParseException("parse error - qualified table name expected (offset=" + matcher.start() + ")",matcher.start())/*return null*/;
										}
										else if (matcher.start(7) < matcher.end(7)) {
											throw new SQLParseException("parse error - unexpected AS keyword (offset=" + matcher.start(7) + ")",matcher.start(7))/*return null*/;
										}
										if (matcher.start(6) < matcher.end(6)) {
											joinTblSchema = matcher.group(5);
											tableName = matcher.group(6);
										}
										else {
											tableName = matcher.group(5);
										}
										joinTblAlias = matcher.group(8);
										joinsToNestedTable = -1;
									}
									else { //case JOIN keyword is followed by a nested select table!
										joinsToNestedTable = 1;
									}
								} //end case must be ending with character '(' or followed by qualified table name
							} //end case JOIN keyword is expected
							else { //case not expecting JOIN keyword
								if (joinType < (byte)0) {
									joinType = SQLJoin.INNER_JOIN;
								}
								if (aliasName.isEmpty()) { //case must be ending with character '(' or followed by qualified table name
									if (sqlSelectStmt.charAt(matcher.end() - 1) != '(') { //case JOIN keyword not followed straight away by left parenthesis
										int start_ = skipWs(sqlSelectStmt, matcher.end(), sqlSelectStmtLen);
										if (start_ >= sqlSelectStmtLen) {
											throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly", sqlSelectStmtLen);
										}
										else {
											if (!matcher.find() || matcher.start() != start_) {
												throw new SQLParseException("System in illegal state - system not capturing a valid character (offset=" + start_ + ")", start_);
											}
											if (sqlSelectStmt.charAt(start_) != '(') {
												if (matcher.start(5) >= matcher.end(5)) {
													throw new SQLParseException("parse error - qualified name expected after JOIN keyword (offset=" + start_ + ")", start_);
												}
												else if (matcher.start(7) < matcher.end(7)) {
													throw new SQLParseException("parse error - unexpected AS keyword (offset=" + matcher.start(7) + ")", matcher.start(7));
												}
												if (matcher.start(6) < matcher.end(6)) {
													joinTblSchema = matcher.group(5);
													tableName = matcher.group(6);
												}
												else {
													tableName = matcher.group(5);
												}
												joinTblAlias = matcher.group(8);
												joinsToNestedTable = -1;
											}
											else {
												joinsToNestedTable = 1;
											}
										}
									} //else case JOIN keyword not followed straight away by left parenthesis
									else {
										joinsToNestedTable = 1;
									}
								}
							} //end case not expecting JOIN keyword
							if (joinsToNestedTable != (byte)-1/*no*/) { //case not joining to a nested select table
								//case ON followed by '(' vs case not followed
								if (!ON_KWORD.equals(joinTblAlias)) {
									if (!matcher.find()) {
										throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly", sqlSelectStmtLen);
									}
									else if (matcher.start(5) >= matcher.end(5)) {
										throw new SQLParseException("parse error - keyword ON expected (offset=" + matcher.start() + ")", matcher.start());
									}
									else if (matcher.start(7) < matcher.end(7)) {
										throw new SQLParseException("parse error - unexpected AS keyword (offset=" + matcher.start(7) + ")", matcher.start(7));
									}
									else if (matcher.start(6) < matcher.end(6)) {
										throw new SQLParseException("parse error - ON keyword expected or ON keyword cannot be used as schema name (offset=" + matcher.start(6) + ")", matcher.start(6));
									}
									else if (!zone_equals_ci(sqlSelectStmt, matcher.start(5), matcher.end(5), ON_KWORD)) {
										throw new SQLParseException("parse error - ON keyword expected (offset=" + matcher.start(5) + ")", matcher.start(5));
									}
									else if (matcher.start(8) < matcher.end(8)) {
										tableNameMayBecomeTableAlias = true;
										tableName = matcher.group(8);
									}
									else {
										int start_ = skipWs(sqlSelectStmt, matcher.end(5), sqlSelectStmtLen);
										if (start_ >= sqlSelectStmtLen) {
											throw new SQLParseException("parse error - end of SQL statement string reached unexpectedly", sqlSelectStmtLen);
										}
										else if (sqlSelectStmt.charAt(start_) == '(') {
											
										}
									}
								}
								else {
									joinTblAlias = EMPTY_STR;
								}
							} //end case not joining to a nested select table
							else { 
								
							}
						} //end case of JOIN that is hiding!?
						previousEndedWith/*joinType*/ = aliasName.equalsIgnoreCase(LEFT_KWORD) ? SQLJoin.LEFT_OUTER_JOIN :
											aliasName.equalsIgnoreCase(INNER_KWORD) ? SQLJoin.INNER_JOIN :
											aliasName.equalsIgnoreCase(RIGHT_KWORD) ? SQLJoin.RIGHT_OUTER_JOIN :
											aliasName.equalsIgnoreCase(FULL_KWORD) ? SQLJoin.FULL_OUTER_JOIN : (byte)-1;
						if (previousEndedWith/*joinType*/ > (byte)-1) {
							System.out.println("!!!!!!!!!!!!!!!!!previousEndedWith=" + previousEndedWith + ", matcher.group(8): " + matcher.group(8) + ", aliasName: " + aliasName + ", stack[stackSzM1].class=" + stack[stackSzM1].getClass().getName());
							if (stack[stackSzM1].getType() == SQLWidget.JOIN) {
								SQLColumn col;
								if (mustBeNumberLiteral) {
									col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, EMPTY_STR);
								}
								else if (parametarizedParts != (byte)0) {
									col = new SQLParameterColumn(tableName, columnName, parametarizedParts, EMPTY_STR);
								}
								else {
									String pseudoColName = tableName.isEmpty() ? SQLPseudoColumnNames.checkPseudoColName(columnName) : null;
									SQLTableRef tblRef =  pseudoColName == null && tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentSelect.getTableByAliasExt(tableName);
									col = pseudoColName != null ? new SQLPseudoColumnRef(pseudoColName, EMPTY_STR) :
											tblRef == null ? new SQLRawColumnRef(tableName, columnName, EMPTY_STR) :
															new SQLColumnRef(tblRef, columnName, EMPTY_STR);
								}
								currentJoin.__addJoinColumn(col);
								continue main_loop;
							}
							else if (currentSelect.fromTablesCount == 0) {
								SQLTableRef fromTbl = parametarizedParts != (byte)0 ? new SQLParameterTableRef(tableName/*schema*/, columnName/*tableName*/, parametarizedParts, EMPTY_STR)
															: new SQLStmtTable(tableName/*schema*/, columnName/*tableName*/);
								currentSelect.__addFromTable(fromTbl);
							}
							if (!tableName.isEmpty() || currentSelect == null || currentSelect.fromTablesCount == 0) {
								throw new SQLParseException("parse error - offset: " + matcher.start(), matcher.start())/*return null*/;
							}
							SQLTableRef lastFromTable = currentSelect.fromTables[currentSelect.fromTablesCount - 1];
							if (lastFromTable.isSQLSelectTable()/*lastFromTable instanceof SQLSelectTable*/) {
								lastFromTable.asSQLSelectTable()/*((SQLSelectTable)lastFromTable)*/.name = columnName;
							}
							else {
								currentSelect.fromTables[currentSelect.fromTablesCount - 1] = new SQLStmtTableAlias(columnName, (SQLStmtTable)lastFromTable)/*off*/;
							}
							continue main_loop;
						}
						else if (aliasName.equalsIgnoreCase(OUTER_KWORD) && tableName.isEmpty()/*the name is not qualified*/) {
							previousEndedWith/*joinType*/ = columnName.equalsIgnoreCase(LEFT_KWORD) ? SQLJoin.LEFT_OUTER_JOIN :
												columnName.equalsIgnoreCase(INNER_KWORD) ? SQLJoin.INNER_JOIN :
												columnName.equalsIgnoreCase(RIGHT_KWORD) ? SQLJoin.RIGHT_OUTER_JOIN :
												columnName.equalsIgnoreCase(FULL_KWORD) ? SQLJoin.FULL_OUTER_JOIN : (byte)-1;
							if (previousEndedWith/*joinType*/ > (byte)-1) {
								if (currentSelect == null || currentSelect.fromTablesCount == 0) {
									throw new SQLParseException(matcher.start())/*return null*/;
								}
								continue main_loop;
							}
						}
						if (aliasName.equalsIgnoreCase(AND_KWORD)) {
							//handle as part of current expression after the creation of a column that is not aliased
						}
						else if (aliasName.equalsIgnoreCase(OR_KWORD)) {
							//handle as part of current expression after the creation of a column that is not aliased
						}
						else if (aliasName.equalsIgnoreCase(IN_KWORD)) {
							//handle as part of current expression after the creation of a column that is not aliased
						}
						else if (aliasName.equalsIgnoreCase(DIV_KWORD)) {
							//handle as part of current expression after the creation of a column that is not aliased
						}
						else if (aliasName.equalsIgnoreCase(MOD_KWORD)) {
							//handle as part of current expression after the creation of a column that is not aliased
						}
						else if (aliasName.equalsIgnoreCase(BETWEEN_KWORD)) {
							//handle as part of current expression after the creation of a column that is not aliased
						}
						else if (aliasName.equalsIgnoreCase(IS_KWORD)) {
							//handle as part of current expression after the creation of a column that is not aliased
						}
					}
				}
				else if (!asIndicatorIsPresent && ends_with_left_parenth(sqlSelectStmt, matcher.start(), matcher.end())) { //case its is actually a function expression start!!!
					if (parametarizedParts != (byte)0) throw new SQLParseException("parse error - the name of a function cannot be parametrized (" + matcher.start() + ")", matcher.start())/*return null*/;
//					System.out.println("tableName: " + tableName + ", columnName: " + columnName);
					if (tableName.isEmpty() && JOIN_KWORD.equalsIgnoreCase(columnName)) {
						//System.out.println("JOIN keyword alone like a function");
						if (previousEndedWith < (byte)0) {
							previousEndedWith = SQLJoin.INNER_JOIN;
						}
					}
					//System.out.println("matcher.group(): " + matcher.group() + ", sqlSelectStmt.substring(matcher.start(), matcher.end()): " + sqlSelectStmt.substring(matcher.start(), matcher.end()));
					currentExprCol = new SQLFuncExprColumn(tableName/*ownerSchema*/, columnName/*functionName*/);
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.TABLE_REF:
						if (currentSelect.fromTablesCount > -1) throw new SQLParseException("parse error - " + stack[stackSzM1].getClass().getName() + "-" + matcher.start() + ", currentSelect.fromTablesCount: " + currentSelect.fromTablesCount + " stackSzM1: " + stackSzM1, matcher.start())/*return null*/;
						currentSelect.__addColumn(currentExprCol);
						break;
					case SQLWidget.JOIN:
						currentJoin.__addJoinColumn(currentExprCol);
						break;
					case SQLWidget.WHERE_CLAUSE:
						((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(currentExprCol);
						break;
					case SQLWidget.GROUP_BY_CLAUSE:
						((SQLGroupByClause)stack[stackSzM1]).__addColumn(currentExprCol);
						break;
					case SQLWidget.ORDER_BY_CLAUSE:
						((SQLOrderByClause)stack[stackSzM1]).__addColumn(currentExprCol);
						break;
					case SQLWidget.HAVING_CLAUSE:
						((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(currentExprCol);
						break;
					case SQLWidget.FUNC_EXPR:
					case SQLWidget.CASE_EXPR:
					case SQLWidget.PARENTH_EXPR:
					case SQLWidget.EXPR:
						((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(currentExprCol);
						break;
					default:
						throw new SQLParseException(stack[stackSzM1].getClass().getName() + "-" + matcher.start() + ", stack[stackSzM1].getType(): " + stack[stackSzM1].getType(), matcher.start())/*return null*/;
					}
					stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
					stack[stackSzM1] = currentExprCol;
					continue main_loop;
				}
				else {
					aliasName = EMPTY_STR;
				}
				if (tableName.isEmpty()) {
					SQLColumn col = null;
					if (mustBeNumberLiteral) {
						col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
					}
					else {
						String pseudoColumnName = SQLPseudoColumnNames.checkPseudoColName(columnName/*value*/);
						if (pseudoColumnName != null) {
							col = new SQLPseudoColumnRef(pseudoColumnName, aliasName);
						}
						else {
							byte literalType = SQLLiteral.checkLiteralType(columnName);
							if (literalType > -1 && literalType != SQLLiteral.STRING_LITERAL) {
								col = new SQLLiteral(columnName/*value*/, literalType, aliasName);
							}
						}
					}
					if (col != null) {
						switch (stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
						case SQLWidget.SELECT_STMT_COLUMN:
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
							if (currentSelect.fromTablesCount > -1)  throw new SQLParseException(matcher.start())/*return null*/; //valid table name expected
							currentSelect.__addColumn(col);
							continue main_loop;
						case SQLWidget.WHERE_CLAUSE:
							((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(col);
							continue main_loop;
						case SQLWidget.GROUP_BY_CLAUSE:
							((SQLGroupByClause)stack[stackSzM1]).__addColumn(col);
							continue main_loop;
						case SQLWidget.HAVING_CLAUSE:
							((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(col);
							continue main_loop;
						case SQLWidget.ORDER_BY_CLAUSE:
							((SQLOrderByClause)stack[stackSzM1]).__addColumn(col);
							continue main_loop;
						case SQLWidget.JOIN:
							currentJoin.__addJoinColumn(col);
							continue main_loop;
						case SQLWidget.FUNC_EXPR:
						case SQLWidget.PARENTH_EXPR:
						case SQLWidget.FUNC_ANALYTIC_CLAUSE:
						case SQLWidget.CASE_EXPR:
						case SQLWidget.EXPR:
							((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(col);
							continue main_loop;
						}
					}
				}
				switch (stack[stackSzM1].getType())
				{
				case SQLWidget.SELECT_STATEMENT:
				case SQLWidget.WITH_STATEMENT:
				case SQLWidget.SELECT_STMT_COLUMN:
				case SQLWidget.EXISTS_CONDITION_COLUMN:
				case SQLWidget.IN_SELECT_COLUMN:
					if (currentSelect.fromTablesCount > -1) {//case FROM keyword has already been parsed
//						System.out.println("currentSelect: \r\n" + currentSelect);
						if (currentSelect.whereClause != null ||
							currentSelect.groupByClause != null || currentSelect.orderByClause != null) {
							throw new SQLParseException("parse error (stack[stackSzM1].class=" + stack[stackSzM1].getClass().getName() + ", offset=" + matcher.start() + ")", matcher.start())/*return null*/;
						}
						if (asIndicatorIsPresent) throw new SQLParseException(matcher.start())/*return null*/; //unexpected AS keyword
						if (parametarizedParts != (byte)0) {
							if (aliasName == null) aliasName = EMPTY_STR;
							previousTblOrJoin = new SQLParameterTableRef(tableName/*schema*/, columnName/*tableName*/, parametarizedParts, aliasName);
							currentSelect.__addFromTable(previousTblOrJoin/*fromTbl*/);
						}
						else {
							SQLStmtTable fromTbl = new SQLStmtTable(tableName/*schema*/, columnName/*tableName*/);
							if (aliasName != null && !aliasName.isEmpty()) {
								previousTblOrJoin = new SQLStmtTableAlias(aliasName, fromTbl);
								currentSelect.__addFromTable(previousTblOrJoin);
							}
							else {
								currentSelect.__addFromTable(fromTbl);
								previousTblOrJoin = fromTbl;
							}
						}
					}
					else if (mustBeNumberLiteral) {
						SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
						currentSelect.__addColumn(col);
					}
					else if (parametarizedParts != (byte)0) {
						currentSelect.__addColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, aliasName));
					}
					else {
						//tblREf = tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentSelect.getTableByAliasExt(tableName);
						currentSelect.__addColumn(new SQLRawColumnRef(tableName, columnName, aliasName));
					}
					continue main_loop;
				case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
					if (!(stack[stackSzM1] instanceof SQLSelectTable)) break;
					if (currentSelect.fromTablesCount > -1) { //case FROM keyword has already been parsed
						if (currentSelect.whereClause != null ||
							currentSelect.groupByClause != null || currentSelect.orderByClause != null) throw new SQLParseException(matcher.start())/*return null*/;
						if (asIndicatorIsPresent) throw new SQLParseException(matcher.start())/*return null*/; //unexpected AS keyword
						if (parametarizedParts != (byte)0) {
							if (aliasName == null) aliasName = EMPTY_STR;
							previousTblOrJoin = new SQLParameterTableRef(tableName/*schema*/, columnName/*tableName*/, parametarizedParts, aliasName);
							currentSelect.__addFromTable(previousTblOrJoin/*fromTbl*/);
						}
						else {
							SQLStmtTable fromTbl = new SQLStmtTable(tableName/*schema*/, columnName/*tableName*/);
							if (aliasName != null && !aliasName.isEmpty()) {
								previousTblOrJoin = new SQLStmtTableAlias(aliasName, fromTbl);
								currentSelect.__addFromTable(previousTblOrJoin);
							}
							else {
								currentSelect.__addFromTable(fromTbl);
								previousTblOrJoin = fromTbl;
							}
						}
					}
					else if (mustBeNumberLiteral) {
						SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
						currentSelect.__addColumn(col);
					}
					else if (parametarizedParts != (byte)0) {
						currentSelect.__addColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, aliasName));
					}
					else {
						//tblREf = tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentSelect.getTableByAliasExt(tableName);
						currentSelect.__addColumn(new SQLRawColumnRef(tableName, columnName, aliasName));
					}
					continue;
				case SQLWidget.JOIN:
					if (mustBeNumberLiteral) {
						SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
						currentJoin.__addJoinColumn(col);
					}
					else if (parametarizedParts != (byte)0) {
						currentJoin.__addJoinColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, aliasName));
					}
					else {
						SQLColumn col;
						String pseudoColName = tableName.isEmpty() ? SQLPseudoColumnNames.checkPseudoColName(columnName) : null;
						if (pseudoColName != null) {
							col = new SQLPseudoColumnRef(pseudoColName, aliasName);
						}
						else {
							tblREf = tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentJoin.joinedTbl.getAliasName().equals(tableName) ? currentJoin.joinedTbl : currentSelect.getTableByAliasExt(tableName);
							if (tblREf == null) throw new SQLParseException("parse error - " + tableName + "-" + matcher.start() + " - " + currentSelect.getClass().getName(), matcher.start())/*return null*/; //unknown table alias
							col = new SQLColumnRef(tblREf, columnName, aliasName);
						}
						currentJoin.__addJoinColumn(col);
					}
					continue main_loop;
				case SQLWidget.WHERE_CLAUSE:
					if (mustBeNumberLiteral) {
						SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
						((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(col);
					}
					else if (parametarizedParts != (byte)0) {
						((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, aliasName));
					}
					else {
						SQLColumn col;
						String pseudoColName = tableName.isEmpty() ? SQLPseudoColumnNames.checkPseudoColName(columnName) : null;
						if (pseudoColName != null) {
							col = new SQLPseudoColumnRef(pseudoColName, aliasName);
						}
						else {
							tblREf = tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentSelect.getTableByAliasExt(tableName);
							if (tblREf == null) {
								if (currentSelect.parentSelectStmt != null &&
										(currentSelect.parentSelectStmt.fromTablesCount == 0 || (currentSelect.parentSelectStmt.whereClause == null
											&& currentSelect.parentSelectStmt.groupByClause == null
											&& currentSelect.parentSelectStmt.groupByClause == null))) {
									col = new SQLRawColumnRef(tableName, columnName, aliasName);
								}
								else {
									//System.out.println("*****************currentSelect: \r\n" + currentSelect);
									throw new SQLParseException("parse error - unresolved table (tableName: " + tableName + ", columnName: " + columnName + ")", matcher.start())/*return null*/; //unknown table alias
								}
							}
							else {
								col = new SQLColumnRef(tblREf, columnName, aliasName);
							}
						}
						((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(col);
					}
					continue main_loop;
				case SQLWidget.ORDER_BY_CLAUSE:
					if (mustBeNumberLiteral) {
						SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
						((SQLOrderByClause)stack[stackSzM1]).__addColumn(col);
					}
					else if (parametarizedParts != (byte)0) {
						((SQLOrderByClause)stack[stackSzM1]).__addColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, aliasName));
					}
					else {
						SQLColumn col;
						String pseudoColName = tableName.isEmpty() ? SQLPseudoColumnNames.checkPseudoColName(columnName) : null;
						if (pseudoColName != null) {
							col = new SQLPseudoColumnRef(pseudoColName, aliasName);
						}
						else {
							tblREf = tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentSelect.getTableByAliasExt(tableName);
							if (tblREf == null) throw new SQLParseException(matcher.start())/*return null*/; //unknown table alias
							col = new SQLColumnRef(tblREf, columnName, aliasName);
						}
						((SQLOrderByClause)stack[stackSzM1]).__addColumn(col);
					}
					continue main_loop;
				case SQLWidget.GROUP_BY_CLAUSE:
					if (mustBeNumberLiteral) {
						SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
						((SQLGroupByClause)stack[stackSzM1]).__addColumn(col);
					}
					else if (parametarizedParts != (byte)0) {
						((SQLGroupByClause)stack[stackSzM1]).__addColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, aliasName));
					}
					else {
						SQLColumn col;
						String pseudoColName = tableName.isEmpty() ? SQLPseudoColumnNames.checkPseudoColName(columnName) : null;
						if (pseudoColName != null) {
							col = new SQLPseudoColumnRef(pseudoColName, aliasName);
						}
						else {
							tblREf = tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentSelect.getTableByAliasExt(tableName);
							if (tblREf == null) throw new SQLParseException(matcher.start())/*return null*/; //unknown table alias
							col = new SQLColumnRef(tblREf, columnName, aliasName);
						}
						((SQLGroupByClause)stack[stackSzM1]).__addColumn(col);
					}
					continue main_loop;
				case SQLWidget.HAVING_CLAUSE:
					if (mustBeNumberLiteral) {
						SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
						((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(col);
					}
					else if (parametarizedParts != (byte)0) {
						((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, aliasName));
					}
					else {
						SQLColumn col;
						String pseudoColName = tableName.isEmpty() ? SQLPseudoColumnNames.checkPseudoColName(columnName) : null;
						if (pseudoColName != null) {
							col = new SQLPseudoColumnRef(pseudoColName, aliasName);
						}
						else {
							tblREf = tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentSelect.getTableByAliasExt(tableName);
							if (tblREf == null) throw new SQLParseException(matcher.start())/*return null*/; //unknown table alias
							col = new SQLColumnRef(tblREf, columnName, aliasName);
						}
						((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(col);
					}
					continue main_loop;
				case SQLWidget.FUNC_EXPR:
				case SQLWidget.PARENTH_EXPR:
				case SQLWidget.FUNC_ANALYTIC_CLAUSE:
				case SQLWidget.CASE_EXPR:
				case SQLWidget.EXPR:
					switch(stack[stackSzM1 - 1].getType())
					{
					case SQLWidget.JOIN:
					case SQLWidget.WHERE_CLAUSE:
					case SQLWidget.GROUP_BY_CLAUSE:
					case SQLWidget.HAVING_CLAUSE:
					case SQLWidget.ORDER_BY_CLAUSE:
						if (mustBeNumberLiteral) {
							SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
							((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(col);
						}
						else if (parametarizedParts != (byte)0) {
							((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, aliasName));
						}
						else {
							SQLColumn col;
							String pseudoColName = tableName.isEmpty() ? SQLPseudoColumnNames.checkPseudoColName(columnName) : null;
							if (pseudoColName != null) {
								col = new SQLPseudoColumnRef(pseudoColName, aliasName);
							}
							else {
								tblREf = tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentSelect.getTableByAliasExt(tableName);
								if (tblREf == null) throw new SQLParseException(matcher.start())/*return null*/; //unknown table alias
								col = new SQLColumnRef(tblREf, columnName, aliasName);
							}
							((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(col);
						}
						continue main_loop;
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
					case SQLWidget.TABLE_REF:
						if (currentSelect.fromTablesCount > 0) {
							if (mustBeNumberLiteral) {
								SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
								((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(col);
							}
							else if (parametarizedParts != (byte)0) {
								((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, aliasName));
								continue main_loop;
							}
							else {
								String pseudoColName = tableName.isEmpty() ? SQLPseudoColumnNames.checkPseudoColName(columnName) : null;
								if (pseudoColName != null) {
									SQLColumn col = new SQLPseudoColumnRef(pseudoColName, aliasName);
									((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(col);
									continue main_loop;
								}
								else {
									tblREf = tableName.isEmpty() ? SQLStmtTable.NO_TABLE : currentSelect.getTableByAliasExt(tableName);
									if (tblREf != null) {
										((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(new SQLColumnRef(tblREf, columnName, aliasName));
										continue main_loop;
									}
								}
							}
						}
						break;
					}
					if (mustBeNumberLiteral) {
						SQLColumn col = new SQLLiteral(numberStr/*value*/, decimalLiteral ? SQLLiteral.DECIMAL_LITERAL : SQLLiteral.INTEGER_LITERAL/*literalType*/, aliasName);
						((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(col);
					}
					else if (parametarizedParts != (byte)0) {
						((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(new SQLParameterColumn(tableName, columnName, parametarizedParts, aliasName));
					}
					else {
						((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(new SQLRawColumnRef(tableName, columnName, aliasName));
					}
					continue main_loop;
				default:
				}
			}
			else if ((aliasName = matcher.group(9)) != null) { //case closing parenthesis followed by ON keyword for a join
				//System.out.println("matcher.group(): '" + matcher.group() + "', matcher.group(9): '" + matcher.group(9) + "'" + "', matcher.group(6): '" + matcher.group(6) + "', matcher: " + matcher.toString());
				if (stackSzM1 < 2) throw new SQLParseException(matcher.start())/*return null*/;
				switch(stack[stackSzM1].getType())
				{
				case SQLWidget.WHERE_CLAUSE:
					SQLWhereClause whereClause = (SQLWhereClause)stack[stackSzM1];
					if (whereClause.involvedColumnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
					stack[stackSzM1--] = null;
					break;
				case SQLWidget.GROUP_BY_CLAUSE:
					SQLGroupByClause groupByClause = (SQLGroupByClause)stack[stackSzM1];
					if (groupByClause.columnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
					stack[stackSzM1--] = null;
					break;
				case SQLWidget.ORDER_BY_CLAUSE:
					SQLOrderByClause orderByClause = (SQLOrderByClause)stack[stackSzM1];
					if (orderByClause.columnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
					stack[stackSzM1--] = null;
					break;
				case SQLWidget.HAVING_CLAUSE:
					SQLHavingClause havingClause = (SQLHavingClause)stack[stackSzM1];
					if (havingClause.involvedColumnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
					stack[stackSzM1--] = null;
					break;

				}
				if (stack[stackSzM1].getType() != SQLWidget.TABLE_REF) {
					//test if SQLWidget.SELECT_STATEMENT since 2017-06-28, needed possibly because ensured the select statement also is push to stack in case of nested select statement
					//QUESTION: what if nesting-depth is greater than 2 ==> will see if the case shows up
					//QUESTION: what if combining SQL ==> will handle the case if it shows up
					if (stack[stackSzM1].getType() != SQLWidget.SELECT_STATEMENT) {
						throw new SQLParseException("parse error (stack[stackSzM1].class=" + stack[stackSzM1].getClass().getName() + ", offset=" + matcher.start() + ")", matcher.start())/*return null*/;
					}
					stack[stackSzM1--] = null; //remove the select statement
					if (stackSzM1 < 0 || stack[stackSzM1].getType() != SQLWidget.TABLE_REF) {
						throw new SQLParseException("parse error (stack[stackSzM1].class=" + stack[stackSzM1].getClass().getName() + ", offset=" + matcher.start() + ")", matcher.start())/*return null*/;
					}
				}
				SQLSelectTable nestedSelectTbl = (SQLSelectTable)stack[stackSzM1];
				if (nestedSelectTbl.selectStmt.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
				nestedSelectTbl.__trim();
				stack[stackSzM1--] = null; //remove table from stack
				if (stack[stackSzM1].getType() != SQLWidget.JOIN) throw new SQLParseException(matcher.start())/*return null*/;
				currentJoin = ((SQLJoin)stack[stackSzM1]);
				if (currentJoin.joinedTbl != null) throw new SQLParseException(matcher.start())/*return null*/; //nested joined table already set ==> duplicate closing parantheis followed by ON keyword
				currentJoin.joinedTbl = nestedSelectTbl;
				if (stackSzM1 < 1) throw new SQLParseException(matcher.start())/*return null*/;
				switch(stack[stackSzM1 - 1].getType())
				{
				case SQLWidget.SELECT_STATEMENT:
				case SQLWidget.WITH_STATEMENT:
					currentSelect = (SQLSelectStmt)stack[stackSzM1 - 1];
					break;
				case SQLWidget.SELECT_STMT_COLUMN:
				case SQLWidget.EXISTS_CONDITION_COLUMN:
				case SQLWidget.IN_SELECT_COLUMN:
					currentSelect = ((SQLSelectStmtColumn)stack[stackSzM1 - 1]).selectStmt;
					break;
				case SQLWidget.TABLE_REF:
					currentSelect = ((SQLSelectTable)stack[stackSzM1 - 1]).selectStmt;
					break;
				default:
					 throw new SQLParseException(matcher.start())/*return null*/;
				}
				nestedSelectTbl.name = aliasName;
				//stack[stackSzM1--] = null; //remove join from stack - WAS A BUG
				continue main_loop;
			}
			else if (zone_equals_ci(sqlSelectStmt, start, end, ",")) { //case ouput column end marker reached???
				if (stackSzM1 < 0) throw new SQLParseException("parse error - unexpected/dangling comma (" + matcher.start() + ")", matcher.start())/*return null*/; //select statement cannot start with '('
				switch(stack[stackSzM1].getType())
				{
				case SQLWidget.FUNC_EXPR: //case comma is actually a function list argument separator
				case SQLWidget.PARENTH_EXPR:  //case list of arguments for operator IN
				case SQLWidget.FUNC_ANALYTIC_CLAUSE:
					continue main_loop;
				case SQLWidget.SELECT_STATEMENT:
				case SQLWidget.WITH_STATEMENT:
				case SQLWidget.SELECT_STMT_COLUMN:
				case SQLWidget.IN_SELECT_COLUMN:
				case SQLWidget.EXISTS_CONDITION_COLUMN:
				case SQLWidget.TABLE_REF:
					currentSelect.outputColumnsCount++;
					continue main_loop;
				case SQLWidget.GROUP_BY_CLAUSE:
				case SQLWidget.ORDER_BY_CLAUSE:
					continue main_loop;
				default:
					throw new SQLParseException(stack[stackSzM1].getClass().getName() + "-offset: " + matcher.start() + ", stack[stackSzM1].getType(): " + SQLWidget.getTypeCode(stack[stackSzM1].getType()), matcher.start())/*return null*/;
				}
			}
			else {
				grp = matcher.group();
//				if (grp.indexOf(")") > -1) {
//					System.out.println("CONTAINS CLOSING PARENTH");
//				}
				//int start = matcher.start();
				//int end = matcher.end();
				start = skipWs(sqlSelectStmt, start, end);
				char ch = sqlSelectStmt.charAt(start);
				if (ch == '(') {
					if (stackSzM1 < 0 && withStmtStatus != WITH_STMT_NEXT_STARTED) {
						throw new SQLParseException("parse error - unexpected/orphan/dangling nested select statement (" + matcher.start() +  ")", matcher.start())/*return null*/; //select statement cannot start with '('
					}
					start = skipWs(sqlSelectStmt, ++start, end);
					if (start < end) { //case nested select table
//						System.out.println("stack[stackSzM1].class: " + stack[stackSzM1].getClass().getName());
						if (!zone_equals_ci(sqlSelectStmt, start, end, SELECT_KWORD)) throw new SQLParseException(matcher.start())/*return null*/; //'(SELECT' is expected
						if (operatorType > (byte)-1) {
//							System.out.println("OPERATOR_TYPE IS GREATER THAN MINUSONE");
							if (stack[stackSzM1].getType() != SQLWidget.SET_STATEMENT) {
								throw new SQLParseException("parse error - the current statement must be a combining statement for an operator to be applied (" + matcher.start() +  ")", matcher.start())/*return null*/; //select statement cannot start with '('
							}
							selectStmt = new SQLCombiningSelectStmtItem(operatorType);
							operatorType = -1; //reset to avoid using wrong operator type next time
							SQLSelectTable stmtTbl = new SQLSelectTable(selectStmt);
							stmtTbl.parentSelectStmt = currentSelect; //OLD KO: currentSelect != null ? currentSelect.parentSelectStmt : null;
							((SQLCombiningStatement)stack[stackSzM1]).__add(selectStmt);
							currentSelect = selectStmt;
							stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
							stack[stackSzM1] = stmtTbl;
							stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
							stack[stackSzM1] = selectStmt;
							selectStmt.fromTablesCount = -1;
							continue main_loop;
						}
						if (stack[stackSzM1].getType() == SQLWidget.TABLE_REF) { //BUG-FIX-2017-06-19 - was not handling the case for combining select table
							SQLTableRef tblRef = stack[stackSzM1].asSQLTableRef();
							if (!tblRef.isSQLCombiningSelectTable()) {
								throw new SQLParseException("parse error - combining SQL operator expected before SELECT statement (offset=" + matcher.start() + ")", matcher.start());
							}
							SQLCombiningSelectTable combininingSelectTbl = tblRef.asSQLCombiningSelectTable();
							if (combininingSelectTbl.selectStmt.itemsCount != 0) {
								if (operatorType < 0) {
									throw new SQLParseException("parse error - combining SQL operator expected before SELECT statement (offset=" + matcher.start() + ")", matcher.start());
								}
								selectStmt = new SQLCombiningSelectStmtItem(operatorType);
								operatorType = -1; //reset to avoid using wrong operator type next time
							}
							else {
								selectStmt = new SQLSelectStmt();
							}
						}
						else {
							selectStmt = new SQLSelectStmt();
						}
						start = skipWs(sqlSelectStmt, start + 6, end);
						if (start < end) {
							if (sqlSelectStmt.charAt(start) == '$') { //$$Hint
								start = moveToNextWs(sqlSelectStmt, start + 6, end);
								if (start < end) {
									start = skipWs(sqlSelectStmt, start, end);
								}
							}
						}
						if (start < end) {
							selectStmt.selectDistinct = zone_equals_ci(sqlSelectStmt, start, end, DISTINCT_KWORD);
						}
						selectStmt.fromTablesCount = -1; //indicates that keyword 'FROM' is not yet reached
						SQLWidget widget = null;
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
						case SQLWidget.SELECT_STMT_COLUMN:
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
							if (currentSelect.fromTablesCount < 0) {
								SQLSelectStmtColumn stmtCol = new SQLSelectStmtColumn(selectStmt);
								selectStmt.parentSelectStmt = currentSelect;
								currentSelect.__addColumn(stmtCol);
								widget = stmtCol;
							}
							else {
//								System.out.println("MKA!!!");
								SQLSelectTable stmtTbl = new SQLSelectTable(selectStmt);
								stmtTbl.parentSelectStmt = currentSelect; //OLD KO: currentSelect != null ? currentSelect.parentSelectStmt : null;
								selectStmt.fromTablesCount = -1;
								currentSelect.__addFromTable(stmtTbl);
								//widget = stmtTbl;
								//BUG-FIX-2017-06-20 - was not pushing selectStmt to stack, below
								currentSelect = selectStmt;
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = stmtTbl;
								stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
								stack[stackSzM1] = selectStmt;
								continue main_loop;
							}
							break ;
						case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
							SQLTableRef tblRef = stack[stackSzM1].asSQLTableRef();
							if (!tblRef.isSQLSelectTable()) {
								if (tblRef.isSQLCombiningSelectTable()) {
									//System.out.println("GABI, GABI... RAPHA, RAPHA, ...");
									SQLCombiningSelectTable combiningSelectTbl = tblRef.asSQLCombiningSelectTable();
									if (combiningSelectTbl.withinBrackets) {
										SQLCombiningStatement combiningSelect = new SQLCombiningStatement();
										combiningSelect.__add(selectStmt);
										SQLCombiningSelectTable nestedCombiningSelectTbl = new SQLCombiningSelectTable(combiningSelect, true/*withinBrackets*/);
										nestedCombiningSelectTbl.parentSelectStmt = combiningSelectTbl.parentSelectStmt;
										currentSelect = selectStmt;
										stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
										stack[stackSzM1] = nestedCombiningSelectTbl;
										stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
										stack[stackSzM1] = selectStmt;
										combiningSelectTbl.selectStmt.__add(combiningSelect);
										continue main_loop;
									}
									throw new SQLParseException("parse error - combining SQL operator expected before SELECT statement (offset=" + matcher.start() + ")", matcher.start());
								}
								break;
							}
							if (currentSelect.fromTablesCount < 0) {
								SQLSelectStmtColumn stmtCol = new SQLSelectStmtColumn(selectStmt);
								selectStmt.parentSelectStmt = currentSelect; //bug-fix - 2017-04-05 - was missing
								currentSelect.__addColumn(stmtCol);
								widget = stmtCol;
							}
							else {
								SQLSelectTable stmtTbl = new SQLSelectTable(selectStmt);
								stmtTbl.parentSelectStmt = currentSelect; //OLD KO: currentSelect != null ? currentSelect.parentSelectStmt : null;
								selectStmt.fromTablesCount = -1;
								currentSelect.__addFromTable(stmtTbl);
								widget = stmtTbl;
							}
							break ;
						case SQLWidget.JOIN:
							SQLSelectStmtColumn stmtCol = new SQLSelectStmtColumn(selectStmt);
							selectStmt.parentSelectStmt = currentSelect; //bug-fix - 2017-04-05 - was missing
							currentJoin.__addJoinColumn(stmtCol);
							widget = stmtCol;
							break ;
						case SQLWidget.WHERE_CLAUSE:
							stmtCol = new SQLSelectStmtColumn(selectStmt);
							selectStmt.parentSelectStmt = currentSelect; //bug-fix - 2017-04-05 - was missing
							((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(stmtCol);
							widget = stmtCol;
							break ;
						case SQLWidget.HAVING_CLAUSE:
							stmtCol = new SQLSelectStmtColumn(selectStmt);
							selectStmt.parentSelectStmt = currentSelect; //bug-fix - 2017-04-05 - was missing
							((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(stmtCol);
							widget = stmtCol;
							break ;
						case SQLWidget.GROUP_BY_CLAUSE:
							stmtCol = new SQLSelectStmtColumn(selectStmt);
							selectStmt.parentSelectStmt = currentSelect; //bug-fix - 2017-04-05 - was missing
							((SQLGroupByClause)stack[stackSzM1]).__addColumn(stmtCol);
							widget = stmtCol;
							break ;
						case SQLWidget.ORDER_BY_CLAUSE:
							stmtCol = new SQLSelectStmtColumn(selectStmt);
							selectStmt.parentSelectStmt = currentSelect; //bug-fix - 2017-04-05 - was missing
							((SQLOrderByClause)stack[stackSzM1]).__addColumn(stmtCol);
							widget = stmtCol;
							break ;
						case SQLWidget.CASE_EXPR:
						case SQLWidget.FUNC_EXPR:
						case SQLWidget.PARENTH_EXPR:
						case SQLWidget.EXPR:
							//bug-fix - 2017-04-05 - these cases were not handled
							stmtCol = new SQLSelectStmtColumn(selectStmt);
							selectStmt.parentSelectStmt = currentSelect;
							((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(stmtCol);
							widget = stmtCol;
							break ;
						}
						currentSelect = selectStmt;
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = widget;

						//BUG-FIX-2017-06-28 - was not pushing the created select statement to the stack!!!
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = selectStmt;

						continue main_loop;
					} //end case nested select table
					else {
						if (stack[stackSzM1].getType() == SQLWidget.JOIN) {
							if (((SQLJoin)stack[stackSzM1]).joinColumnsCount == 0) { //case '(' is that of the current join that just follows JOIN keyword
								continue main_loop;
							}
						}
						//System.out.println("start marker of parenthesis operator reached - offset: " + matcher.start() + ", matcher.group(): " + matcher.group() + ", stack[stackSzM1].class: " + stack[stackSzM1].getClass().getName() + ", currentSelect.fromTablesCount: " + currentSelect.fromTablesCount);
						SQLParenthExprColumn col; // = new SQLParenthExprColumn();
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
							if (end < sqlSelectStmtLen) { //since 2017-06-19 - fix for sqls for extracting UOMs
								start = skipWs(sqlSelectStmt, end/*start*/, sqlSelectStmtLen);
								if (start < sqlSelectStmtLen && sqlSelectStmt.charAt(start) == '(') {
									SQLCombiningSelectTable selectTbl = new SQLCombiningSelectTable(true/*withinBrackets*/);
									currentSelect.__addFromTable(selectTbl);
									stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
									stack[stackSzM1] = selectTbl;
									continue main_loop;
								}
							}
							if (currentSelect.fromTablesCount > -1) {
								throw new SQLParseException(matcher.start())/*return null*/; //')' is an invalid character for a name
							}
							col = new SQLParenthExprColumn();
							currentSelect.__addColumn(col);
							break;
						case SQLWidget.JOIN:
							col = new SQLParenthExprColumn();
							currentJoin.__addJoinColumn(col);
							break;
						case SQLWidget.WHERE_CLAUSE:
							col = new SQLParenthExprColumn();
							((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(col);
							break;
						case SQLWidget.HAVING_CLAUSE:
							col = new SQLParenthExprColumn();
							((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(col);
							break;
						case SQLWidget.ORDER_BY_CLAUSE:
							col = new SQLParenthExprColumn();
							((SQLOrderByClause)stack[stackSzM1]).__addColumn(col);
							break;
						case SQLWidget.GROUP_BY_CLAUSE:
							col = new SQLParenthExprColumn();
							((SQLGroupByClause)stack[stackSzM1]).__addColumn(col);
							break;
						case SQLWidget.CASE_EXPR:
						case SQLWidget.PARENTH_EXPR:
						case SQLWidget.FUNC_ANALYTIC_CLAUSE:
						case SQLWidget.FUNC_EXPR:
						case SQLWidget.EXPR:
							col = new SQLParenthExprColumn();
							((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(col);
							break;
						default:
							col = new SQLParenthExprColumn();
						}
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = col;
						currentExprCol = col;
						continue main_loop;
					}
				}
				else if (ch == ')') {
					if (stackSzM1 < 0) throw new SQLParseException("parse error: dangling right parenthesis (" + matcher.start() + ")", matcher.start())/*return null*/;
//					System.out.println("closed expression - class: " + stack[stackSzM1].getClass().getName() + ", offset: " + matcher.start() + "removed: \r\n" + stack[stackSzM1]);
					prevMatchIsClosingParenth = true;
//					System.out.println("stack[stackSzM1].getType(): " + SQLWidget.getTypeCode(stack[stackSzM1].getType()));

					if (stack[stackSzM1].getType() == SQLWidget.SELECT_STATEMENT) {//NOTE-2017-06-28: needed to add this because the select statement is now also pushed to the stack in case of nested select

//						System.out.println("HIT THE POINT");

						//NOTE-2017-06-28: needed to add this because the select statement is now also pushed to the stack in case of nested select
						if (stackSzM1 < 1) throw new SQLParseException("parse error: dangling right parenthesis (" + matcher.start() + ")", matcher.start())/*return null*/;
						switch(stack[stackSzM1 - 1].getType())
						{
						case SQLWidget.SELECT_STMT_COLUMN:
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.TABLE_REF: //nested select table
							stack[stackSzM1].__trim(); //trim select statement
							stack[stackSzM1--] = null; //remove select from stack
							break ;
						}
					}

					__outer_switch:
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.FUNC_EXPR:
					case SQLWidget.PARENTH_EXPR:
					case SQLWidget.FUNC_ANALYTIC_CLAUSE:
						SQLColumn col = (SQLColumn)stack[stackSzM1];
						if (col.getType() == SQLWidget.FUNC_EXPR) {
							maybeAnalyticFuncExpr = (SQLFuncExprColumn)col; //just in case the function is followed by an analytic clause
							maybeAnalyticFuncExpr.parentWidget = stack[stackSzM1 - 1];
						}
						col.__trim();
						prevClosingParenthForCol = col;
//						System.out.println("removing from stack - stack[stackSzM1].getType()): " + SQLWidget.getTypeCode(stack[stackSzM1].getType()));
						stack[stackSzM1--] = null; //remove select statement column, function expression, parenthesis or analytical clause from stack
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
							currentSelect = ((SQLSelectStmt)stack[stackSzM1]);
							if (currentSelect.fromTablesCount > -1) throw new SQLParseException(matcher.start())/*return null*/; //select-column not expected after FROM keywork
							//currentSelect.__addColumn(col); //commented out because added upon handling of opening parenthesis counterpart
							break ;
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.SELECT_STMT_COLUMN:
							currentSelect = ((SQLSelectStmtColumn)stack[stackSzM1]).selectStmt;
							if (currentSelect.fromTablesCount > -1) throw new SQLParseException(matcher.start())/*return null*/; //select-column not expected after FROM keywork
							//currentSelect.__addColumn(col); //commented out because added upon handling of opening parenthesis counterpart
							break ;
						case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
							currentSelect = ((SQLSelectTable)stack[stackSzM1]).selectStmt;
							if (currentSelect.fromTablesCount > -1) throw new SQLParseException(matcher.start())/*return null*/; //select-column not expected after FROM keywork
							//currentSelect.__addColumn(col); //commented out because added upon handling of opening parenthesis counterpart
							break ;
						case SQLWidget.JOIN:
							currentJoin = (SQLJoin)stack[stackSzM1];
							previousTblOrJoin = currentJoin;
							//currentJoin.__addJoinColumn(col); //commented out because added upon handling of opening parenthesis counterpart
							break ;
						case SQLWidget.CASE_EXPR:
						case SQLWidget.FUNC_EXPR:
						case SQLWidget.PARENTH_EXPR:
						case SQLWidget.FUNC_ANALYTIC_CLAUSE:
						case SQLWidget.EXPR:
							currentExprCol = (SQLExprColumn)stack[stackSzM1];
							//currentExprCol.__addInvolvedColumn(col); //
							break ;
						/* COMMENTED OUT BECAUSE the column is added upon the handling of the counterpart opening parenthessis
						case SQLWidget.WHERE_CLAUSE:
							((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(col);
							break ;
						case SQLWidget.GROUP_BY_CLAUSE:
							((SQLGroupByClause)stack[stackSzM1]).__addColumn(col);
							break ;
						case SQLWidget.ORDER_BY_CLAUSE:
							((SQLOrderByClause)stack[stackSzM1]).__addColumn(col);
							break ;*/
						}
						continue main_loop;
					case SQLWidget.WHERE_CLAUSE:
//						System.out.println("WAS IN WHERE CLAUSE!!!");
						SQLWhereClause whereClause = (SQLWhereClause)stack[stackSzM1];
						if (whereClause.involvedColumnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/; //exmpty where clause
						whereClause.__trim();
						stack[stackSzM1--] = null; //remove where clause
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STMT_COLUMN:
							prevClosingParenthForCol = (SQLSelectStmtColumn)stack[stackSzM1];
							break __outer_switch;
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.WITH_STATEMENT:
							break __outer_switch;
						case SQLWidget.TABLE_REF:
							prevClosingParenthForNestedTbl = stack[stackSzM1].asSQLTableRef().asSQLSelectTbl(); //OLD: (SQLSelectTable)stack[stackSzM1]; //kep track of this to be able to set the tabl alias name
							break __outer_switch;
						case SQLWidget.SELECT_STATEMENT: //since 2017-06-20
							if (stackSzM1 < 1) {
								break ;
							}

							int stackSzM2 = stackSzM1 - 1;
							if (stack[stackSzM2].getType() != SQLWidget.TABLE_REF) {
								if (stack[stackSzM2].getType() == SQLWidget.SELECT_STMT_COLUMN) { //BUG-FIX-2017-06-28 - was not testing if SQLWidget.SELECT_STMT_COLUMN
									System.out.println("//////////////////////////////// - stackSzM1: " + stackSzM1);
									prevClosingParenthForCol = (SQLSelectStmtColumn)stack[stackSzM2];
									stack[stackSzM1].__trim(); //trim last select statement of combining select statement
									stack[stackSzM2].__trim(); //trim simple select table or combining select statement
									stack[stackSzM1] = null; //remove last select statement of combining select statement from stack
									stack[stackSzM2] = null; //remove combining select statement from stack
									stackSzM1 -= 2;
									if (stackSzM1 > -1) {
										switch(stack[stackSzM1].getType())
										{
										case SQLWidget.SELECT_STATEMENT:
										case SQLWidget.WITH_STATEMENT:
											currentSelect = (SQLSelectStmt)stack[stackSzM1];
										}
									}
									continue main_loop;
								}
								break ;
							}
							SQLTableRef tblRef = stack[stackSzM2].asSQLTableRef();
							if (!tblRef.isSQLSelectTbl()) {//BUG-FIX-2017-06-20 - changed to tblRef.isSQLSelectTbl() as can also be simple select table;  it served to get mplt_BC_ORA_SalesInvoiceLinesFact_Brazil to work//OLD: if (!tblRef.isSQLCombiningSelectTable()) {
								break ;
							}
							prevClosingParenthForNestedTbl = tblRef.asSQLSelectTbl();
							stack[stackSzM1].__trim(); //trim last select statement of combining select statement
							stack[stackSzM2].__trim(); //trim simple select table or combining select statement
							stack[stackSzM1] = null; //remove last select statement of combining select statement from stack
							stack[stackSzM2] = null; //remove combining select statement from stack
							stackSzM1 -= 2;
							if (stackSzM1 > -1) {
								switch(stack[stackSzM1].getType())
								{
								case SQLWidget.SELECT_STATEMENT:
								case SQLWidget.WITH_STATEMENT:
									currentSelect = (SQLSelectStmt)stack[stackSzM1];
								}
							}
							continue main_loop;
						}
						throw new SQLParseException("parse error stack[stackSzM1].class(" + stack[stackSzM1].getClass().getName() + ", offset: " + matcher.start()  + ", stack[stackSzM1].getType(): " + stack[stackSzM1].getType() + ")", matcher.start())/*return null*/; //unexpected closing parenthesis - nested select is expected
					case SQLWidget.HAVING_CLAUSE:
						SQLHavingClause havingClause = (SQLHavingClause)stack[stackSzM1];
						if (havingClause.involvedColumnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/; //exmpty where clause
						havingClause.__trim();
						stack[stackSzM1--] = null;
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STMT_COLUMN:
							prevClosingParenthForCol = (SQLSelectStmtColumn)stack[stackSzM1];;
							break __outer_switch;
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.WITH_STATEMENT:
							break __outer_switch;
						case SQLWidget.TABLE_REF:
							prevClosingParenthForNestedTbl = stack[stackSzM1].asSQLTableRef().asSQLSelectTbl(); //OLD: (SQLSelectTable)stack[stackSzM1]; //kep track of this to be able to set the tabl alias name
							break __outer_switch;
						}
						throw new SQLParseException(matcher.start())/*return null*/; //unexpected closing parenthesis - nested select is expected
					case SQLWidget.ORDER_BY_CLAUSE:
						SQLOrderByClause orderByClause = (SQLOrderByClause)stack[stackSzM1];
						if (orderByClause.columnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/; //exmpty order by clause
						orderByClause.__trim();
						stack[stackSzM1--] = null;
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STMT_COLUMN:
							prevClosingParenthForCol = (SQLSelectStmtColumn)stack[stackSzM1];;
							break __outer_switch;
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.WITH_STATEMENT:
							break __outer_switch;
						case SQLWidget.TABLE_REF:
							prevClosingParenthForNestedTbl = stack[stackSzM1].asSQLTableRef().asSQLSelectTbl(); //OLD: (SQLSelectTable)stack[stackSzM1]; //kep track of this to be able to set the tabl alias name
							break __outer_switch;
						}
						throw new SQLParseException(matcher.start())/*return null*/; //unexpected closing parenthesis - nested select is expected
					case SQLWidget.GROUP_BY_CLAUSE:
						SQLGroupByClause groupByClause = (SQLGroupByClause)stack[stackSzM1];
						if (groupByClause.columnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/; //exmpty order by clause
						groupByClause.__trim();
						stack[stackSzM1--] = null;
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STMT_COLUMN:
							prevClosingParenthForCol = (SQLSelectStmtColumn)stack[stackSzM1];;
							break __outer_switch;
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.WITH_STATEMENT:
							break __outer_switch;
						case SQLWidget.TABLE_REF:
							prevClosingParenthForNestedTbl = stack[stackSzM1].asSQLTableRef().asSQLSelectTbl(); //OLD: (SQLSelectTable)stack[stackSzM1]; //kep track of this to be able to set the tabl alias name
							break __outer_switch;
						case SQLWidget.SELECT_STATEMENT:
							//BUG-FIX-2017-06-20 - was not handling this case, which was leading to exception firing in all cases
							if (stackSzM1 < 1) {
								break ;
							}
							int j = stackSzM1 - 1;
							if (stack[j].getType() != SQLWidget.TABLE_REF) {
								break ;
							}
							SQLTableRef tblRf = stack[j].asSQLTableRef();
							if (!tblRf.isSQLSelectTbl()) {
								break ;
							}
							prevClosingParenthForNestedTbl = tblRf.asSQLSelectTbl();
							stack[stackSzM1--] = null; //remove select table from stack!!!
							break __outer_switch;
						}
						throw new SQLParseException("parse error (stack[stackSzM1].class: " + stack[stackSzM1].getClass().getName() + ", offset=" + matcher.start() + ")", matcher.start())/*return null*/; //unexpected closing parenthesis - nested select is expected
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
					//case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.WITH_STATEMENT:
						if (currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
						break ;
					case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
						if (currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
						prevClosingParenthForNestedTbl = stack[stackSzM1].asSQLTableRef().asSQLSelectTbl(); //OLD: (SQLSelectTable)stack[stackSzM1]; //kep track of this to be able to set the tabl alias name
						break;
					case SQLWidget.SELECT_STATEMENT:
						//BUG-FIX-2017-06-20 - was not handling this case, which was leading to exception firing in all cases
						if (stackSzM1 < 1) {
							break ;
						}
						int j = stackSzM1 - 1;
						switch(stack[j].getType())
						{
						case SQLWidget.SELECT_STMT_COLUMN:
							prevClosingParenthForCol = (SQLSelectStmtColumn)stack[j/*stackSzM1*/]; //BUG-FIX-2017-06-28 - was using stackSzM1 which normally points to the select statement itself instead of to its parent; may be it is because select statement as not in all the cases also push to the stack in case of nested select!!!
							stack[stackSzM1--] = null; //BUG-FIX-2017-06-28 - remove select table from stack!! - was not remoing may be because the select statement was pushed to the stack for SELECT_STMT_COLUMN
							break __outer_switch;
						case SQLWidget.TABLE_REF:
							SQLTableRef tblRf = stack[j].asSQLTableRef();
							if (!tblRf.isSQLSelectTbl()) {
								break ;
							}
							prevClosingParenthForNestedTbl = tblRf.asSQLSelectTbl();
							stack[stackSzM1--] = null; //remove select table from stack!!!
							break __outer_switch;
						}
						throw new SQLParseException(matcher.start())/*return null*/; //unexpected closing parenthesis after non nested select statement
					case SQLWidget.JOIN:
						if (currentJoin.joinColumnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
						break ;
					default:
						throw new SQLParseException(stack[stackSzM1].getClass().getName() + "-offset: " + matcher.start(), matcher.start())/*return null*/; //select column expected
					} //end __outer_switch
					SQLWidget widget = stack[stackSzM1];
					widget.__trim();
					stack[stackSzM1--] = null; //remove the nested select from the stack
					if (widget.getType() == SQLWidget.WITH_STATEMENT) {
						withStmtStatus = WITH_STMT_JUST_ENDED;
						continue main_loop;
					}

					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
						currentSelect = (SQLSelectStmt)stack[stackSzM1];
						continue main_loop;
					case SQLWidget.JOIN:
						currentJoin = (SQLJoin)stack[stackSzM1];
						previousTblOrJoin = currentJoin;
						continue main_loop;
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
					case SQLWidget.SELECT_STMT_COLUMN:
						currentSelect = ((SQLSelectStmtColumn)stack[stackSzM1]).selectStmt;
						continue main_loop;
					case SQLWidget.TABLE_REF:
						currentSelect = ((SQLSelectTable)stack[stackSzM1]).selectStmt;
						continue main_loop;
					case SQLWidget.CASE_EXPR:
					case SQLWidget.FUNC_EXPR:
					case SQLWidget.PARENTH_EXPR:
					case SQLWidget.FUNC_ANALYTIC_CLAUSE:
					case SQLWidget.EXPR:
						currentExprCol = (SQLExprColumn)stack[stackSzM1];
						continue main_loop;
					}

					continue main_loop;
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, FROM_KWORD)) {
					if (stackSzM1 < 0) throw new SQLParseException("parse error: orphan FROM keyword found (" + matcher.start() + ")" , matcher.start())/*return null*/;
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
						if (currentSelect.getNumOfFromTables() > -1) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect.fromTablesCount = 0;
						currentSelect.outputColumnsCount++;
						continue main_loop;
					case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
						SQLTableRef tblRef = stack[stackSzM1].asSQLTableRef();
						if (!tblRef.isSQLSelectTable()/*!(stack[stackSzM1] instanceof SQLSelectTable)*/ || currentSelect.getNumOfFromTables() > -1) {
							throw new SQLParseException(matcher.start())/*return null*/;
						}
						currentSelect.fromTablesCount = 0;
						currentSelect.outputColumnsCount++;
						continue main_loop;
					case SQLWidget.CASE_EXPR:
						if (currentSelect == null) {
							throw new SQLParseException(matcher.start())/*return null*/;
						}
						currentSelect.fromTablesCount = 0;
						currentSelect.outputColumnsCount++;
						continue main_loop;
					default:
						throw new SQLParseException(stack[stackSzM1].getClass().getName() + "-" + matcher.start() + ", matcher.group(): " + matcher.group() + ", stackSize: " + (stackSzM1 + 1), matcher.start())/*return null*/;
					}
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, WHERE_KWORD)) {
					if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
//					System.out.println("SPOTTED WHERE CLAUSE START - stack[stackSzM1].class" + stack[stackSzM1].getClass().getName() + ", stackSzM1: " + stackSzM1);
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
						if (currentSelect.whereClause != null || currentSelect.groupByClause != null || currentSelect.orderByClause != null) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect.whereClause = new SQLWhereClause();
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = currentSelect.whereClause;
						continue main_loop;
					case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
						if (!(stack[stackSzM1] instanceof SQLSelectTable) || currentSelect.whereClause != null || currentSelect.groupByClause != null || currentSelect.orderByClause != null) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect.whereClause = new SQLWhereClause();
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = currentSelect.whereClause;
						continue main_loop;
					case SQLWidget.JOIN: //join must be ended by new WHERE clause
						if (currentJoin.joinColumnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
						stack[stackSzM1--] = null;
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
						case SQLWidget.SELECT_STMT_COLUMN:
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
							currentSelect.whereClause = new SQLWhereClause();
							stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
							stack[stackSzM1] = currentSelect.whereClause;
							continue main_loop;
						default:
							throw new SQLParseException(matcher.start())/*return null*/;
						}
					default:
						throw new SQLParseException(matcher.start())/*return null*/;
					}
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, ORDER_KWORD)) {
					//System.out.println("???ORDER BY!!!");
					if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
						if (currentSelect.orderByClause != null) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect.orderByClause = new SQLOrderByClause();
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = currentSelect.orderByClause; //add order by clause to stack
						continue main_loop;
					case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
						if (!(stack[stackSzM1] instanceof SQLSelectTable) || currentSelect.orderByClause != null) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect.orderByClause = new SQLOrderByClause();
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = currentSelect.orderByClause; //add order by clause to stack
						continue main_loop;
					case SQLWidget.JOIN: //join ended by keyword ORDER BY
						switch(stack[stackSzM1 - 1].getType())
						{
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
						case SQLWidget.SELECT_STMT_COLUMN:
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
							currentSelect.orderByClause = new SQLOrderByClause();
							stack[stackSzM1] = currentSelect.orderByClause; //replace join with order by in stack
							continue main_loop;
						case SQLWidget.WHERE_CLAUSE:
						case SQLWidget.HAVING_CLAUSE:
						case SQLWidget.GROUP_BY_CLAUSE:
							currentSelect.orderByClause = new SQLOrderByClause();;
							stack[stackSzM1] = currentSelect.orderByClause; //replace WHERE, HAVING or GROUP BY clause with order by in stack
							continue main_loop;
						default:
							throw new SQLParseException("parse error" + stack[stackSzM1].getClass().getName() + '-' + matcher.start(), matcher.start())/*return null*/;
						}
					case SQLWidget.WHERE_CLAUSE:
					case SQLWidget.GROUP_BY_CLAUSE:
					case SQLWidget.HAVING_CLAUSE:
						currentSelect.orderByClause = new SQLOrderByClause();;
						stack[stackSzM1] = currentSelect.orderByClause; //replace WHERE, HAVING or GROUP BY clause with order by in stack
						continue main_loop;
					default:
						throw new SQLParseException("parse error" + stack[stackSzM1].getClass().getName() + '-' + matcher.start(), matcher.start())/*return null*/;
					}
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, GROUP_KWORD)) {
//					System.out.println("YEAH, GROUP keyword spotted - ");
					if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
//					System.out.println("YEAH, GROUP keyword spotted - stack[stackSzM1].class: " + stack[stackSzM1].getClass().getName());
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
						if (currentSelect.groupByClause != null || currentSelect.orderByClause != null) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect.groupByClause = new SQLGroupByClause();
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = currentSelect.groupByClause;
						continue main_loop;
					case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
						if (!(stack[stackSzM1] instanceof SQLSelectTable) || currentSelect.groupByClause != null || currentSelect.orderByClause != null) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect.groupByClause = new SQLGroupByClause();
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = currentSelect.groupByClause;
						continue main_loop;
					case SQLWidget.JOIN: //join ended by keyword GROUP BY
						switch(stack[stackSzM1 - 1].getType())
						{
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
						case SQLWidget.SELECT_STMT_COLUMN:
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.TABLE_REF: //nested select table - note that the table and not the associated select statement gets added to the stack
							currentSelect.groupByClause = new SQLGroupByClause();
							//stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1); //commented out because the join must be removed before the group by is added ==> replace join with group by in stack
							stack[stackSzM1] = currentSelect.groupByClause;
							continue main_loop;
						default:
							throw new SQLParseException("parse error - " + stack[stackSzM1 - 1].getClass() + "-" + matcher.start(), matcher.start())/*return null*/;
						}
					case SQLWidget.WHERE_CLAUSE:
						currentSelect.groupByClause = new SQLGroupByClause();
						//stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1); //commented out because the join must be removed before the group by is added ==> replace join with group by in stack
						stack[stackSzM1] = currentSelect.groupByClause;
						continue main_loop;
					default:
						throw new SQLParseException(matcher.start())/*return null*/;
					}
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, OVER_KWORD)) { //NOTE: OVER WILL ALWAS SEE AS A COLUMN ALIAS AS PEr THe REGEX
					if (stackSzM1 < 0) throw new SQLParseException("parse error - " + stack[stackSzM1].getClass().getName() + "-" + matcher.start(), matcher.start())/*return null*/;
					//System.out.println("maybeAnalyticFuncExpr: " + maybeAnalyticFuncExpr);
					int end1 = matcher.end();
					if (!matcher.find()) throw new SQLParseException(end1)/*return null*/;
					start = matcher.start();
					if (sqlSelectStmt.charAt(start) != '(') throw new SQLParseException(matcher.start())/*return null*/;
					end = matcher.end();
					start = skipWs(sqlSelectStmt, start + 1, end);
					if (start >= end) throw new SQLParseException(matcher.start())/*return null*/;
					if (!zone_equals_ci(sqlSelectStmt, start, end, PARTITION_KWORD)) throw new SQLParseException(matcher.start())/*return null*/; //'(SELECT' or '(PARTITION' is expected
					if (maybeAnalyticFuncExpr == null || maybeAnalyticFuncExpr.analyticClause != null || maybeAnalyticFuncExpr.parentWidget != stack[stackSzM1]) throw new SQLParseException(matcher.start())/*return null*/; //unexpected analytic clause

					maybeAnalyticFuncExpr.analyticClause = new SQLFunctionAnalyticClause();
					maybeAnalyticFuncExpr.analyticClause.outputNumber = maybeAnalyticFuncExpr.outputNumber;
					maybeAnalyticFuncExpr.analyticClause.localOutputNumber = maybeAnalyticFuncExpr.localOutputNumber;
					stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
					stack[stackSzM1] = maybeAnalyticFuncExpr.analyticClause;
					maybeAnalyticFuncExpr = null;
					continue main_loop;

				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, CASE_KWORD)) {
					if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
					SQLCaseExprColumn caseCol = new SQLCaseExprColumn();
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
					case SQLWidget.TABLE_REF:
						if (currentSelect.fromTablesCount > -1 && currentSelect.whereClause == null
								&& currentSelect.groupByClause == null && currentSelect.orderByClause == null
								&& currentSelect.havingClause == null && currentSelect.limit < 0) {
							throw new SQLParseException(matcher.start())/*return null*/;
						}
						currentSelect.__addColumn(caseCol);
						break;
					case SQLWidget.SET_STATEMENT:
						throw new SQLParseException(matcher.start())/*return null*/;
					case SQLWidget.WHERE_CLAUSE:
						((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(caseCol);
						break;
					case SQLWidget.HAVING_CLAUSE:
						((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(caseCol);
						break;
					case SQLWidget.ORDER_BY_CLAUSE:
						((SQLOrderByClause)stack[stackSzM1]).__addColumn(caseCol);
						break;
					case SQLWidget.GROUP_BY_CLAUSE:
						((SQLGroupByClause)stack[stackSzM1]).__addColumn(caseCol);
						break;
					case SQLWidget.JOIN:
						((SQLJoin)stack[stackSzM1]).__addJoinColumn(caseCol);
						break;
					case SQLWidget.CASE_EXPR:
					case SQLWidget.FUNC_EXPR:
					case SQLWidget.PARENTH_EXPR:
					case SQLWidget.FUNC_ANALYTIC_CLAUSE:
					case SQLWidget.EXPR:
						((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(caseCol);
						break;
					default:
						throw new SQLParseException(matcher.start())/*return null*/;
					}
					stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
					stack[stackSzM1] = caseCol;
					currentExprCol = caseCol;
					continue main_loop;
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, WHEN_KWORD)) {
					if (stackSzM1 < 0 || stack[stackSzM1].getType() != SQLWidget.CASE_EXPR) throw new SQLParseException(matcher.start())/*return null*/;
					SQLCaseExprColumn caseCol = (SQLCaseExprColumn)stack[stackSzM1];
					switch(caseCol.blockType)
					{
					case SQLCaseExprColumn.WHEN:
					case SQLCaseExprColumn.ELSE:
					case SQLCaseExprColumn.END:
						throw new SQLParseException(matcher.start())/*return null*/;
					}
					caseCol.blockType = SQLCaseExprColumn.WHEN;
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, THEN_KWORD)) {
					if (stackSzM1 < 0 || stack[stackSzM1].getType() != SQLWidget.CASE_EXPR) throw new SQLParseException(matcher.start())/*return null*/;
					SQLCaseExprColumn caseCol = (SQLCaseExprColumn)stack[stackSzM1];
					switch(caseCol.blockType)
					{
					case SQLCaseExprColumn.THEN:
					case SQLCaseExprColumn.ELSE:
					case SQLCaseExprColumn.END:
						throw new SQLParseException(matcher.start())/*return null*/;
					}
					caseCol.blockType = SQLCaseExprColumn.THEN;
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, ELSE_KWORD)) {
					if (stackSzM1 < 0 || stack[stackSzM1].getType() != SQLWidget.CASE_EXPR) throw new SQLParseException(matcher.start())/*return null*/;
					SQLCaseExprColumn caseCol = (SQLCaseExprColumn)stack[stackSzM1];
					switch(caseCol.blockType)
					{
					case SQLCaseExprColumn.ELSE:
					case SQLCaseExprColumn.WHEN:
					case SQLCaseExprColumn.END:
						throw new SQLParseException(matcher.start())/*return null*/;
					}
					caseCol.blockType = SQLCaseExprColumn.ELSE;//BUG-FIX-2017-06-28 - was mistakenly setting stage/blokcType to SQLCaseExprColumn.WHEN
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, END_KWORD)) {
//					System.out.println(":::::::::::matcher.group(): " + matcher.group() + ", matcher.start(): " + matcher.start());
					if (stackSzM1 < 0 || stack[stackSzM1].getType() != SQLWidget.CASE_EXPR) throw new SQLParseException(matcher.start())/*return null*/;
					SQLCaseExprColumn caseCol = (SQLCaseExprColumn)stack[stackSzM1];
					switch(caseCol.blockType)
					{
					case SQLCaseExprColumn.END:
					case SQLCaseExprColumn.WHEN:
						throw new SQLParseException(matcher.start())/*return null*/;
					}
					caseCol.blockType = SQLCaseExprColumn.END;
					SQLExprColumn exprCol = currentExprCol;
					currentExprCol = null;
					stack[stackSzM1--] = null; //remove case col from stack
					SQLWidget topWidget = stack[stackSzM1];
					switch (topWidget.getType())
					{
					case SQLWidget.EXPR:
					case SQLWidget.CASE_EXPR:
					case SQLWidget.FUNC_EXPR:
					case SQLWidget.PARENTH_EXPR:
					case SQLWidget.FUNC_ANALYTIC_CLAUSE:
						currentExprCol = (SQLExprColumn)topWidget;
						//currentExprCol.__addInvolvedColumn(caseCol); //commented out because added when CASE keyword is handled
						break ;
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
						if (currentSelect.fromTablesCount > -1) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect = (SQLSelectStmt)topWidget;
						//currentSelect.__addColumn(caseCol); //commented out because added when CASE keyword is handled
						break ;
					case SQLWidget.SET_STATEMENT:
						SQLCombiningStatement setStmt_ = (SQLCombiningStatement)topWidget;
						if (setStmt_.isEmpty()) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect = setStmt_.getLast().asSQLSelectStmt();
						//currentSelect.__addColumn(caseCol); //commented out because added when CASE keyword is handled
						break;
					case SQLWidget.JOIN:
						currentJoin = (SQLJoin)topWidget;
						//currentJoin.__addJoinColumn(caseCol); //commented out because added when CASE keyword is handled
						previousTblOrJoin = currentJoin;
						break ;
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
						currentSelect = ((SQLSelectStmtColumn)topWidget).selectStmt;
						//currentSelect.__addColumn(caseCol); //commented out because added when CASE keyword is handled
						break ;
					case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
						currentSelect = ((SQLSelectTable)topWidget).selectStmt;
						//currentSelect.__addColumn(caseCol); //commented out because added when CASE keyword is handled
						break ;
					}
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, HAVING_KWORD)) {
					if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
						if (currentSelect.havingClause != null || currentSelect.groupByClause == null || currentSelect.limit > -1 || currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect.havingClause = new SQLHavingClause();
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = currentSelect.havingClause;
						continue main_loop;
					case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
						if (!(stack[stackSzM1] instanceof SQLSelectTable) || currentSelect.havingClause != null || currentSelect.groupByClause == null || currentSelect.limit > -1 || currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
						currentSelect.havingClause = new SQLHavingClause();
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = currentSelect.havingClause;
						continue main_loop;
					case SQLWidget.WHERE_CLAUSE:
					case SQLWidget.GROUP_BY_CLAUSE:
						currentSelect.havingClause = new SQLHavingClause();
						//stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1); //commented out to just replace instead of removing WHERE or ORDER BY and then adding the new HAVING to the stack
						stack[stackSzM1] = currentSelect.havingClause;
						continue main_loop;
					default:
						throw new SQLParseException("parse error - " + stack[stackSzM1].getClass().getName() + "-" + matcher.start(), matcher.start())/*return null*/;
					}
				}
				else if ((isExistsStmt = zone_equals_ci(sqlSelectStmt, start, end, EXISTS_KWORD)) || is_in_select(sqlSelectStmt, start, end)/*OLD KO: zone_equals_ci(sqlSelectStmt, start, end, IN_KWORD)*/) {
					if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
//					System.out.println("YEAH, IN KEYWORD: " + (!isExistsStmt));
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.WHERE_CLAUSE:
					case SQLWidget.HAVING_CLAUSE:
						if (currentSelect.limit > -1) throw new SQLParseException(matcher.start())/*return null*/;
						break;
					case SQLWidget.CASE_EXPR:
						if (isExistsStmt) {
							throw new SQLParseException("parse error - unexpected EXISTS keyword within CASE expression (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
						}
						SQLCaseExprColumn caseExpr = (SQLCaseExprColumn)stack[stackSzM1];
						if (caseExpr.blockType != SQLCaseExprColumn.WHEN) {
							throw new SQLParseException("parse error - unexpected IN keyword outside CASE expression WHEN blocks (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
						}
						SQLColumn caseWhenCol = caseExpr.involvedColumns[caseExpr.involvedColumnsCount - 1];
						if (caseWhenCol.aliasName == null || caseWhenCol.aliasName.isEmpty()) {
							caseWhenCol.aliasName = IN_KWORD;
						}
						else if (NOT_KWORD.equals(caseWhenCol.aliasName)) {
							caseWhenCol.aliasName = SQLOperator.get_Operator(ISQLOperators.NOT_IN_OP).symbol;
						}
						continue main_loop;
					default:
						throw new SQLParseException("parse error - where clause or having clause expected (stack[stackSzM1].class: " + stack[stackSzM1].getClass().getName() + ", offset: " + matcher.start() + ")", matcher.start())/*return null*/;
					}
					selectStmt = new SQLSelectStmt();
					start = skipWs(sqlSelectStmt, start + 6, end);
					if (start < end) {
						if (sqlSelectStmt.charAt(start) == '$') { //$$Hint
							start = moveToNextWs(sqlSelectStmt, start + 6, end);
							if (start < end) {
								start = skipWs(sqlSelectStmt, start, end);
							}
						}
					}
					if (start < end) {
						selectStmt.selectDistinct = zone_equals_ci(sqlSelectStmt, start, end, DISTINCT_KWORD);
					}
					selectStmt.fromTablesCount = -1; //indicates that keyword 'FROM' is not yet reached
					SQLSelectStmtColumn nestedSelectStmtCol;
					if (stack[stackSzM1].getType() == SQLWidget.WHERE_CLAUSE) {
						nestedSelectStmtCol = isExistsStmt ? new SQLExistsConditionColumn(selectStmt) : new SQLInSelectColumn(selectStmt);
						((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(nestedSelectStmtCol);
					}
					else {
						nestedSelectStmtCol = isExistsStmt ? new SQLExistsConditionColumn(selectStmt) : new SQLInSelectColumn(selectStmt);
						((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(nestedSelectStmtCol);
					}
					selectStmt/*nestedSelectStmtCol*/.parentSelectStmt = currentSelect;
					stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
					stack[stackSzM1] = nestedSelectStmtCol;
					currentSelect = selectStmt;
					continue main_loop;
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, LIMIT_KWORD)) {
					if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
						if (currentSelect.limit > -1 || currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/; //case duplicate LIMIT keyword or LIMIT keyword before from tables
						break ;
					case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
						if (!(stack[stackSzM1] instanceof SQLSelectTable)) throw new SQLParseException(matcher.start())/*return null*/;
						if (currentSelect.limit > -1 || currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/; //case duplicate LIMIT keyword or LIMIT keyword before from tables
						break;
					case SQLWidget.WHERE_CLAUSE:
					case SQLWidget.HAVING_CLAUSE:
					case SQLWidget.ORDER_BY_CLAUSE:
					case SQLWidget.GROUP_BY_CLAUSE:
					case SQLWidget.JOIN:
						stack[stackSzM1].__trim();
						stack[stackSzM1--] = null; //remove from stack
						break;
					default:
						throw new SQLParseException(matcher.start())/*return null*/;
					}
					start = skipWs(sqlSelectStmt, start + 5, end);
					currentSelect.limit = __parseLimit(sqlSelectStmt, start, end);
					continue main_loop;
				}
				else if (zone_equals_ci(sqlSelectStmt, start, end, OFFSET_KWORD)) {
					if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
					switch(stack[stackSzM1].getType())
					{
					case SQLWidget.SELECT_STATEMENT:
					case SQLWidget.WITH_STATEMENT:
					case SQLWidget.SELECT_STMT_COLUMN:
					case SQLWidget.EXISTS_CONDITION_COLUMN:
					case SQLWidget.IN_SELECT_COLUMN:
						break ;
					case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
						if (!(stack[stackSzM1] instanceof SQLSelectTable)) throw new SQLParseException(matcher.start())/*return null*/;
						break;
					default:
						throw new SQLParseException(matcher.start())/*return null*/;
					}
					if (currentSelect.limit < 0 || currentSelect.offset > -1 || currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/; //case OFFSET before LIMIT keyword or duplicate OFFSET keyword or OFFSET keyword before from tables
					start = skipWs(sqlSelectStmt, start + 5, end);
					currentSelect.offset = __parseLimit(sqlSelectStmt, start, end);
					continue main_loop;
				}
				else {
					start = skipWs(sqlSelectStmt, start, end);
					if (zone_equals_ci(sqlSelectStmt, start, end, SELECT_KWORD)) {
						boolean isCombiningSelectTbl = false;
						if (stackSzM1 > -1) {
							if (stackSzM1 != 0 || stack[stackSzM1].getType() != SQLWidget.SET_STATEMENT) {
								if (stack[stackSzM1].getType() == SQLWidget.TABLE_REF) { //BUG-FIX-2017-06-19 - was not handling the case for combining select table
									SQLTableRef tblRef = stack[stackSzM1].asSQLTableRef(); //GAB
									if (!tblRef.isSQLCombiningSelectTable()) {
										//BUG-FIX-2017-06-20 - was no handling the case for SQLSelectTable becoming SQLCombiningSelectTable (see mplt_BC_ORA_UOMConversionGeneral_IntraClass_SQ_QUAL)
										if (!tblRef.isSQLSelectTable()) {
											throw new SQLParseException("parse error - combining select table or combining select statement is required to nest combined select statement (stack[stackSzM1].class=" + stack[stackSzM1].getClass().getName() + ", offset=" + matcher.start() + ")", matcher.start())/*return null*/; //case nested select stmt not preceeded by '(' character
										}
										SQLSelectTable selectTbl = tblRef.asSQLSelectTable();
										SQLCombiningStatement combiningSelect = new SQLCombiningStatement();
										combiningSelect.__add(selectTbl.selectStmt);
										SQLCombiningSelectTable combiningSelectTbl = new SQLCombiningSelectTable(combiningSelect, true/*withinBrackets*/);
										//substitute SQLCombiningSelectTable for the SQLSelectTable both in stack and in list of from tables or list of select items
										stack[stackSzM1] = combiningSelectTbl;
										tblRef = combiningSelectTbl;
										SQLWidget selectStmtOrCombiningStmt = stack[stackSzM1 - 1];
										SQLSelectStmt parentSelectStmt = selectStmtOrCombiningStmt.getType() == SQLWidget.SET_STATEMENT ? ((SQLCombiningStatement)selectStmtOrCombiningStmt).__parentSelectStmt() : (SQLSelectStmt)selectStmtOrCombiningStmt;
										if (selectStmtOrCombiningStmt.getType() != SQLWidget.SET_STATEMENT) {
											parentSelectStmt.fromTables[parentSelectStmt.fromTablesCount - 1] = combiningSelectTbl;
										}
										else {
											SQLCombiningStatement parentCombiningStmt = (SQLCombiningStatement)selectStmtOrCombiningStmt;
											parentCombiningStmt.items[parentCombiningStmt.itemsCount - 1] = combiningSelect;
										}
									}
									if (operatorType < 0) throw new SQLParseException("parse error - UNION, EXCEPT, MINUS or INTERSECT operator expected prior to the select statement (" + matcher.start() + ")", matcher.start())/*return null*/; //case union or minus or intersect is missiong before SELECT keyword
									selectStmt = new SQLCombiningSelectStmtItem(operatorType);
									tblRef.asSQLCombiningSelectTable().selectStmt.__add(selectStmt);
									selectStmt.parentSelectTbl = tblRef.asSQLCombiningSelectTable();
									isCombiningSelectTbl = true; //since 2017-06-19
								}
								else {
									throw new SQLParseException("parse error - combining select table or combining select statement is required to nest combined select statement (stack[stackSzM1].class" + stack[stackSzM1].getClass().getName() + ", offset=" + matcher.start() + ")", matcher.start())/*return null*/; //case nested select stmt not preceeded by '(' character
								}
							}
							else if (operatorType < 0) {
								throw new SQLParseException("parse error - UNION, EXCEPT, MINUS or INTERSECT operator expected prior to the select statement (" + matcher.start() + ")", matcher.start())/*return null*/; //case union or minus or intersect is missiong before SELECT keyword
							}

						}
						//System.out.println("SELECT!!! - operatorType: " + operatorType);
						if (operatorType < 0) {
							selectStmt = new SQLSelectStmt();
							returnStmt = selectStmt;
						}
						else if (!isCombiningSelectTbl) { //BUG-FIX-2017-06-19 - was not handling the case for combining select table
							selectStmt = new SQLCombiningSelectStmtItem(operatorType);
							setStmt.__add(selectStmt);
						}
						start = skipWs(sqlSelectStmt, start + 6, end);
						if (start < end) {
							if (sqlSelectStmt.charAt(start) == '$') { //$$Hint
								start = moveToNextWs(sqlSelectStmt, start + 6, end);
								if (start < end) {
									start = skipWs(sqlSelectStmt, start, end);
								}
							}
						}
						if (start < end) {
							selectStmt.selectDistinct = zone_equals_ci(sqlSelectStmt, start, end, DISTINCT_KWORD);
						}
						selectStmt.fromTablesCount = -1; //indicates that keyword 'FROM' is not yet reached
						currentSelect = selectStmt;
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = currentSelect;
						if (operatorType > -1) {
							operatorType = -1; //reset for next time
						}
						if (withStmtsCount > 0) {
							selectStmt.withStmts = new SQLWithStmt[withStmtsCount];
							System.arraycopy(withStmts, 0, selectStmt.withStmts, 0, withStmtsCount);
							selectStmt.withStmtsCount = withStmtsCount;
							withStmtsCount = 0;
							withStmts = null;
						}
						continue main_loop;
					} //end if (zone_equals_ci(sqlSelectStmt, start, end, SELECT_KWORD))
					byte newOperatorType = -1;
					if (zone_equals_ci(sqlSelectStmt, start, end, UNION_KWORD)) {
						//System.out.println("UNION_KWORD!!!");
						start = skipWs(sqlSelectStmt, start + 5, end);
						if (start < end) {
							newOperatorType = zone_equals_ci(sqlSelectStmt, start, end, ALL_KWORD) ? SQLSelectStmt.UNION_ALL : SQLSelectStmt.UNION; //OLD KO: operatorType = zone_equals_ci(sqlSelectStmt, start, end, ALL_KWORD) ? SQLSelectStmt.UNION_ALL : SQLSelectStmt.UNION;
						}
						else {
							newOperatorType = SQLSelectStmt.UNION; //OLD KO: operatorType = SQLSelectStmt.UNION;
						}
						//System.out.println("UNION_KWORD!!! - start: " + start + ", end: " + end + ", operatorType: " + operatorType);
						withStmtStatus = NO_WITH_STMT; //reset
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, MINUS_KWORD)) {
						newOperatorType = SQLSelectStmt.MINUS; //OLD KO: operatorType = SQLSelectStmt.MINUS;
						withStmtStatus = NO_WITH_STMT; //reset
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, EXCEPT_KWORD)) {
						newOperatorType = SQLSelectStmt.EXCEPT; //OLD KO: operatorType = SQLSelectStmt.EXCEPT;
						withStmtStatus = NO_WITH_STMT; //reset
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, INTERSECT_KWORD)) {
						newOperatorType = SQLSelectStmt.INTERSECT; //OLD KO: operatorType = SQLSelectStmt.INTERSECT;
						withStmtStatus = NO_WITH_STMT; //reset
					}
					else {
						/*BUG-FIX was wrongly cancelling previous operator
						operatorType = -1;
						*/
						SQLOperator comparisonOp = check_if_comparison_op_symbol(sqlSelectStmt, start, end);
						if (comparisonOp != null) {
							switch(stack[stackSzM1].getType())
							{
							case SQLWidget.WHERE_CLAUSE:
								SQLWhereClause whereClasuse = (SQLWhereClause)stack[stackSzM1];
								if (whereClasuse.involvedColumnsCount > 0) {
									SQLColumn whereCol = whereClasuse.involvedColumns[whereClasuse.involvedColumnsCount - 1];
									if (whereCol.aliasName == null || whereCol.aliasName.isEmpty()) {
										whereCol.aliasName = comparisonOp.symbol;
									}
								}
								continue main_loop;
							case SQLWidget.JOIN:
								SQLJoin join = (SQLJoin)stack[stackSzM1];
								if (join.joinColumnsCount > 0) {
									SQLColumn joinCol = join.joinColumns[join.joinColumnsCount - 1];
									if (joinCol.aliasName == null || joinCol.aliasName.isEmpty()) {
										joinCol.aliasName = comparisonOp.symbol;
									}
								}
								continue main_loop;
							case SQLWidget.CASE_EXPR:
								SQLCaseExprColumn caseExpr = (SQLCaseExprColumn)stack[stackSzM1];
								if (caseExpr.blockType == SQLCaseExprColumn.WHEN && caseExpr.involvedColumnsCount > 0) {
									SQLColumn caseWhenCol = caseExpr.involvedColumns[caseExpr.involvedColumnsCount - 1];
									if (caseWhenCol.aliasName == null || caseWhenCol.aliasName.isEmpty()) {
										caseWhenCol.aliasName = comparisonOp.symbol;
									}
								}
								continue main_loop;
							}
							continue main_loop;
						}
					}
					if (newOperatorType > -1) { //OLD: if (operatorType > -1) {
						if (stackSzM1 < 0) throw new SQLParseException("unexpected UNION, MINUS, EXCEPT or INTERSECT operator (" + matcher.start() + ")", matcher.start())/*return null*/;
						if (operatorType > -1) {
							throw new SQLParseException("parse error - two operator for sets in a row (" + matcher.start() + ")", matcher.start())/*return null*/;
						}
						operatorType = newOperatorType;
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STATEMENT:
							if (currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
							stack[stackSzM1].__trim();
							break;
						case SQLWidget.WITH_STATEMENT:
							throw new SQLParseException("parse error - main select statement expected before operator (" + matcher.start() + ")", matcher.start())/*return null*/; //main select statement expected before operator
						case SQLWidget.SELECT_STMT_COLUMN:
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
							throw new SQLParseException("parse error - closing parenthesis expected (" + matcher.start() + ")", matcher.start())/*return null*/; //cloging parenthesis expected
						case SQLWidget.WHERE_CLAUSE:
							if (((SQLWhereClause)stack[stackSzM1]).involvedColumnsCount < 1 && !currentSelect.withInfaJoins) throw new SQLParseException(matcher.start())/*return null*/;
							stack[stackSzM1].__trim();
							stack[stackSzM1--] = null; //remove from the stack
							stack[stackSzM1].__trim(); //trim the select statement
							break;
						case SQLWidget.GROUP_BY_CLAUSE:
							if (((SQLGroupByClause)stack[stackSzM1]).columnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
							stack[stackSzM1].__trim();
							stack[stackSzM1--] = null; //remove from the stack
							stack[stackSzM1].__trim(); //trim the select statement
							break;
						case SQLWidget.ORDER_BY_CLAUSE:
							if (((SQLOrderByClause)stack[stackSzM1]).columnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
							stack[stackSzM1].__trim();
							stack[stackSzM1--] = null; //remove from the stack
							stack[stackSzM1].__trim(); //trim the select statement
							break;
						case SQLWidget.SET_STATEMENT:
							//NOTE: the select statement items are at creationtime ==> no need to add again, here...
							continue main_loop;
						default:
							throw new SQLParseException("parse error - parentWidgetClass: " + stack[stackSzM1].getClass().getName() + ", offset: " + matcher.start() + ", stackSzM1: " + stackSzM1, matcher.start())/*return null*/;
						}
						if (stackSzM1 == 0) { //case very first UNION, EXCEPT, MINUS or INTERSECT operator reached => replace existing select in the stack with the SQL combining statement
							setStmt = new SQLCombiningStatement();
							setStmt.__add(currentSelect); //add the first select statement
							stack[0] = setStmt;
							returnStmt = setStmt;
						}
						else {
							stack[stackSzM1--] = null; //remove the select statement from the stack
						}
						//System.out.println("::::::operatorType: " + operatorType);
						continue main_loop;
					} //end if (newOperatorType > -1)
					byte joinType = -1;
					if (zone_equals_ci(sqlSelectStmt, start, end, INNER_KWORD)) {
						joinType = SQLJoin.INNER_JOIN;
						int end1 = skipWs(sqlSelectStmt, start + 5, end); //INNER
						if (end1 == start + 5) throw new SQLParseException(matcher.start())/*return null*/;
						start = end1;
						if (!zone_equals_ci(sqlSelectStmt, start, end, JOIN_KWORD)) throw new SQLParseException(matcher.start())/*return null*/;
						end1 = skipWs(sqlSelectStmt, start + 4, end); //JOIN
						if (end1 == start + 4) throw new SQLParseException(matcher.start())/*return null*/;
						start = end1;
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, LEFT_KWORD)) {
						//System.out.println("?????matcher.group(): '" + matcher.group() + "'");
						joinType = SQLJoin.LEFT_OUTER_JOIN;
						start = skipWs(sqlSelectStmt, start + 4, end); //LEFT
						if (start >= end) throw new SQLParseException(matcher.start())/*return null*/;
						if (zone_equals_ci(sqlSelectStmt, start, end, OUTER_KWORD)) { //OLD: if ! throw new SQLParseException(matcher.start())/*return null*/;
							start = skipWs(sqlSelectStmt, start + 5, end); //OUTER
							if (start >= end) throw new SQLParseException(matcher.start())/*return null*/;
						}
						if (!zone_equals_ci(sqlSelectStmt, start, end, JOIN_KWORD)) throw new SQLParseException(matcher.start())/*return null*/;
						start = skipWs(sqlSelectStmt, start + 4, end); //JOIN
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, RIGHT_KWORD)) {
						joinType = SQLJoin.RIGHT_OUTER_JOIN;
						start = skipWs(sqlSelectStmt, start + 5, end); //RIGHT
						if (start >= end) throw new SQLParseException(matcher.start())/*return null*/;
						if (zone_equals_ci(sqlSelectStmt, start, end, OUTER_KWORD)) { //OLD: if ! throw new SQLParseException(matcher.start())/*return null*/;
							start = skipWs(sqlSelectStmt, start + 5, end); //OUTER
							if (start >= end) throw new SQLParseException(matcher.start())/*return null*/;
						}
						if (!zone_equals_ci(sqlSelectStmt, start, end, JOIN_KWORD)) throw new SQLParseException(matcher.start())/*return null*/;
						start = skipWs(sqlSelectStmt, start + 4, end); //JOIN
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, FULL_KWORD)) {
						//System.out.println("matcher.group(): '" + matcher.group() + "'");
						joinType = SQLJoin.FULL_OUTER_JOIN;
						start = skipWs(sqlSelectStmt, start + 4, end); //FULL
						if (start >= end) throw new SQLParseException(matcher.start())/*return null*/;
						if (zone_equals_ci(sqlSelectStmt, start, end, OUTER_KWORD)) { //OLD: if ! throw new SQLParseException(matcher.start())/*return null*/;
							start = skipWs(sqlSelectStmt, start + 5, end); //OUTER
							if (start >= end) throw new SQLParseException(matcher.start())/*return null*/;
						}
						if (!zone_equals_ci(sqlSelectStmt, start, end, JOIN_KWORD)) throw new SQLParseException(matcher.start())/*return null*/;
						start = skipWs(sqlSelectStmt, start + 4, end); //JOIN
					}
					else {
						if (zone_equals_ci(sqlSelectStmt, start, end, OUTER_KWORD)) {
							start = skipWs(sqlSelectStmt, start + 5, end);
						}
						if (zone_equals_ci(sqlSelectStmt, start, end, JOIN_KWORD)) {
							switch(previousEndedWith)
							{
							case SQLJoin.INNER_JOIN:
							case SQLJoin.LEFT_OUTER_JOIN:
							case SQLJoin.RIGHT_OUTER_JOIN:
							case SQLJoin.FULL_OUTER_JOIN:
								joinType = previousEndedWith;
								start = skipWs(sqlSelectStmt, start + 4, end); //JOIN
								if (start >= end) throw new SQLParseException(matcher.start())/*return null*/;
								previousEndedWith = -1;
								break;
							default:
								joinType = SQLJoin.INNER_JOIN;//OLD: throw new SQLParseException(matcher.start())/*return null*/; //join type qualifier missing
								start = skipWs(sqlSelectStmt, start + 4, end); //JOIN
								if (start >= start) throw new SQLParseException(matcher.start())/*return null*/;
								break;
							}
						}
					}
					if (joinType > -1) { //case there's a join with the first table being a select statement ???
						if (sqlSelectStmt.charAt(start) != '(') throw new SQLParseException(matcher.start())/*return null*/;
						start = skipWs(sqlSelectStmt, start + 1, end);
						if (start >= end) throw new SQLParseException(matcher.start())/*return null*/;
						if (!zone_equals_ci(sqlSelectStmt, start, end, SELECT_KWORD)) throw new SQLParseException(matcher.start())/*return null*/;

						__switch_block:
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
						case SQLWidget.SELECT_STMT_COLUMN:
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
							break;
						case SQLWidget.JOIN: //join must be ended by new join
							if (currentJoin.joinColumnsCount < 1) throw new SQLParseException(matcher.start())/*return null*/;
							stack[stackSzM1--] = null;
							switch(stack[stackSzM1].getType())
							{
							case SQLWidget.SELECT_STATEMENT:
							case SQLWidget.WITH_STATEMENT:
							case SQLWidget.SELECT_STMT_COLUMN:
							case SQLWidget.EXISTS_CONDITION_COLUMN:
							case SQLWidget.IN_SELECT_COLUMN:
							case SQLWidget.TABLE_REF: //an instance of SQLSelectTable is expected ==> if not fire exception
								break __switch_block;
							default:
								throw new SQLParseException(matcher.start())/*return null*/;
							}
						default:
							throw new SQLParseException(matcher.start())/*return null*/;
						}

						selectStmt = new SQLSelectStmt();
						SQLSelectTable tbl = new SQLSelectTable(selectStmt);
						tbl.parentSelectStmt = currentSelect; //OLD KO: currentSelect != null ? currentSelect.parentSelectStmt : null;
						currentSelect.__addFromTable(tbl);
						selectStmt.fromTablesCount = -1;
						currentJoin = new SQLJoin(joinType);
						currentSelect.__addJoin(currentJoin);
						currentJoin.tbl = previousTblOrJoin;
						currentJoin.joinedTbl = null; //ensure it is set to null for it to be set when the closing
						previousTblOrJoin = tbl;
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = currentJoin;
						currentSelect = selectStmt;
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = tbl;
						//BUG-FIX-2017-06-28 - was not pushing the select statement to the stack!!!
						stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
						stack[stackSzM1] = selectStmt;
						continue main_loop;
					}
					if (zone_equals_ci(sqlSelectStmt, start, end, "/*")) {
						//System.out.println("START-OF-MULTILINE-COMMENT - operatorType: " + operatorType);
						do
						{
							if (!matcher.find()) throw new SQLParseException(sqlSelectStmt.length())/*return null*/;
							start = matcher.start();
							end = matcher.end();
							if (!zone_equals_ci(sqlSelectStmt, start, end, "*/")) continue;
							continue main_loop;
						} while (true);
					}
					else if (sqlSelectStmt.charAt(start) == '\'') {
						start++;
						end--;
						for (;end>=start;end--)
						{
							if (sqlSelectStmt.charAt(end) == '\'') {
								break ;
							}
						}
						String strVal = __getRefString(sqlSelectStmt.substring(start, end), allStrings);
						curStringLiteral/*SQLLiteral constCol*/ = new SQLLiteral(strVal, EMPTY_STR/*aliasName*/);
						nextMayBeStringLiteralAlias = currentSelect.fromTablesCount < 1;
						//System.out.println("?????currentSelect.fromTablesCount: " + currentSelect.fromTablesCount);
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.EXPR:
						case SQLWidget.FUNC_EXPR:
						case SQLWidget.CASE_EXPR:
						case SQLWidget.PARENTH_EXPR:
						case SQLWidget.FUNC_ANALYTIC_CLAUSE:
							((SQLExprColumn)stack[stackSzM1]).__addInvolvedColumn(curStringLiteral);
							continue main_loop;
						case SQLWidget.SELECT_STATEMENT:
						case SQLWidget.WITH_STATEMENT:
						case SQLWidget.EXISTS_CONDITION_COLUMN:
						case SQLWidget.IN_SELECT_COLUMN:
						case SQLWidget.SELECT_STMT_COLUMN:
						case SQLWidget.TABLE_REF:
							if (currentSelect.fromTablesCount > -1) throw new SQLParseException(matcher.start())/*return null*/;
							currentSelect.__addColumn(curStringLiteral);
							continue main_loop;
						case SQLWidget.JOIN:
							currentJoin.__addJoinColumn(curStringLiteral);
							continue main_loop;
						case SQLWidget.WHERE_CLAUSE:
							((SQLWhereClause)stack[stackSzM1]).__addInvolvedColumn(curStringLiteral);
							continue main_loop;
						case SQLWidget.GROUP_BY_CLAUSE:
							((SQLGroupByClause)stack[stackSzM1]).__addColumn(curStringLiteral);
							continue main_loop;
						case SQLWidget.ORDER_BY_CLAUSE:
							((SQLOrderByClause)stack[stackSzM1]).__addColumn(curStringLiteral);
							continue main_loop;
						case SQLWidget.HAVING_CLAUSE:
							((SQLHavingClause)stack[stackSzM1]).__addInvolvedColumn(curStringLiteral);
							continue main_loop;
						default:
							throw new SQLParseException(matcher.start())/*return null*/;
						}
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, "{")) {
						if (stackSzM1 < 0) throw new SQLParseException(matcher.start())/*return null*/;
						if (stack[stackSzM1].getType() == SQLWidget.WHERE_CLAUSE && currentSelect.joinsCount < 1) {
							SQLWhereClause whereClause = (SQLWhereClause)stack[stackSzM1];
							if (whereClause.involvedColumnsCount > 0) throw new SQLParseException(matcher.start())/*return null*/;
							start = skipWs(sqlSelectStmt, start + 1, end);
							if (start >= end) throw new SQLParseException(matcher.start())/*return null*/;
							end--;
							while (is_ws_char(sqlSelectStmt.charAt(end)))
							{
								end--;
								if (end < start) break;
							}
							end++;
							String tableAlias = sqlSelectStmt.substring(start, end);
							previousTblOrJoin = currentSelect.getTableByAliasExt(tableAlias);
							if (previousTblOrJoin == null) {
								throw new SQLParseException("Unresolved table alias - " + matcher.start(), matcher.start())/*return null*/;
							}
							stack = ensureCanAddOneMoreStackElt(stack, ++stackSzM1);
							stack[stackSzM1] = new SQLInfaJoins();
							continue main_loop;
						}
						throw new SQLParseException(matcher.start())/*return null*/;
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, "}")) {
						if (stackSzM1 < 1) throw new SQLParseException(matcher.start())/*return null*/;
						stack[stackSzM1--] = null; //remove last join from stack
						if (stack[stackSzM1].getType() != SQLWidget.SQL_INFA_JOINS) throw new SQLParseException(matcher.start())/*return null*/;
						SQLInfaJoins infaJoins = ((SQLInfaJoins)stack[stackSzM1]);
						infaJoins.__trim();
						currentSelect.joins = infaJoins.joins;
						currentSelect.joinsCount = infaJoins.joinsCount;
						stack[stackSzM1--] = null;
						continue main_loop;
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, AND_KWORD)) {
						//handle as part of current expression
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, OR_KWORD)) {
						//handle as part of current expression
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, IN_KWORD)) {
						//handle as part of current expression
//						System.out.println("INNNNNNNNNNNNNNNNNNNNNN!!!");
						if (stackSzM1 < 0) {
							throw new SQLParseException("parse error - orphan IN keyword (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
						}
						switch(stack[stackSzM1].getType())
						{
						case SQLWidget.WHERE_CLAUSE:
							SQLWhereClause whereClause = (SQLWhereClause)stack[stackSzM1];
							if (whereClause.involvedColumnsCount > 0) {
								SQLColumn col = whereClause.involvedColumns[whereClause.involvedColumnsCount - 1];
								if (col.aliasName == null || col.aliasName.isEmpty()) {
									col.aliasName = IN_KWORD;
									continue main_loop;
								}
								else if (col.aliasName.equalsIgnoreCase(NOT_KWORD)) {
									col.aliasName = SQLOperator.get_Operator(ISQLOperators.NOT_IN_OP).symbol;
									continue main_loop;
								}
							}
							//NOTE: should never get here... merely ignore the keyword
							continue main_loop; //break ;
						case SQLWidget.HAVING_CLAUSE:
							SQLHavingClause havingClause = (SQLHavingClause)stack[stackSzM1];
							if (havingClause.involvedColumnsCount > 0) {
								SQLColumn col = havingClause.involvedColumns[havingClause.involvedColumnsCount - 1];
								if (col.aliasName == null || col.aliasName.isEmpty()) {
									col.aliasName = IN_KWORD;
									continue main_loop;
								}
								else if (col.aliasName.equalsIgnoreCase(NOT_KWORD)) {
									col.aliasName = SQLOperator.get_Operator(ISQLOperators.NOT_IN_OP).symbol;
									continue main_loop;
								}
							}
							//NOTE: should never get here... merely ignore the keyword
							continue main_loop; //break ;
						case SQLWidget.JOIN:
							SQLJoin join = (SQLJoin)stack[stackSzM1];
							if (join.joinColumnsCount > 0) {
								SQLColumn col = join.joinColumns[join.joinColumnsCount - 1];
								if (col.aliasName == null || col.aliasName.isEmpty()) {
									col.aliasName = IN_KWORD;
									continue main_loop;
								}
								else if (col.aliasName.equalsIgnoreCase(NOT_KWORD)) {
									col.aliasName = SQLOperator.get_Operator(ISQLOperators.NOT_IN_OP).symbol;
									continue main_loop;
								}
							}
							//NOTE: should never get here... merely ignore the keyword
							continue main_loop; //break ;
						case SQLWidget.CASE_EXPR:
							SQLCaseExprColumn caseExpr = (SQLCaseExprColumn)stack[stackSzM1];
							if (caseExpr.involvedColumnsCount > 0) {
								SQLColumn col = caseExpr.involvedColumns[caseExpr.involvedColumnsCount - 1];
								if (col.aliasName == null || col.aliasName.isEmpty()) {
									col.aliasName = IN_KWORD;
									continue main_loop;
								}
								else if (col.aliasName.equalsIgnoreCase(NOT_KWORD)) {
									col.aliasName = SQLOperator.get_Operator(ISQLOperators.NOT_IN_OP).symbol;
									continue main_loop;
								}
							}
							//NOTE: should never get here... merely ignore the keyword
							continue main_loop; //break ;
						}
						throw new SQLParseException("parse error - unexpected IN keyword (offset=" + matcher.start() + ")", matcher.start())/*return null*/;
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, DIV_KWORD)) {
						//handle as part of current expression
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, MOD_KWORD)) {
						//handle as part of current expression
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, BETWEEN_KWORD)) {
						//handle as part of current expression
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, NOT_KWORD)) {
						//handle as part of current expression
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, IS_KWORD)) {
						//handle as part of current expression
					}
					else if (zone_equals_ci(sqlSelectStmt, start, end, "--")) {
						//System.out.println("SQLSelectStmtParseUtil::::start of inline comment - operatorType: " + operatorType);
						continue main_loop;
					}
				}
			}
		} //while (matcher.find());
		if (returnStmt == null) throw new SQLParseException("parse error - empty SQL statement text", end);
		switch(stack[stackSzM1].getType())
		{
		case SQLWidget.SELECT_STATEMENT:
			if (currentSelect.fromTablesCount < 1) throw new SQLParseException(matcher.end());
			break ;
		case SQLWidget.WHERE_CLAUSE:
			if (((SQLWhereClause)stack[stackSzM1]).involvedColumnsCount < 1 && !currentSelect.withInfaJoins) throw new SQLParseException(matcher.end());
			stack[stackSzM1].__trim();
			break ;
		case SQLWidget.GROUP_BY_CLAUSE:
			if (((SQLGroupByClause)stack[stackSzM1]).columnsCount < 1) throw new SQLParseException(matcher.end());
			stack[stackSzM1].__trim();
			break ;
		case SQLWidget.ORDER_BY_CLAUSE:
			if (((SQLOrderByClause)stack[stackSzM1]).columnsCount < 1) throw new SQLParseException(matcher.end());
			stack[stackSzM1].__trim();
			break ;
		case SQLWidget.JOIN:
			if (((SQLJoin)stack[stackSzM1]).joinColumnsCount < 1) throw new SQLParseException(matcher.end());
			stack[stackSzM1].__trim();
			break ;
		case SQLWidget.SET_STATEMENT:
			if (stackSzM1 != 0) {
				throw new SQLParseException("parse error - end of SQL statement reached unexpectedly (stackSzM1: " + stackSzM1 + ", type: " + SQLWidget.getTypeCode(stack[stackSzM1].getType()) + ", class" + stack[stackSzM1].getClass().getName() + ", offset: " + sqlSelectStmt.length() + ")", sqlSelectStmt.length());
			}
			break ;
		default:
			throw new SQLParseException("parse error - end of SQL statement reached unexpectedly (stackSzM1: " + stackSzM1 + ", type: " + SQLWidget.getTypeCode(stack[stackSzM1].getType()) + ", class" + stack[stackSzM1].getClass().getName() + ", offset: " + sqlSelectStmt.length() + ")", sqlSelectStmt.length());
		}
		currentSelect.__trim();
		if (returnStmt.getType() == SQLWidget.SET_STATEMENT) {
			returnStmt.__trim();
		}
		return returnStmt; //stmt;
	}

	private static SQLColumn __newColumn(String tableAlias, String columnName, SQLSelectStmt currentSelect) {
		SQLColumn col;
		if (tableAlias.isEmpty()) {
			byte literalType = SQLLiteral.checkLiteralType(columnName);
			if (literalType != SQLLiteral.STRING_LITERAL) {
				col = new SQLLiteral(columnName/*value*/, literalType, EMPTY_STR);
			}
			else {
				col = new SQLColumnRef(SQLStmtTable.NO_TABLE, columnName, EMPTY_STR);
			}
		}
		else {
			if (currentSelect.fromTablesCount > 0) {
				SQLTableRef tblRef = currentSelect.getTableByAliasExt(tableAlias);
				if (tblRef != null) {
					col = new SQLColumnRef(tblRef, columnName, EMPTY_STR);
				}
				else {
					col = new SQLRawColumnRef(tableAlias, columnName, EMPTY_STR);
				}
			}
			else {
				col = new SQLRawColumnRef(tableAlias, columnName, EMPTY_STR);
			}
		}
		return col;
	}

	static final boolean is_in_select(final String str, int start, final int end) {
		if (!zone_equals_ci(str, start, end, IN_KWORD)) return false;
		start = skipWs(str, start + 2, end);
		if (start < end) {
			if (str.charAt(start) != '(') return false;
			start = skipWs(str, start + 1, end);
			if (start < end) {
				return zone_equals_ci(str, start, end, SELECT_KWORD);
			}
		}
		return false;
	}

	public static final void main(String[] args) { //BBB_SDE_AP_InvoiceHoldsFact BBB_AP_SDE_InvoicePaymentFact BBB_CST_SDE_Ramcs2ForecastQuantityByCGLOrMatref_QUAL BBB_PO_SDE_VendorSiteDimension BBB_SDE_FNDLookupValueDimension set_with_nested_set case_sql_stmt_tes BBB_RESOLVED_SYNONYMS_V mplt_BC_ORA_SalesInvoiceLinesFact_Brazil SDE_ORA_InventoryDailyBalanceFact_sqlquery mplt_BC_ORA_UOMConversionGeneral_IntraClass_SQ_QUAL mplt_BC_ORA_UOMConversionGeneral_InterClass_SQ_QUAL an_sql SQ_AR_XACTS_APPREC_ACRA SQ_AR_XACTS_APPREC_CURCR BBB_GL_SDE_PA_LinkageInformation_Extract_QUAL BBB_CST_SDE_Ramcs2FrozenResourceCostByCaiOrMatnumDetail_QUAL
		String filePath = /*"C:\\Users\\hp\\Documents\\sj\\source\\sql\\BBB_SDE_AP_InvoiceHoldsFact.sql"; //"C:\\Users\\hp\\Documents\\sj\\source\\sql\\sde_sqlquery_example.sql"; //"C:\\Users\\hp\\Documents\\sj\\source\\sql\\BBB_OZF_SDE_OfferDimension_failed.sql"; */"C:\\Users\\hp\\Documents\\sj\\source\\sql\\BBB_CST_SDE_Ramcs2FrozenResourceCostByCaiOrMatnumDetail_QUAL.sql"; //"C:\\Users\\hp\\Documents\\sj\\source\\sql\\sde_sqlquery_example.sql"; //"C:\\Users\\hp\\Documents\\sj\\source\\sql\\BBB_SDE_ORA_SalesInvoiceLinesFact_CUP.sql"; //"C:\\Users\\hp\\Documents\\sj\\source\\sql\\mplt_BC_ORA_SalesInvoiceLinesFact_Brazil.sql"; //"C:\\Users\\hp\\Documents\\sj\\source\\sql\\BBB_SDE_ORA_APTransactionFact_Distributions_CUP_4871.sql"; //"C:\\Users\\hp\\Documents\\sj\\source\\sql\\BBB_SDE_ORA_ProductDimension_CategorySet_Brand_5508_CUP.sql"; //"C:\\Users\\hp\\Documents\\sj\\source\\sql\\BBB_SDE_ORA_ExchangeRatePLYTD_CleanUp.sql"; ;
		File file = null;
		try
		{
			file = new File(filePath);
		}
		catch(Exception ex)
		{
			System.out.println("Error: " + ex.getMessage());
			return;
		}
		char[] buf = new char[(int)file.length()];
		InputStreamReader rdr = null;
		int strLen = -1;
		try
		{
			rdr = new InputStreamReader(new java.io.BufferedInputStream(new FileInputStream(file)), "UTF-8");
			strLen = rdr.read(buf);
		}
		catch(Exception ex)
		{
			//System.out.println("Error: " + ex.getMessage());
			return ;
		}
		String sqlText = new String(buf, 0, strLen);
		//System.out.println(sqlText + "\r\n\r\n");

		SQLStatement stmt = parseSQLStatement(sqlText);
		//SQLSelectStmt selectStmt = (SQLSelectStmt)stmt;
		//System.out.println("selectStmt.columnsCount: " + selectStmt.columnsCount + ", selectStmt.fromTablesCount: " + selectStmt.fromTablesCount + ", selectStmt.joinsCount: " + selectStmt.joinsCount);
		System.out.println("\r\nstmt: \r\n" + stmt);
	}

}