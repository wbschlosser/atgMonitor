import groovy.sql.Sql
import java.text.SimpleDateFormat
/*
 *File: atgMonitor.groovy
 *Author: Wes SChlosser
 *Last modified: 8/21/13
 *A script to update the daily ATG issues list quickly
 */
class IssuesList {

	static main(args) {
		def	startDt = new Date() - 4
		startDt = startDt.format("yyyyMMdd")
		def endDt = new Date() + 1
		endDt = endDt.format("yyyyMMdd")
		def results = ""
		
		println "Running..."
		
		//define and connect to OMS database
		def url = "server URL"
		def user = "username"
		def password = "password"
		Sql dbOms = Sql.newInstance(url, user, password,"oracle.jdbc.OracleDriver")
		
		//define and connect to ecom database
		url = "server URL"
		user = "username"
		password = "password"
		Sql dbEcom = Sql.newInstance(url, user, password,"oracle.jdbc.OracleDriver")
	
		def omsQuery = "select count(*) " + 
					   "from sterling.yfs_order_header a " +
					   "where order_no > '650000000' " +
					   "and DOCUMENT_TYPE = '0001' " +
					   "and to_char(a.order_date,'YYYYMMDD') >= '20130701' " +
					   "and total_amount > 0 " +
					   "AND NOT EXISTS (SELECT 'X' " + 
					   "FROM STERLING.YFS_ORDER_INVOICE S " +
					   "WHERE S.ORDER_HEADER_KEY = A.ORDER_HEADER_KEY)"
					   
		//open ATG orders count in OMS
		dbOms.eachRow(omsQuery) {totalAtgCount ->
			def totalOpenAtg = totalAtgCount[0]
			println "Open ATG Orders Count in OMS (A6): " + totalOpenAtg
			results += "Open ATG Orders in OMS (A6): " + totalOpenAtg
		}
		
		omsQuery = "select count(*)  from sterling.yfs_order_header oh, sterling.yfs_order_hold_type ot " +
				   "where oh.order_header_key=ot.order_header_key " +
				   "and to_char(oh.order_date,'YYYY-MM-DD') >= '2013-07-01' " +
				   "and oh.order_no > '650000000' " +
				   "and oh.hold_flag='Y' " +
				   "and oh.document_type='0001' " +
				   "and ot.status='1100' " +
				   "and ot.hold_type='OMSOutOfBalanceHold'"
				   
		//open OOBs in OMS
		dbOms.eachRow(omsQuery) {totalOob ->
			def totalOobOms = totalOob[0]
			println "Open OOBs in OMS (B6): " + totalOobOms
			results +="\nOpen OOBS (B6): " + totalOobOms
		}
		
		omsQuery ="select count(*) from STERLING.yfs_reprocess_error " +
				  "where errortxnid > '20130701' and " +
				  "state in ('Initial') and " +
				  "TO_CHAR(SUBSTR(MESSAGE,INSTR(MESSAGE,'OrderNo=',1,1)+9,9)) > '650000000' " + 
				  "and flow_name in ('GetCreateOrderMsgASyncService','CashEarnedActivationAsyncService','GetRepriceInfoAsyncService')"
				  
		//open orders exceptions in OMS
		dbOms.eachRow(omsQuery){totalExceptions ->
			def exceptionsTotal = totalExceptions[0]
			println "Open Orders Exceptions in OMS (C6): " + exceptionsTotal
			results += "\nOpen orders exceptions (C6): " + exceptionsTotal
		}

		omsQuery = "WITH OOB as (select sum(oh.TOTAL_AMOUNT) as total,sum(p.total_authorized+p.total_charged) as totalauth " + 
				   "from sterling.yfs_order_header oh, sterling.yfs_order_hold_type ot,sterling.yfs_payment p " +
				   "where " +
				   "oh.order_header_key=ot.order_header_key " +
				   "and oh.order_header_key = p.order_header_key " +
				   "and to_char(oh.order_date,'YYYYMMDD') >= '20130701' " +
				   "and oh.order_no > '650000000' " +
				   "and oh.hold_flag='Y' " +
				   "and oh.document_type='0001' " +
				   "and ot.hold_type='OMSOutOfBalanceHold') " +
				   ", REPRICE as ( " +
				   "select sum(h.TOTAL_AMOUNT) as total,sum(p.total_authorized+p.total_charged) as totalauth from STERLING.yfs_reprocess_error e,sterling.yfs_order_header h,sterling.yfs_order_hold_type t,sterling.yfs_payment p " +
				   "where " +
				   "e.errortxnid > '20130701' " +
				   "and h.ORDER_NO = TO_CHAR(SUBSTR(e.MESSAGE,INSTR(e.MESSAGE,'OrderNo=',1,1)+9,9)) " +
				   "and e.state in ('Initial') and " +
				   "TO_CHAR(SUBSTR(e.MESSAGE,INSTR(e.MESSAGE,'OrderNo=',1,1)+9,9)) > '650000000' " +
				   "and e.flow_name in ('GetRepriceInfoAsyncService') " +
				   "and h.ORDER_HEADER_KEY = t.ORDER_HEADER_KEY " +
				   "and h.order_header_key = p.order_header_key " +
				   "and t.HOLD_TYPE = 'InvoiceHoldIndicator' " +
				   "and h.HOLD_FLAG = 'Y' " +
				   "and not exists (select 'x' from STERLING.YFS_ORDER_HOLD_TYPE ht " +
				   "where ht.ORDER_HEADER_KEY = h.ORDER_HEADER_KEY " +
				   "and ht.HOLD_TYPE = 'OMSOutOfBalanceHold') " +
				   "), " +
				   "missingunitcost as ( " +
				   "SELECT sum(B.TOTAL_AMOUNT) as total " +
				   " FROM STERLING.YFS_ORDER_LINE A " +
				   " JOIN STERLING.YFS_ORDER_HEADER B " +
				   "   ON A.ORDER_HEADER_KEY = B.ORDER_HEADER_KEY " +
				   "WHERE to_char(b.order_date,'YYYYMMDD') >='20130701' " +
				   " AND A.UNIT_PRICE <> A.RETAIL_PRICE/A.ORDERED_QTY " +
				   " AND B.DOCUMENT_TYPE='0001' " +
				   " AND B.INVOICE_COMPLETE ='N'AND B.ORDER_NO > '650000000' " +
				   " AND A.ORDERED_QTY <> 0 " +
				   "), " +
				   "invalidcreditcard as ( " +
				   "select sum(h.total_amount) as total,sum(p.total_authorized+p.total_charged) as totalauth " +
				   "from sterling.yfs_payment p, sterling.yfs_order_header h " +
				   "where h.order_header_key=p.order_header_key " +
				   "and h.document_type='0001' " +
				   "and length(translate(trim(credit_card_no),' +-.0123456789',' ')) > 0 " +
				   "and to_char(h.ORDER_DATE,'YYYYMMDD') >= '20130701' " +
				   "and credit_card_no <> 'XXXXXXXXXXXXXX00' ) " +
				   ", " +
				   "totaldollar as ( " +
				   "Select sum(o.total) as total,sum(o.totalauth) as totalauth from " +
				   "oob o " +
				   "union " +
				   "select sum(r.total) as total,sum(r.totalauth) as totalauth from " +
				   "reprice r " +
				   "union " +
				   "select sum(i.total) as total,sum(i.totalauth) as totalauth from " +
				   "invalidcreditcard i " +
				   ") " +
				   "Select sum(t.total),sum(t.totalauth) from " +
				   "totaldollar t"
				   
		//total exceptions dollars unresolved
		dbOms.eachRow(omsQuery) {totalImpacted ->
			def totalDollarsImpacted = totalImpacted[1]
			println "exception dollars unresolved (authorized) (G6): " + totalDollarsImpacted
			results +="\nexceptions dollars unresolved (authorized) (G6): " + totalDollarsImpacted
		}

		
		omsQuery="SELECT " + 
				 "sum(p.total_authorized+p.total_charged) as totalauth " + 
				 "FROM STERLING.YFS_ORDER_HEADER  A                   " + 
				 "JOIN STERLING.YFS_ORDER_HOLD_TYPE B            " + 
				 "ON A.ORDER_HEADER_KEY = B.ORDER_HEADER_KEY " + 
				 "JOIN STERLING.YFS_PAYMENT P " + 
				 "ON A.ORDER_HEADER_KEY = P.ORDER_HEADER_KEY " + 
				 "WHERE a.order_no > '650000000' " + 
				 "and B.STATUS = '1100'                              " + 
				 "AND (B.HOLD_TYPE = 'InvoiceHoldIndicator') " + 
				 "AND TRUNC(B.MODIFYTS) < TRUNC(SYSDATE - 1)   " + 
				 "and not exists (	select 'x' from sterling.yfs_order_hold_type ht " + 
				 "where ht.ORDER_HEADER_KEY = A.ORDER_HEADER_KEY " + 
				 "and ht.HOLD_TYPE = 'OMSOutOfBalanceHold')"
				 
		//total repricing dollars unresolved
		dbOms.eachRow(omsQuery) { repriceImpact ->
			def totRepImp = repriceImpact[0]
			println "reprice dollars unresolved (H6): " + totRepImp
			results += "\nreprice dollars unresolved (H6): " + totRepImp
		}
		
		omsQuery = "select to_char(order_date,'mm/dd/yyyy'), count(*) " + 
				   "from STERLING.yfs_order_header a " +
				   "where order_no > '650000000' " +
				   "and DOCUMENT_TYPE = '0001' " +
				   "and to_char(order_date,'yyyyMMDD') >= '$startDt' " +
				   "and to_char(order_date,'yyyyMMDD') < '$endDt' " +
				   " group by to_char(order_date,'mm/dd/yyyy') order by to_char(order_date,'mm/dd/yyyy')"
				   
		println "\nOrder Count in OMS (A11 and C48): "
		results += "\n\nOrder Count in OMS (A11 and C48): "
		dbOms.eachRow(omsQuery) {dailyAtgCount ->
			def dailyAtgCountDate = dailyAtgCount[0]
			def dailyAtgOrders = dailyAtgCount[1]
			println dailyAtgCountDate+ ": " + dailyAtgOrders
			results += "\n" + dailyAtgCountDate + ": " + dailyAtgOrders
		}
		
		omsQuery = "select to_char(oh.order_date,'mm/dd/yyyy'), count(*) " +  
				   "from sterling.yfs_order_header oh, sterling.yfs_order_hold_type ot " +
				   "where oh.order_header_key=ot.order_header_key " +
				   "and to_char(oh.order_date,'YYYYMMDD') >= '$startDt' " +
				   "and to_char(oh.order_date,'YYYYMMDD') < '$endDt' " +
				   "and oh.order_no > '650000000' " +
				   "and oh.document_type='0001' " +
				   "and ot.hold_type='OMSOutOfBalanceHold' " +
				   "group by to_char(oh.order_date,'mm/dd/yyyy') order by to_char(oh.order_date,'mm/dd/yyyy')"
				   
		//orders in oob holds for each date
		println "\nOObs In OMS (A12): "
		results += "\n\nOObs In OMS (A12): "
		dbOms.eachRow(omsQuery) {dailyOobs ->
			def dailyOobDate = dailyOobs[0]
			def dailyOob = dailyOobs[1]
			println dailyOobDate + ": " + dailyOob
			results += "\n" + dailyOobDate + ": " + dailyOob
		}
		omsQuery = "WITH OOB AS (select TO_CHAR(oh.ORDER_DATE,'YYYYMMDD') as orderdt,sum(oh.TOTAL_AMOUNT) as total,sum(p.total_authorized+p.total_charged) as totalauth,sum(oh.TOTAL_AMOUNT) from sterling.yfs_order_header oh, sterling.yfs_order_hold_type ot,STERLING.YFS_PAYMENT p " +
				   "where  " +
				   "oh.order_header_key=ot.order_header_key " +
				   "and to_char(oh.order_date,'YYYYMMDD') >= '$startDt'  " +
				   "and oh.order_no > '650000000' " +
				   "and p.ORDER_HEADER_KEY = oh.ORDER_HEADER_KEY " +
				   "and oh.document_type='0001' " +
				   "and oh.ORDER_HEADER_KEY = p.order_header_key " +
				   "and ot.hold_type='OMSOutOfBalanceHold' " +
				   "group by TO_CHAR(oh.ORDER_DATE,'YYYYMMDD')), " +
				   "CASH AS ( " +
				   " " +
				   "select h.order_no, " +
				   "to_char(h.order_date,'YYYYMMDD') as orderdt, " +
				   "sum(h.total_amount) as total,sum(p.total_authorized+p.total_charged) as totalauth  " +
				   "from STERLING.yfs_reprocess_error e,sterling.yfs_order_header h,STERLING.YFS_PAYMENT p " +
				   "where e.errortxnid > '$startDt' " +
				   "and p.ORDER_HEADER_KEY = h.ORDER_HEADER_KEY " +
				   "and to_char(h.order_date,'YYYYMMDD') >= '$startDt' " +
				   "and TO_CHAR(SUBSTR(MESSAGE,INSTR(MESSAGE,'OrderNo=',1,1)+9,9)) > '650000000'    " +
				   "and PARENTTXNID=errortxnid " +
				   "and h.order_no = TO_CHAR(SUBSTR(e.MESSAGE,INSTR(e.MESSAGE,'OrderNo=',1,1)+9,9)) " +
				   "and flow_name in ('CashEarnedActivationAsyncService') " +
				   "group by to_char(h.order_date,'YYYYMMDD'),h.order_no), " +
				   "REPRICE AS ( " +
				   "SELECT  " +
				   "to_char(a.order_date,'YYYYMMDD') as orderdt, " +
				   "sum(a.total_amount) as total, " +
				   "sum(p.total_authorized+p.total_charged) as totalauth  " +
				   "FROM STERLING.YFS_ORDER_HEADER  A                   " +
				   "JOIN STERLING.YFS_ORDER_HOLD_TYPE B            " +
				   "ON A.ORDER_HEADER_KEY = B.ORDER_HEADER_KEY " +
				   "JOIN STERLING.YFS_PAYMENT P " +
				   "ON A.ORDER_HEADER_KEY = P.ORDER_HEADER_KEY " +
				   "WHERE a.order_no > '650000000' " +
				   "and to_char(a.order_date,'YYYYMMDD') >='$startDt' " +
				   "and B.STATUS = '1100'                              " +
				   "AND (B.HOLD_TYPE = 'InvoiceHoldIndicator') " +
				   "AND TRUNC(B.MODIFYTS) < TRUNC(SYSDATE - 1)   " +
				   "and not exists (select 'x' from sterling.yfs_order_hold_type ht " +
				   "where ht.ORDER_HEADER_KEY = A.ORDER_HEADER_KEY " +
				   "and ht.HOLD_TYPE = 'OMSOutOfBalanceHold') " +
				   "group by to_char(a.order_date,'YYYYMMDD')) " +
				   ", " +
				   "ORDCREATE AS ( " +
				   " " +
				   "select h.order_no, " +
				   "to_char(h.order_date,'YYYYMMDD') as orderdt, " +
				   "sum(h.total_amount) as total,sum(p.total_authorized+p.total_charged) as totalauth  " +
				   "from STERLING.yfs_reprocess_error e,sterling.yfs_order_header h,STERLING.YFS_PAYMENT p " +
				   "where e.errortxnid > '$startDt' " +
				   "and p.ORDER_HEADER_KEY = h.ORDER_HEADER_KEY " +
				   "and to_char(h.order_date,'YYYYMMDD') >= '$startDt' " +
				   "and TO_CHAR(SUBSTR(MESSAGE,INSTR(MESSAGE,'OrderNo=',1,1)+9,9)) > '650000000'    " +
				   "and PARENTTXNID=errortxnid " +
				   "and h.order_no = TO_CHAR(SUBSTR(e.MESSAGE,INSTR(e.MESSAGE,'OrderNo=',1,1)+9,9)) " +
				   "and e.flow_name in ('GetRepriceInfoAsyncService') " +
				   "and h.order_no not in (select order_no from cash) " +
				   "and h.order_no not in (select order_no from reprice) " +
				   "and flow_name in ('GetCreateOrderMsgASyncService') " +
				   "group by to_char(h.order_date,'YYYYMMDD'),h.order_no) " +
				   ",EXCEPTIONREP as ( " +
				   "select orderdt,sum(total) as total,sum(totalauth) as totalauth from  " +
				   "reprice r " +
				   "group by orderdt " +
				   "union " +
				   "select orderdt,sum(total) as total,sum(totalauth) as totalauth from " +
				   "cash k  " +
				   "group by orderdt " +
				   "union " +
				   "select orderdt,sum(total) as total,sum(totalauth) as totalauth from " +
				   "ordcreate " +
				   "group by orderdt), " +
				   "REPROCESS as ( " +
				   "select orderdt as orderdt,sum(total) as total,sum(totalauth) as totalauth " +
				   "from exceptionrep " +
				   "group by orderdt), " +
				   "missingunitcost as ( " +
				   "SELECT to_char(B.ORDER_DATE,'YYYYMMDD') as orderdt,sum(B.TOTAL_AMOUNT) as total,sum(p.total_authorized+p.total_charged) as totalauth " +
				   " FROM STERLING.YFS_ORDER_LINE A " +
				   " JOIN STERLING.YFS_ORDER_HEADER B " +
				   "   ON A.ORDER_HEADER_KEY = B.ORDER_HEADER_KEY " +
				   " JOIN STERLING.YFS_PAYMENT p " +
				   "   ON p.order_header_key = a.ORDER_HEADER_KEY " +
				   "WHERE to_char(b.order_date,'YYYYMMDD') >='$startDt' " +
				   " AND A.UNIT_PRICE <> A.RETAIL_PRICE/A.ORDERED_QTY " +
				   " AND B.DOCUMENT_TYPE='0001' " +
				   " AND B.INVOICE_COMPLETE ='N'AND B.ORDER_NO > '650000000' " +
				   " AND A.ORDERED_QTY <> 0 " +
				   " group by to_char(b.order_date,'YYYYMMDD') " +
				   "), " +
				   "invalidcreditcard as ( " +
				   "select to_char(h.ORDER_DATE,'YYYYMMDD') as orderdt,sum(h.TOTAL_AMOUNT) as total,sum(p.total_authorized+p.total_charged) as totalauth " +
				   "from sterling.yfs_payment p, sterling.yfs_order_header h " +
				   "where h.order_header_key=p.order_header_key " +
				   "and h.document_type='0001' " +
				   "and length(translate(trim(credit_card_no),' +-.0123456789',' ')) > 0 " +
				   "and to_char(h.ORDER_DATE,'YYYYMMDD') >= '$startDt' " +
				   "and credit_card_no <> 'XXXXXXXXXXXXXX00'  " +
				   "group by to_char(h.order_date,'YYYYMMDD')) " +
				   ", " +
				   "totaldollar as ( " +
				   "Select o.orderdt as orderdt,sum(o.total) as total,sum(o.totalauth) as totalauth from " +
				   "oob o " +
				   "group by o.orderdt " +
				   "union " +
				   "select r.orderdt as orderdt,sum(r.total) as total,sum(r.totalauth) as totalauth from " +
				   "reprocess r " +
				   "group by r.orderdt " +
				   "union " +
				   "select u.orderdt as orderdt,sum(u.total) as total,sum(u.totalauth) as totalauth from  " +
				   "missingunitcost u " +
				   "group by u.orderdt " +
				   "union " +
				   "select i.orderdt as orderdt,sum(i.total) as total,sum(i.totalauth) as totalauth from " +
				   "invalidcreditcard i " +
				   "group by i.orderdt) " +
				   "Select t.orderdt,sum(t.total),sum(t.totalauth) from " +
				   "totaldollar t " +
				   "group by t.orderdt " +
				   "order by t.orderdt"
				   
		//dollar impact by date
		println "\nImpact by date (A16 and A17):"
		results += "\n\nImpact by date (A16 and A17):"
		dbOms.eachRow(omsQuery) {dailyImpact ->
			def dailyImpactDate = dailyImpact[0]
			def dailySum = dailyImpact[1]
			def dailyTotalAuth = dailyImpact[2]
			println dailyImpactDate + ": \n      sum: " + dailySum + "\ntotalAuth: " + dailyTotalAuth
			results += "\n" + dailyImpactDate + ": \n      sum: " + dailySum + "\ntotalAuth: " + dailyTotalAuth
		}
		def atgQuery = "select count(*), to_char(SUBMITTED_DATE,'mm/dd/yyyy') " +
					   "FROM ATGPRDCORE.DCSPP_ORDER ord " +
					   "where ord.STATE in ('PROCESSING','NO_PENDING_ACTION') " +
					   "and ORDER_ID > '650000000' " +
					   "and to_char(SUBMITTED_DATE,'yyyymmdd') >= '$startDt' " +
					   "and to_char(SUBMITTED_DATE,'yyyymmdd') < '$endDt' " +
					   "group by to_char(ord.SUBMITTED_DATE,'mm/dd/yyyy') " +
					   "order by to_char(ord.SUBMITTED_DATE,'mm/dd/yyyy')"
					   
        //ATG order count by date
        println "\nAtg Order Count by day (B48): "
		results += "\n\nAtg Order Count by day (B48):"
		dbEcom.eachRow(atgQuery) {dailyAtgOrders ->
			def atgOrderDate = dailyAtgOrders[1]
			def atgOrderCount = dailyAtgOrders[0]
			println atgOrderDate+ ": "+atgOrderCount
			results += "\n" + atgOrderDate + ": " + atgOrderCount
		}

		println "Orders OOB (D48): "
		results += "\n\nOrders OOB (D48): "

		def v = 0
		//loop for queries with specific days (D48)
		while (v < 4){
			def loopSDt = new Date() - 3 + v
			loopSDt = loopSDt.format("yyyyMMdd")
			def loopEDt = new Date() - 2 + v
			loopEDt = loopEDt.format("yyyyMMdd")
			
			omsQuery = "with orders as ( " +
					   "select oh.ORDER_NO,oh.order_header_key,oh.TOTAL_AMOUNT  from sterling.yfs_order_header oh, sterling.yfs_order_hold_type ot " +
					   "where oh.order_header_key=ot.order_header_key " +
					   "and to_char(oh.order_date,'YYYYMMDD') >= '${loopSDt}' " +
					   "and to_char(oh.order_date,'YYYYMMDD') < '${loopEDt}' " +
					   "and oh.order_no > '650000000' " +
					   "and oh.document_type='0001' " +
					   "and ot.status = '1100' " +
					   "and ot.hold_type='OMSOutOfBalanceHold' " +
					   ")" +
					   "SELECT count(*), " +
					   "sum(b.TOTAL_authorized+b.TOTAL_CHARGED-o.total_amount) \"diff\" " +
					   "from orders o,STERLING.YFS_PAYMENT b " +
					   "where o.order_header_key = b.ORDER_HEADER_KEY"
					   
			//out of balance order count
			dbOms.eachRow(omsQuery){difference ->
				def diffAmnt = difference[0]
				println "$loopSDt: " + diffAmnt
				results += "\n$loopSDt: " + diffAmnt
			}
			v++
		}//end while
		println "-------------------------------------------------------------------------------"
		results += "\n-------------------------------------------------------------------------------"
		
		//incorrect unit cost
		println "Incorrect Unit Cost (H48): "
		results += "\n\nIncorrect Unit Cost (H48): "
		
		def j = 0
		//loop for queries with specific days (H48)
		while (j < 4){
			def loopSDt = new Date() - 3 + j
			loopSDt = loopSDt.format("yyyyMMdd")
			def loopEDt = new Date() - 2 + j
			loopEDt = loopEDt.format("yyyyMMdd")
			
			omsQuery = "SELECT count(*) " +
					   "FROM STERLING.YFS_ORDER_LINE OL " +
					   "JOIN STERLING.YFS_ORDER_HEADER OH " +
					   "ON OL.ORDER_HEADER_KEY = OH.ORDER_HEADER_KEY " +
					   "WHERE to_char(OH.order_date,'YYYYMMDD') >= '${loopSDt}' " +
					   "and to_char(OH.order_date,'YYYYMMDD') < '${loopEDt}' " +
					   "AND OL.UNIT_PRICE <> OL.RETAIL_PRICE/OL.ORDERED_QTY " +
					   "AND OH.DOCUMENT_TYPE='0001' " +
					   "AND OH.INVOICE_COMPLETE ='N' AND OH.ORDER_NO > '650000000' " +
					   "AND OL.ORDERED_QTY <> 0"
					   
			dbOms.eachRow(omsQuery) {incorrect->
				def ordCount = incorrect[0]
				println "$loopSDt: " + ordCount
				results += "\n$loopSDt: "  + ordCount
			}
			j++
		}//end while
		println "-------------------------------------------------------------------------------"
		results += "\n-------------------------------------------------------------------------------"

		def i = 0
		//loop for queries with specific days (I-N48)
		while (i < 4){
			def loopSDt = new Date() - 3 + i
			loopSDt = loopSDt.format("yyyyMMdd")
			def loopEDt = new Date() - 2 + i
			loopEDt = loopEDt.format("yyyyMMdd")
			
			println "\nFor date: $loopSDt"
			results += "\n\nFor date: $loopSDt"
			
			omsQuery = "select count(*),flow_name,errorstring from STERLING.yfs_reprocess_error " +
					   "where " + 
					   "errortxnid >= '${loopSDt}' and " +
					   "errortxnid < '${loopEDt}' " + 
					   "and TO_CHAR(SUBSTR(MESSAGE,INSTR(MESSAGE,'OrderNo=',1,1)+9,9)) > '650000000'   and PARENTTXNID=errortxnid " +
					   "and flow_name in ('GetCreateOrderMsgASyncService','CashEarnedActivationAsyncService','GetRepriceInfoAsyncService') " +
					   "group by state,flow_name, errorcode, errorstring order by flow_name"
					   
			//orders in exceptions
			println "\nOrders in exceptions (I-N48): "
			results += "\nOrders in exceptions (I-N48):\n"
			dbOms.eachRow(omsQuery) {exceptions ->
				def exCount = exceptions[0]
				def exFlow = exceptions[1]
				def exStr = exceptions[2]
				println exCount + "\t" +exFlow +"\t" + exStr + "\n"
				results += exCount + "\t" +exFlow +"\t" + exStr + "\n"
			}
			omsQuery = "select count(*),errorstring from STERLING.yfs_reprocess_error " +
			           "where " +
			           "errortxnid >= '${loopSDt}' and " +
			           "errortxnid < '${loopEDt}' " +
			           "and TO_CHAR(SUBSTR(MESSAGE,INSTR(MESSAGE,'OrderNo=',1,1)+9,9)) > '650000000'   and PARENTTXNID=errortxnid " +
			           "and flow_name in ('GetCreateOrderMsgASyncService','CashEarnedActivationAsyncService','GetRepriceInfoAsyncService') " +
			           "group by errorcode, errorstring order by errorstring"
			
            // orders in exceptions
            println "\nOrders in exceptions (I-N48): "
            results += "\nOrders in exceptions (I-N48): \n"
            dbOms.eachRow(omsQuery) {exceptions ->
	            def exCount = exceptions[0]
	            def exStr = exceptions[1]
	            println exCount + "\t" + exStr + "\n"
	            results += exCount + "\t" + exStr + "\n"
            }
		
			
			println "-------------------------------------------------------------------------------"
			results += "\n-------------------------------------------------------------------------------"
			i++
		}//end while
/* --not needed, issue was fixed 7/18/2013
		
		omsQuery = "select order_date, count(*) OrderCount " +
		"from (select distinct oh.order_no, to_char(oh.order_date,'MM-dd-yyyy') order_date " +
		"from STERLING.yfs_payment yp, STERLING.yfs_order_header oh " +
		"where oh.order_no > '650000000' and " +
		"oh.document_type = '0001' and " +
		"oh.total_amount <> 0 and " +
		"oh.order_header_key= yp.order_header_key and " +
		"LENGTH(TRIM(TRANSLATE(yp.credit_card_no, ' +-.0123456789', ' '))) is not null order by oh.order_no) group by order_date order by order_date"
		
		//invalid credit card count by date
		println "\ninvalid credit cards by date (O48):"
		results += "\ninvalid credit cards (O48):"
		dbOms.eachRow(omsQuery) {dailyCredit ->
			def dailyCreditDate = dailyCredit[0]
			def dailyCreditCount = dailyCredit[1]
			println dailyCreditDate + ": " + dailyCreditCount
			results += "\n" + dailyCreditDate + ": " + dailyCreditCount
		}
*/
		 omsQuery = "SELECT TO_CHAR(B.MODIFYTS,'MM/DD/YYYY') HOLD_DATE, count(*), sum(A.TOTAL_AMOUNT) " +
		"FROM STERLING.YFS_ORDER_HEADER A " +
		"JOIN STERLING.YFS_ORDER_HOLD_TYPE B " +
		"ON A.ORDER_HEADER_KEY = B.ORDER_HEADER_KEY " +
		"WHERE a.order_no > '650000000' " +
		"and B.STATUS = '1100' " +
		"AND (B.HOLD_TYPE = 'InvoiceHoldIndicator') " +
		"AND TRUNC(B.MODIFYTS) < TRUNC(SYSDATE - 1) " +
		"group by TO_CHAR(B.MODIFYTS,'MM/DD/YYYY') " +
		"order by TO_CHAR(B.MODIFYTS,'MM/DD/YYYY')"
		
		println "\nOrders in repricing hold (P48):"
		results += "\n\nOrders in repricing hold (P48):"
		//orders in repricing hold
		dbOms.eachRow(omsQuery) {dailyHolds ->
			def holdDate = dailyHolds[0]
			def orderCount = dailyHolds[1]
			def orderSum = dailyHolds[2]
			println holdDate + ": " + orderCount + " " + orderSum
			results += "\n" + holdDate + ": " + orderCount + " " + orderSum
		}
	
		def ant = new AntBuilder()
		def mailList = ''//email addresses to send the results to go here
		ant.mail(mailhost:''/*enter your mail host*/, mailport:'25', subject:"ATG issues list", tolist:mailList){
		  from(address:'')
		  replyto(address:''/*enter an email, usually your own*/)
		  message(results)
		}
	}//end main
}
