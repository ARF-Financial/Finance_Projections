Imports System.Data.SqlClient
Imports System.Data.SqlTypes

Module FinanceProjections
    '================================================================================================================
    ' FinanceProjections.exe
    ' Adam Robins 2014-04-18
    '
    ' Prepare data for Tableau Finance Dashboard showing original projected loan cashflow stream vs. actual collections
    '
    ' ASR 11/3/2016 - store records in tblFinanceProjections_Static
    '================================================================================================================

    Private Connect_String As String = Configuration.ConfigurationManager.ConnectionStrings("Connect_String").ConnectionString

    Dim rc, IMLoanNbr As Integer
    Dim IMRefi As Boolean
    Dim SCPrinPor, SCIntPor, PLPrincipal, PLInterest, PLIntPenalty, PLBadDebtRec, IMRemPrin, IMRemInt As Decimal
    Dim IMRefiDate, SCStart_Date, SCEnd_Date, TransDate As Date

    Function Main() As Integer

        WriteToEventLog("I", "Start")

        ' Clear the FinanceProjections tables
        rc = Reset_Tables()
        If rc <> 0 Then
            Return rc
        End If

        '==============================================================
        ' Main Logic
        '==============================================================

        Try
            '
            ' Get funded loans from InfoMine
            '
            Dim IMconn As New SqlConnection(Connect_String)
            IMconn.Open()

            Dim IMLoanSQL As String = "SELECT loannbr, refinancedate, trialbal_remainPrin, trialbal_remainInt " &
                          "FROM tblInfomineAllLoans where loannbr > 10000 and loannbr < 90000 and loanfundingdate >= '01/01/2013'"
            Dim IMLoanDA As New SqlDataAdapter(IMLoanSQL, IMconn)
            Dim IMLoanDS As New DataSet
            IMLoanDA.Fill(IMLoanDS, "IMLoans")
            IMconn.Close()
            '
            ' Process each loan 
            '
            Dim IMLoanDT As DataTable = IMLoanDS.Tables.Item("IMLoans")
            Dim IMLoan As DataRow

            For Each IMLoan In IMLoanDT.Rows
                IMLoanNbr = IMLoan.Item("loannbr")
                If IsDBNull(IMLoan.Item("refinancedate")) Then
                    IMRefi = False
                Else
                    IMRefi = True
                    IMRefiDate = IMLoan.Item("refinancedate")
                End If
                If IMLoan.IsNull("trialbal_remainPrin") Then
                    IMRemPrin = 0
                Else
                    IMRemPrin = IMLoan.Item("trialbal_remainPrin")
                End If
                If IMLoan.IsNull("trialbal_remainInt") Then
                    IMRemInt = 0
                Else
                    IMRemInt = IMLoan.Item("trialbal_remainInt")
                End If

                'Process the projected payment schedule for the loan
                rc = Process_ARFSchedule()
                If rc <> 0 Then
                    Return rc
                End If
            Next

            ' Add Projections to tblFinanceProjections_Static 
            rc = Static_Proj()
            If rc <> 0 Then
                Return rc
            End If

            ' Now add the actual collections data for each loan to tblFinanceProjections
            rc = Process_SalesRepPandL()
            If rc <> 0 Then
                Return rc
            End If

            WriteToEventLog("I", "End")
            Return 0

        Catch ex As Exception
            WriteToEventLog("E", "Function Main - Message: " & ex.Message & " StackTrace: " & ex.StackTrace)
            Return -1
        End Try

    End Function

    Function Process_ARFSchedule() As Integer
        'The ARFSchedule 1140 records have the projected loan payments breakdown by date range.  A typical loan will have only
        'one 1140 record for the entire loan period, however a funding modification may allow for multiple records with varying payment terms.

        Dim Scconn As New SqlConnection(Connect_String)
        Try
            ' get the 1140 records for the loan
            Scconn.Open()
            Dim ScSQL As String = "SELECT start_date, end_date, coalesce(prinpor,0) as prinpor, coalesce(intepor,0) as intepor FROM tblARFSchedule " &
                                  "WHERE type_Code = 1140 and ApplyID = " & IMLoanNbr
            Dim ScLoanDA As New SqlDataAdapter(ScSQL, Scconn)
            Dim ScLoanDS As New DataSet
            ScLoanDA.Fill(ScLoanDS, "Sched1140")

            ' Process each 1140 record
            Dim ScLoanDT As DataTable = ScLoanDS.Tables.Item("Sched1140")
            Dim ScRec As DataRow
            Dim Pmt_Date As Date
            Dim PostRefi As Integer
            Dim WK_RemPrin As Decimal = IMRemPrin
            Dim WK_RemInt As Decimal = IMRemInt
            Dim first As Boolean = True

            For Each ScRec In ScLoanDT.Rows
                SCStart_Date = ScRec.Item("start_date")
                SCEnd_Date = ScRec.Item("end_date")
                SCPrinPor = ScRec.Item("prinpor")
                SCIntPor = ScRec.Item("intepor")

                ' calculate the first Wednesday payment date on or after the 1140 start date
                Pmt_Date = SCStart_Date.AddDays(3 - SCStart_Date.DayOfWeek)
                If Pmt_Date < SCStart_Date Then
                    Pmt_Date.AddDays(7)
                End If

                ' write an output record for each payment date in the 1140 date range. 
                Do While Pmt_Date <= SCEnd_Date
                    If Pmt_Date > Today() Then
                        If WK_RemPrin - SCPrinPor >= 0 Then
                            WK_RemPrin -= SCPrinPor
                        Else
                            WK_RemPrin = 0
                        End If
                        If WK_RemInt - SCIntPor >= 0 Then
                            WK_RemInt -= SCIntPor
                        Else
                            WK_RemInt = 0
                        End If
                    End If
                    ' set flag for projected payments post refi date because Finance Dashboard does not count them
                    If IMRefi And Pmt_Date > IMRefiDate Then
                        PostRefi = 1
                    Else
                        PostRefi = 0
                    End If
                    ' insert into Finance Projections Table
                    Dim PSSQL As String = "INSERT INTO tblFinanceProjections (LoanNbr, TransType, TransDate, ProjPrincipal, ProjInterest, ProjRemPrin, ProjRemInt, PostRefi) VALUES (" &
                                             IMLoanNbr & ",'PROJ','" & Pmt_Date & "'," & SCPrinPor & "," & SCIntPor & "," & WK_RemPrin & "," & WK_RemInt & "," & PostRefi & ")"
                    Dim PSCmd As New SqlCommand(PSSQL, Scconn)
                    PSCmd.ExecuteNonQuery()
                    Pmt_Date = Pmt_Date.AddDays(7)
                Loop
            Next

            Scconn.Close()
            Return 0

        Catch ex As Exception
            Scconn.Close()
            WriteToEventLog("E", "Function Process_ARFSchedule - Message: " & ex.Message & " StackTrace: " & ex.StackTrace)
            Return -1
        End Try

    End Function

    Function Process_SalesRepPandL() As Integer
        ' get the actual payment data for the loans from the SaleRepPandL table, adding it to the Projections table

        Dim PLconn As New SqlConnection(Connect_String)
        Try
            PLconn.Open()
            Dim PLSql As String
            PLSql = "insert into tblFinanceProjections (LoanNbr, TransType, TransDate, ActInterest, ActPrincipal, ActIntPenalty, ActBadDebtRec) " &
                    "Select fp.loannbr, transtype, transdate, InterestAmt, principalamt, IntPenaltyAmt, BadDebtRecAmt " &
                    "From tblSalesRepPandL pl inner Join (Select distinct loannbr from tblFinanceProjections) fp On pl.loanid = fp.LoanNbr " &
                    "where transtype in ('ACH','ACH CREDIT','ACH RETURN','MANUAL ADJ') and not transdate is null"

            'PLSql = "insert into tblFinanceProjections (LoanNbr, TransType, TransDate, ActInterest, ActPrincipal, ActIntPenalty, ActBadDebtRec) " &
            '         "select im.loannbr, transtype, transdate, InterestAmt, principalamt, IntPenaltyAmt, BadDebtRecAmt " &
            '         "from tblinfomineallloans im inner join tblSalesRepPandL PL on im.LoanNbr = PL.LoanID " &
            '         "where transtype in ('ACH','ACH CREDIT','ACH RETURN','MANUAL ADJ') and not transdate is null and PL.LoanID = " & IMLoanNbr

            Dim PLCmd As New SqlCommand(PLSql, PLconn)
            PLCmd.ExecuteNonQuery()
            PLconn.Close()
            Return 0

        Catch ex As Exception
            PLconn.Close()
            WriteToEventLog("E", "Function Process_SalesRepPandL - Message: " & ex.Message & " StackTrace: " & ex.StackTrace)
            Return -1
        End Try

    End Function

    Function Static_Proj() As Integer
        ' Add the new incremental projections to tblFinanceProjections_Static.  Unlike tblFinanceProjections, the records in tblFinanceProjections_Static are not rewritten if any
        ' payment schedule changes are made.
        Dim Scconn As New SqlConnection(Connect_String)
        Try
            Scconn.Open()
            Dim FPSSql As String = "insert into tblFinanceProjections_Static (Loannbr, transdate, projprincipal, projinterest, ProjRemPrin, ProjRemInt) " &
                               "Select fp.loannbr, fp.transdate, fp.projprincipal, fp.projinterest, fp.ProjRemPrin, fp.ProjRemInt " &
                               "From tblFinanceProjections fp left Join tblFinanceProjections_Static fps on fp.loannbr = fps.loannbr And fp.TransDate = fps.transdate " &
                               "Where fps.loannbr Is null And fps.transdate Is null and fp.transtype  = 'PROJ' "
            Dim FPSCmd As New SqlCommand(FPSSql, Scconn)
            FPSCmd.ExecuteNonQuery()
            Scconn.Close()
            Return 0
        Catch ex As Exception
            Scconn.Close()
            WriteToEventLog("E", "Function Static_Proj - Message: " & ex.Message & " StackTrace: " & ex.StackTrace)
            Return -1
        End Try
    End Function

    Function Reset_Tables() As Integer
        '
        ' Execute stored procedure to drop and recreate FinanceProjections tables
        '
        Dim conn As SqlConnection
        Dim cmd As SqlCommand

        Try
            conn = New SqlConnection(Connect_String)
            conn.Open()

            'Drop & recreate tables
            cmd = New SqlCommand("sp_FinanceProjections_Reset", conn) With {
                .CommandType = CommandType.StoredProcedure
            }
            cmd.ExecuteNonQuery()
            conn.Close()
            Return 0

        Catch ex As Exception
            WriteToEventLog("E", "Function Reset_Tables - Message: " & ex.Message & " StackTrace: " & ex.StackTrace)
            Return -1
        End Try

    End Function

    Public Function WriteToEventLog(ByVal entryType As String, ByVal entry As String) As Boolean
        'record to the application event log

        Console.WriteLine("FinanceProjections Event: " & entryType & "- " & entry)

        ' Write entry to Application Event Log

        Dim objEventLog As New EventLog
        Dim eventType As EventLogEntryType
        Dim appName As String = "ARF"
        Dim logName As String = "ARF_FinanceProjections"

        If entryType = "I" Then
            eventType = EventLogEntryType.Information
        Else
            eventType = EventLogEntryType.Error
        End If

        Try
            'Register the Application as an Event Source
            If Not EventLog.SourceExists(appName) Then
                EventLog.CreateEventSource(appName, logName)
            End If
            'log the entry
            objEventLog.Source = appName
            objEventLog.WriteEntry(entry, eventType)
            Return True
        Catch Ex As Exception
            Return False
        End Try

    End Function

End Module

