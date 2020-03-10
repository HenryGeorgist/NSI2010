
Imports System.Data.OleDb
Imports System.IO
Imports System.Data
Imports Poly2Tri
Imports LifeSimGIS
Imports GDALAssist

Public Class HAZUSToStructures
    Public Event ReportProgress(ByVal percentcomplete As Double)
    Public Event ReportErrorMessage(ByVal message As String, ByVal structureInventoryname As String)
    Public Event ReportNSIErrorMessage(ByVal message As String, ByVal outputdest As String)
    'Public Event ReportProgressMessage(ByVal message As String)
    Private _counter As Integer = 1
    Private _e As System.ComponentModel.DoWorkEventArgs
    Private _bw As System.ComponentModel.BackgroundWorker
    Private _structureInventoryname As String
    Public Sub CreateSI(ByVal sender As Object, e As System.ComponentModel.DoWorkEventArgs)
        _bw = CType(sender, System.ComponentModel.BackgroundWorker)

        Dim args As New CreateSIArgs

        args = CType(e.Argument, CreateSIArgs)
        _structureInventoryname = System.IO.Path.GetFileNameWithoutExtension(args.OutputDest)
        Dim ret As DataTable = Nothing
        Try
            ret = CreateSI(args.StudyAreaShapefile, args.BndryGrbsMDBPath, args.MSHMDBPath, args.VEHMDBPath, args.OutputDest)
        Catch ex As Exception
            RaiseEvent ReportErrorMessage(ex.Message, _structureInventoryname)
            e.Cancel = True
            e.Result = Nothing
        End Try

        If IsNothing(ret) Then
            e.Cancel = True
            e.Result = Nothing
        Else
            e.Cancel = False
            e.Result = ret
        End If
    End Sub
    Public Sub CreateStateLevelSI(ByVal sender As Object, e As System.ComponentModel.DoWorkEventArgs)
        _bw = CType(sender, System.ComponentModel.BackgroundWorker)

        Dim args As New CreateStateSIArgs

        args = CType(e.Argument, CreateStateSIArgs)

        Dim hzdatabases As List(Of String) = args.HAZUSDatabaseDirectory
        Dim outputDests As List(Of String) = args.NSIOutputDest
        Dim HazusDirectory As String = Nothing
        Dim OutputDirectory As String = Nothing
        For j = 0 To hzdatabases.Count - 1
            HazusDirectory = hzdatabases(j)
            OutputDirectory = outputDests(j)
            'Gather the state level County data.
            Dim dinfo As New DirectoryInfo(OutputDirectory)
            Dim statereport As New StateReport
            statereport.message = "Current State: " & dinfo.Name
            _bw.ReportProgress((j / hzdatabases.Count) * 100, statereport)
            _bw.ReportProgress(0, "Checking Databases")
            If Not File.Exists(HazusDirectory & "\bndrygbs.mdb") Then
                RaiseEvent ReportNSIErrorMessage("HAZUS database file not found:  " & HazusDirectory & "\bndrygbs.mdb", OutputDirectory & "\" & System.IO.Directory.GetParent(HazusDirectory).Name & "_logfile.txt")
                e.Cancel = True
            End If
            Dim bndryconn As New OleDbConnection("Provider=Microsoft.Jet.OleDb.4.0;Data Source=" & HazusDirectory & "\bndrygbs.mdb")
            bndryconn.Open()
            If Not File.Exists(HazusDirectory & "\MSH.mdb") Then
                RaiseEvent ReportNSIErrorMessage("HAZUS database file not found:  " & HazusDirectory & "\MSH.mdb", OutputDirectory & "\" & System.IO.Directory.GetParent(HazusDirectory).Name & "_logfile.txt")
                e.Cancel = True
            End If
            Dim MSHconn As New OleDbConnection("Provider=Microsoft.Jet.OleDb.4.0;Data Source=" & HazusDirectory & "\MSH.mdb")
            MSHconn.Open()
            Dim ComputeVehicleValues As Boolean = True
            Dim VehConn As New OleDbConnection
            If Not File.Exists(HazusDirectory & "\flVeh.mdb") Then
                RaiseEvent ReportNSIErrorMessage("HAZUS database file not found:  " & HazusDirectory & "\flVeh.mdb", OutputDirectory & "\" & System.IO.Directory.GetParent(HazusDirectory).Name & "_logfile.txt")
                e.Cancel = True
                ComputeVehicleValues = False
            Else
                VehConn = New OleDbConnection("Provider=Microsoft.Jet.OleDb.4.0;Data Source=" & HazusDirectory & "\flVeh.mdb")
                VehConn.Open()
            End If
            Dim restrictions(4) As String, Index_Check_Table As New DataTable, cmd As OleDbCommand
            restrictions(2) = "CensusBlock"
            Try
                restrictions(4) = "hzBldgCountOccupB"
                Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
                If Index_Check_Table.Rows.Count = 0 Then
                    cmd = New OleDbCommand("Create INDEX CensusBlock ON hzBldgCountOccupB (CensusBlock)", bndryconn)
                    cmd.ExecuteNonQuery()
                End If
                restrictions(4) = "hzDemographicsB"
                Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
                If Index_Check_Table.Rows.Count = 0 Then
                    cmd = New OleDbCommand("Create INDEX CensusBlock ON hzDemographicsB (CensusBlock)", bndryconn)
                    cmd.ExecuteNonQuery()
                End If
                restrictions(4) = "hzCensusBlock"
                Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
                If Index_Check_Table.Rows.Count = 0 Then
                    cmd = New OleDbCommand("Create INDEX CensusBlock ON hzCensusBlock (CensusBlock)", bndryconn)
                    cmd.ExecuteNonQuery()
                End If
                restrictions(4) = "hzExposureOccupB"
                Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
                If Index_Check_Table.Rows.Count = 0 Then
                    cmd = New OleDbCommand("Create INDEX CensusBlock ON hzExposureOccupB (CensusBlock)", bndryconn)
                    cmd.ExecuteNonQuery()
                End If
                restrictions(4) = "hzExposureContentOccupB"
                Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
                If Index_Check_Table.Rows.Count = 0 Then
                    cmd = New OleDbCommand("Create INDEX CensusBlock ON hzExposureContentOccupB (CensusBlock)", bndryconn)
                    cmd.ExecuteNonQuery()
                End If
                restrictions(2) = "Tract"
                restrictions(4) = "hzCensusBlock"
                Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
                If Index_Check_Table.Rows.Count = 0 Then
                    cmd = New OleDbCommand("Create INDEX Tract ON hzCensusBlock (Tract)", bndryconn)
                    cmd.ExecuteNonQuery()
                End If
                '
                restrictions(2) = "CensusBlock"
                restrictions(4) = "flNightVehicleInv"
                Index_Check_Table = VehConn.GetSchema("INDEXES", restrictions)
                If Index_Check_Table.Rows.Count = 0 Then
                    cmd = New OleDbCommand("Create INDEX CensusBlock ON flNightVehicleInv (CensusBlock)", VehConn)
                    cmd.ExecuteNonQuery()
                End If
            Catch
                'MsgBox("Error with indexes")
                RaiseEvent ReportNSIErrorMessage("Error with indexes", OutputDirectory & "\" & System.IO.Directory.GetParent(OutputDirectory).Name & "_logfile.txt")
                e.Cancel = True
            End Try
            Dim countyNames As New List(Of String)
            Dim countyFips As New List(Of String)

            Using Command As OleDbCommand = New OleDbCommand("select CountyFips,CountyName from hzCounty", bndryconn)
                Using CountyReader As OleDbDataReader = Command.ExecuteReader
                    If CountyReader.HasRows Then
                        While CountyReader.Read
                            countyFips.Add(CountyReader.Item(0).ToString)
                            countyNames.Add(CountyReader.Item(1).ToString)

                        End While
                    End If
                End Using
            End Using
            'create a structure inventory for each county
            For i = 0 To countyFips.Count - 1
                Try
                    If CreateCountySI(countyFips(i), countyNames(i), bndryconn, MSHconn, VehConn, OutputDirectory & "\" & countyFips(i) & ".shp") Then
                        RaiseEvent ReportNSIErrorMessage("County successful", OutputDirectory & "\" & countyFips(i) & "_logfile.txt")
                        _bw.ReportProgress(0, "County " & countyNames(i) & " is complete")
                        RaiseEvent ReportNSIErrorMessage("County " & countyNames(i) & " was successful.", OutputDirectory & "\" & Left(countyFips(i), 2) & "_logfile.txt")
                    Else
                        _bw.ReportProgress(0, "County " & countyNames(i) & " failed")
                        RaiseEvent ReportNSIErrorMessage("County did not succeed", OutputDirectory & "\" & countyFips(i) & "_logfile.txt")
                        RaiseEvent ReportNSIErrorMessage("County " & countyNames(i) & " did not succeed.", OutputDirectory & "\" & Left(countyFips(i), 2) & "_logfile.txt")
                    End If
                Catch ex As Exception
                    'write out a state level error file.
                    RaiseEvent ReportNSIErrorMessage("State issue creating inventory for county " & countyFips(i) & ", " & countyNames(i) & ".", OutputDirectory & "\" & Left(countyFips(i), 2) & "_logfile.txt")
                    MsgBox(ex.Message)
                End Try
            Next

            e.Cancel = False
        Next

    End Sub
    Private Function CreateCountySI(ByVal CountyFips As String, ByVal countyName As String, ByVal bndryconn As OleDbConnection, ByVal MSHConn As OleDbConnection, ByVal VehConn As OleDbConnection, ByVal outfilename As String) As Boolean
        _structureInventoryname = System.IO.Directory.GetParent(outfilename).FullName & "\" & System.IO.Path.GetFileNameWithoutExtension(outfilename) & "_logfile.txt" 'System.IO.Path.ChangeExtension(outfilename, ".txt")
        Dim cmd As OleDbCommand
        'Dim Tree As New RTree.RTree(Of Int32)
        Dim Tracts As New List(Of String)
        Dim Extent(3) As Double
        '
        Using Command As OleDbCommand = New OleDbCommand("select Tract from hzTract WHERE CountyFips = '" & CountyFips & "'", bndryconn)
            Using TractReader As OleDbDataReader = Command.ExecuteReader
                If TractReader.HasRows Then
                    While TractReader.Read
                        Tracts.Add(TractReader.Item(0).ToString)
                    End While
                End If
            End Using
        End Using
        '
        Dim CensusBlockFeature As PolygonFeature
        'Dim CensusBlockFeatures As New PolygonFeatures
        '
        Dim CBShape() As Byte
        Dim Nparts As Int32, Npoints As Int32
        Dim parts() As Int32, PartLength As Int32, part() As Double
        '
        Dim CensusBlocks As New Dictionary(Of String, LifeSimGIS.PolygonFeature)
        'Dim DummyIntersectingFeature As PolygonFeature = Nothing
        For Each Tract As String In Tracts
            Using Command As OleDbCommand = New OleDbCommand("SELECT SHAPE,CensusBlock from hzCensusBlock WHERE Tract = '" & Tract & "'", bndryconn)
                Using CBReader As OleDbDataReader = Command.ExecuteReader
                    If CBReader.HasRows Then
                        While CBReader.Read
                            CBShape = CType(CBReader.Item(0), Byte())
                            CensusBlockFeature = New PolygonFeature
                            Buffer.BlockCopy(CBShape, 4, Extent, 0, 32)
                            CensusBlockFeature.Extent = New Extent(Extent(2), Extent(0), Extent(3), Extent(1))
                            '
                            Nparts = BitConverter.ToInt32(CBShape, 36) : ReDim parts(Nparts - 1)
                            Npoints = BitConverter.ToInt32(CBShape, 40)
                            Buffer.BlockCopy(CBShape, 44, parts, 0, 4 * Nparts) 'this defines the location in the point array where each part begins

                            For partcount As Int32 = 0 To parts.Count - 1
                                'get the part
                                If partcount = parts.Count - 1 Then
                                    PartLength = (Npoints - parts(partcount)) * 2 : ReDim part(PartLength - 1)
                                Else
                                    PartLength = (parts(partcount + 1) - parts(partcount)) * 2 : ReDim part(PartLength - 1)
                                End If
                                Buffer.BlockCopy(CBShape, 44 + 4 * Nparts + parts(partcount) * 16, part, 0, PartLength * 8) '_ShapefileReader.ReadBytes(8 * PartLength), 0, part, 0, 8 * PartLength)
                                '
                                CensusBlockFeature.PolygonFeature.Add(New LifeSimGIS.Polygon(part))
                            Next
                            CensusBlocks.Add(CBReader.Item(1).ToString, CensusBlockFeature)

                        End While
                    End If
                End Using
            End Using
        Next
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Create Structure Inventory.
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Dimension the variables required for generating the structure inventory
        Dim SI_Points As New List(Of PointD)
        Dim HZBldgCountRec, HZCensusRec As OleDbDataReader
        Dim StructureTotal As Int32
        Dim PctElders As Double
        Dim HazusPopulations(9) As Int32
        '
        Dim UnassignedDayPopU65, UnassignedDayPopO65, UnassignedNightPopU65, UnassignedNightPopO65 As Int32
        Dim HAZUS_Structures As List(Of HazusStructureAttributes)
        Dim AverageVehicleValue As Single
        'Used for redistributing excess population
        Dim TotalHouseholds As Double
        Dim HouseHoldWeights(0) As Double
        Dim PopulationDistribution() As Int32
        '
        Dim dt As New DataTable
        dt.Columns.Add("St_Name", GetType(String))
        dt.Columns.Add("CBFips", GetType(String))
        dt.Columns.Add("DamCat", GetType(String))
        dt.Columns.Add("OccType", GetType(String))
        dt.Columns.Add("N_Stories", GetType(Int16))
        dt.Columns.Add("Basement", GetType(String))
        dt.Columns.Add("BldgType", GetType(String))
        dt.Columns.Add("Found_Ht", GetType(Single))
        'dt.Columns.Add("Pop2amU65", GetType(Int32))
        'dt.Columns.Add("Pop2amO65", GetType(Int32))
        'dt.Columns.Add("Pop2pmU65", GetType(Int32))
        'dt.Columns.Add("Pop2pmO65", GetType(Int32))
        dt.Columns.Add("Val_Struct", GetType(Single))
        dt.Columns.Add("Val_Cont", GetType(Single))
        dt.Columns.Add("Val_Other", GetType(Single))
        dt.Columns.Add("Val_Vehic", GetType(Single))
        dt.Columns.Add("MedYrBlt", GetType(Int32))
        dt.Columns.Add("FipsEntry", GetType(Int32))
        dt.Columns.Add("Found_Type", GetType(String))
        dt.Columns.Add("PostFirm", GetType(Byte))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Loop through each census block and generate stuctures with required attributes.
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim i As Int32 = 0
        Dim counter As Integer = 0
        For Each cb As KeyValuePair(Of String, LifeSimGIS.PolygonFeature) In CensusBlocks

            If i Mod (100) = 0 Then
                If i = 0 Then
                    _bw.ReportProgress((i / CensusBlocks.Count) * 100, "Censusblock " & i + 1 & " of " & CensusBlocks.Count & " - " & countyName)
                Else
                    _bw.ReportProgress((i / CensusBlocks.Count) * 100, "Censusblock " & i & " of " & CensusBlocks.Count & " - " & countyName)
                End If

                RaiseEvent ReportProgress(i / CensusBlocks.Count)
            End If
            Try
                'First gather structure information
                cmd = New OleDbCommand("SELECT * from hzBldgCountOccupB WHERE CensusBlock = '" & cb.Key & "'", bndryconn)
                HZBldgCountRec = cmd.ExecuteReader
                If HZBldgCountRec.HasRows Then
                    HZBldgCountRec.Read()

                    'calculate the total number of structures
                    StructureTotal = 0
                    For j = 2 To HZBldgCountRec.FieldCount - 1
                        StructureTotal += CInt(HZBldgCountRec.Item(j))
                    Next
                    'RaiseEvent ReportNSIErrorMessage("CensusBlock: " & cb.Key & " has " & StructureTotal & " Structures in hzBldngCountOccupB.", _structureInventoryname)
                    If StructureTotal > 0 Then
                        'Reset
                        UnassignedDayPopU65 = 0
                        UnassignedDayPopO65 = 0
                        UnassignedNightPopU65 = 0
                        UnassignedNightPopO65 = 0
                        'Get Percentage of population that is elders
                        PctElders = GetPctElders(bndryconn, cb.Key)
                        '
                        'For HazusPopulations indexes (Population, ResidDay, ResidNight, Hotel, Visitor, WorkingCom, WorkingInd, Commuting5PM, SchoolEnrollmentKto12, SchoolEnrollmentCollege)
                        HazusPopulations = GetHazusPopulations(bndryconn, cb.Key)
                        'Get Census Block reader for this census block
                        Using Command As OleDbCommand = New OleDbCommand("SELECT BldgSchemesId, BlockType, PctWithBasemnt, Pct1StoryRes1, Pct2StoryRes1, Pct3StoryRes1, PctSplitLvlRes1, Pct1to2StryRes3, Pct3to4StryRes3, Pct5StryplusRes3 from hzCensusBlock WHERE CensusBlock = '" & cb.Key & "'", bndryconn)
                            HZCensusRec = Command.ExecuteReader
                            If HZCensusRec.HasRows Then
                                HZCensusRec.Read()
                            Else
                                Continue For
                            End If
                        End Using
                        'blocktype
                        'block type
                        Dim blocktype As String = HZCensusRec.Item("BlockType")

                        Dim medianyear As Integer = GetMedianYear(bndryconn, cb.Key)
                        Dim sIDandEY As SchemeIDAndEntryYear = GetFirmEntryYear(MSHConn, cb.Key)
                        Dim FipEntry As Integer = sIDandEY.EntryYear
                        Dim SchemeID As String = sIDandEY.SchemeID
                        Dim prefirm As Byte = 0
                        If medianyear >= FipEntry Then prefirm = 1
                        If SchemeID.Substring(2, 1) <> blocktype Then
                            SchemeID = SchemeID.Remove(2, 1).Insert(2, blocktype)
                        End If

                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Residential Structures and populate them
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        HAZUS_Structures = GetResidentialStructures(cb.Key, HazusPopulations, PctElders, blocktype, prefirm, SchemeID, HZBldgCountRec, HZCensusRec, MSHConn, bndryconn)
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Test for Unassigned population
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        If HAZUS_Structures.Count = 0 Then
                            If HazusPopulations(1) > 0 Or HazusPopulations(2) > 0 Then
                                UnassignedNightPopU65 = CInt(Math.Round(0.99 * HazusPopulations(2) * (1 - PctElders), 0)) 'ResidentialUnder65
                                UnassignedNightPopO65 = CInt(Math.Round(0.99 * HazusPopulations(2) * PctElders, 0)) 'ResidentialOver65
                                '
                                UnassignedDayPopU65 = CInt(Math.Round(0.75 * HazusPopulations(1) * (1 - PctElders), 0)) 'ResidentialUnder65
                                UnassignedDayPopO65 = CInt(Math.Round(0.75 * HazusPopulations(1) * PctElders, 0)) 'ResidentialOver65
                                'RaiseEvent ReportNSIErrorMessage("Unassigned Population in census block " & cb.Key & ": ResNightU65: " & UnassignedNightPopU65 & " ResNightO65: " & UnassignedNightPopO65 & " ResDaytU65: " & UnassignedDayPopU65 & " ResNightO65: " & UnassignedDayPopO65, _structureInventoryname)
                                'Debug.Print("Unassigned Population: ResNightU65: " & UnassignedNightPopU65 & " ResNightO65: " & UnassignedNightPopO65 & " ResDaytU65: " & UnassignedDayPopU65 & " ResNightO65: " & UnassignedDayPopO65)
                            End If
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Commercial Structures and populate them
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Dim CommercialStructures As List(Of HazusStructureAttributes) = GetCommercialStructures(cb.Key, HazusPopulations, PctElders, blocktype, prefirm, SchemeID, HZBldgCountRec, HZCensusRec, MSHConn, bndryconn)
                        If CommercialStructures.Count = 0 Then
                            If HazusPopulations(1) > 0 Or HazusPopulations(3) > 0 Or HazusPopulations(4) > 0 Or HazusPopulations(5) > 0 Then
                                UnassignedNightPopU65 += CInt(Math.Round(0.02 * HazusPopulations(5), 0)) 'Under65
                                UnassignedDayPopU65 += CInt(Math.Round(0.98 * HazusPopulations(5) + 0.2 * HazusPopulations(1) * (1 - PctElders) + 0.8 * HazusPopulations(3) + HazusPopulations(4), 0)) 'Under65
                                UnassignedDayPopO65 += CInt(Math.Round(0.2 * HazusPopulations(1) * PctElders, 0)) 'Over65
                            End If
                        Else
                            HAZUS_Structures.AddRange(CommercialStructures)
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Industrial Structures and populate them
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Dim IndustrialStructures As List(Of HazusStructureAttributes) = GetIndustrialStructures(cb.Key, HazusPopulations, PctElders, blocktype, prefirm, SchemeID, HZBldgCountRec, HZCensusRec, MSHConn, bndryconn)
                        If IndustrialStructures.Count = 0 Then
                            If HazusPopulations(6) > 0 Then
                                UnassignedNightPopU65 += CInt(Math.Round(0.1 * HazusPopulations(6), 0)) 'Under65
                                UnassignedDayPopU65 += CInt(Math.Round(0.8 * HazusPopulations(6), 0)) 'Under65
                            End If
                        Else
                            HAZUS_Structures.AddRange(IndustrialStructures)
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Educational Structures and populate them
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Dim EducationalStructures As List(Of HazusStructureAttributes) = GetEducationalStructures(cb.Key, HazusPopulations, PctElders, blocktype, prefirm, SchemeID, HZBldgCountRec, HZCensusRec, MSHConn, bndryconn)
                        If EducationalStructures.Count = 0 Then
                            If HazusPopulations(8) > 0 Or HazusPopulations(9) > 0 Then
                                UnassignedDayPopU65 += CInt(Math.Round(0.8 * HazusPopulations(8) + HazusPopulations(9), 0)) 'Under65
                            End If
                        Else
                            HAZUS_Structures.AddRange(EducationalStructures)
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Hotel Structures and populate them
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Dim HotelStructures As List(Of HazusStructureAttributes) = GetHotelStructures(cb.Key, HazusPopulations, PctElders, blocktype, prefirm, SchemeID, HZBldgCountRec, HZCensusRec, MSHConn, bndryconn)
                        If HotelStructures.Count = 0 Then
                            If HazusPopulations(3) > 0 Then
                                UnassignedDayPopU65 += CInt(Math.Round(0.2 * HazusPopulations(3), 0)) 'Under65
                                UnassignedNightPopU65 += CInt(Math.Round(HazusPopulations(3), 0)) 'Under65
                            End If
                        Else
                            HAZUS_Structures.AddRange(HotelStructures)
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Assign Any Excess Population
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        TotalHouseholds = 0
                        ReDim HouseHoldWeights(HAZUS_Structures.Count - 1)
                        'Unassigned daytime population
                        If UnassignedDayPopO65 > 0 Or UnassignedDayPopU65 > 0 Then
                            If HAZUS_Structures.Count = 0 Then RaiseEvent ReportNSIErrorMessage("CensusBlock: " & cb.Key & "There are no structures, and unassigned population.  PopDayU65: " & UnassignedDayPopU65 & " PopDayO65: " & UnassignedDayPopO65, _structureInventoryname)
                            For Each HazusStructure As HazusStructureAttributes In HAZUS_Structures
                                TotalHouseholds += HazusStructure.HouseHoldsDay
                            Next
                            'if there arent any structures, this doesnt do anything

                            For j As Int32 = 0 To HAZUS_Structures.Count - 1
                                HouseHoldWeights(j) = HAZUS_Structures(j).HouseHoldsDay / TotalHouseholds
                            Next
                            '
                            If UnassignedDayPopO65 > 0 Then
                                PopulationDistribution = ValueArray(HouseHoldWeights, UnassignedDayPopO65)
                                For j As Int32 = 0 To HouseHoldWeights.Count - 1
                                    HAZUS_Structures(j).pop2pmo65 += PopulationDistribution(j)
                                Next
                            End If
                            If UnassignedDayPopU65 > 0 Then
                                PopulationDistribution = ValueArray(HouseHoldWeights, UnassignedDayPopU65)
                                For j As Int32 = 0 To HouseHoldWeights.Count - 1
                                    HAZUS_Structures(j).pop2pmu65 += PopulationDistribution(j)
                                Next
                            End If
                        End If
                        'Unassigned nighttime population
                        TotalHouseholds = 0
                        If UnassignedNightPopO65 > 0 Or UnassignedNightPopU65 > 0 Then
                            If HAZUS_Structures.Count = 0 Then RaiseEvent ReportNSIErrorMessage("CensusBlock: " & cb.Key & "There are no structures, and unassigned population.  PopNightU65: " & UnassignedNightPopU65 & " PopNightO65: " & UnassignedNightPopO65, _structureInventoryname)
                            For Each HazusStructure As HazusStructureAttributes In HAZUS_Structures
                                TotalHouseholds += HazusStructure.HouseHoldsNight
                            Next
                            For j As Int32 = 0 To HAZUS_Structures.Count - 1
                                HouseHoldWeights(j) = HAZUS_Structures(j).HouseHoldsNight / TotalHouseholds
                            Next
                            '
                            If UnassignedNightPopO65 > 0 Then
                                PopulationDistribution = ValueArray(HouseHoldWeights, UnassignedNightPopO65)
                                For j As Int32 = 0 To HouseHoldWeights.Count - 1
                                    HAZUS_Structures(j).pop2amo65 += PopulationDistribution(j)
                                Next
                            End If
                            If UnassignedNightPopU65 > 0 Then
                                PopulationDistribution = ValueArray(HouseHoldWeights, UnassignedNightPopU65)
                                For j As Int32 = 0 To HouseHoldWeights.Count - 1
                                    HAZUS_Structures(j).pop2amu65 += PopulationDistribution(j)
                                Next
                            End If
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Vehicle Values and Assing Foundation Heights
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        ' If ComputeVehicleValues = True Then
                        Using NightVehiclesCommand As OleDbCommand = New OleDbCommand("SELECT TotalExp FROM flNightVehicleInv WHERE CensusBlock = '" & cb.Key & "'", VehConn)
                            Using NightVehicleReader As OleDbDataReader = NightVehiclesCommand.ExecuteReader
                                If NightVehicleReader.HasRows Then NightVehicleReader.Read()
                                AverageVehicleValue = CSng(NightVehicleReader.GetDouble(0) / HAZUS_Structures.Count)
                                System.Threading.Tasks.Parallel.ForEach(HAZUS_Structures, Sub(HAZUSStructure)
                                                                                              HAZUSStructure.vehicval = AverageVehicleValue
                                                                                          End Sub)
                            End Using
                        End Using
                        'End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Store census block data for writing out at the end.
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Dim structurecount As Integer = HAZUS_Structures.Count
                        Dim successes As Integer = 0

                        Try
                            SI_Points.AddRange(GetMultiPolyPointsTesselate(cb.Value, HAZUS_Structures.Count))
                            'update datatable for dbf
                            For Each HAZUS_Structure As HazusStructureAttributes In HAZUS_Structures
                                With HAZUS_Structure
                                    .Name = .OccType & " " & CountyFips & " " & counter.ToString("D8")
                                    dt.Rows.Add({.Name, cb.Key, .DamCat, .OccType, .Stories, .basement, .bldgtype, .foundationheight, .StructVal, .contentval, 0, .vehicval, medianyear, FipEntry, .FoundationType, prefirm})
                                End With
                                counter += 1
                            Next
                        Catch ex As Exception
                            'place missing points into the point shapefile...
                            RaiseEvent ReportNSIErrorMessage("There were " & successes & " successful points, and " & structurecount - successes & " unsuccessful points in the census block " & cb.Key & "." & vbNewLine & "Attempting alternative structure placement methodology", _structureInventoryname)
                            ' Debug.Print(i.ToString & ", There were " & successes & " successful points, and " & structurecount - successes & " unsuccessful points in the census block " & IntersectingCensusBlocks(i) & "." & vbNewLine & "Attempting alternative structure placement methodology")
                            If successes = 0 Then
                                Dim GridPoints As List(Of PointD) = GetPointsGridCells(cb.Value, HAZUS_Structures.Count)
                                If GridPoints.Count <> HAZUS_Structures.Count Then
                                    RaiseEvent ReportNSIErrorMessage("Alternative placement failed in the census block " & cb.Key & ".", _structureInventoryname)
                                Else
                                    SI_Points.AddRange(GridPoints)
                                    For Each HAZUS_Structure As HazusStructureAttributes In HAZUS_Structures
                                        With HAZUS_Structure
                                            .Name = .OccType & " " & CountyFips & " " & counter.ToString("D8")
                                            dt.Rows.Add({.Name, cb.Key, .DamCat, .OccType, .Stories, .basement, .bldgtype, .foundationheight, .StructVal, .contentval, 0, .vehicval, medianyear, FipEntry, .FoundationType, prefirm})
                                        End With
                                        counter += 1
                                    Next
                                    RaiseEvent ReportNSIErrorMessage("Alternative placement methodology Successful", _structureInventoryname)
                                    'Debug.Print("Alternative placement methodology Successful")
                                End If
                            Else
                                'some structures already placed, so dont re place them... 
                                RaiseEvent ReportNSIErrorMessage("This Should never happen. " & "Census Block: " & cb.Key, _structureInventoryname)
                            End If



                        End Try
                        'RaiseEvent ReportNSIErrorMessage("CensusBlock: " & cb.Key & " had " & HAZUS_Structures.Count & " structures placed", _structureInventoryname)
                    Else
                        'structure total was zero or less.
                        'RaiseEvent ReportNSIErrorMessage("CensusBlock: " & cb.Key & " had 0 structures placed", _structureInventoryname)
                    End If
                Else
                    'hazusreader didnt have rows
                    'RaiseEvent ReportNSIErrorMessage("CensusBlock: " & cb.Key & " had 0 structures placed", _structureInventoryname)
                End If
                HZBldgCountRec.Close()
                If i Mod 1000 = 0 Then System.GC.Collect()
            Catch ex As Exception
                RaiseEvent ReportNSIErrorMessage("CensusBlock: " & cb.Key & " had the following exeption " & vbNewLine & ex.ToString, _structureInventoryname)
                'Debug.Print(i.ToString)
            End Try

            i += 1
        Next

        'bndryconn.Close() : bndryconn.Dispose()
        Dim HazusProjection As New EPSGProjection(4269)

        Dim StructurePoints As New PointFeatures(SI_Points.ToArray)
        Dim NewPntShape As New ShapefileWriter(outfilename)
        Try
            NewPntShape.WriteFeatures(StructurePoints, dt, HazusProjection)
        Catch ex As Exception
            RaiseEvent ReportNSIErrorMessage("Error Creating Shapefile" & vbNewLine & ex.Message & vbNewLine & "There were " & StructurePoints.Points.Count & " points, and " & dt.Rows.Count & " data rows", _structureInventoryname)
            MsgBox(ex.Message)
            Return False
        End Try
        Return True
    End Function
    Private Function CreateSI(ByVal PolygonShapefile As String, ByVal BndryGbs As String, ByVal MSH As String, ByVal FlVeh As String, ByVal OutFilename As String) As DataTable
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Set up the hazus database readers.
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        _bw.ReportProgress(0, "Checking Databases")
        If Not File.Exists(BndryGbs) Then
            MsgBox("HAZUS database file not found:  " & BndryGbs, MsgBoxStyle.Critical, "Error Locating File bndrygbs.mdb")
            Return Nothing
        End If
        Dim bndryconn As New OleDbConnection("Provider=Microsoft.Jet.OleDb.4.0;Data Source=" & BndryGbs)
        bndryconn.Open()
        If Not File.Exists(MSH) Then
            MsgBox("HAZUS database file not found:  " & MSH, MsgBoxStyle.Critical, "Error Locating File MSH.mdb")
            Return Nothing
        End If
        Dim MSHconn As New OleDbConnection("Provider=Microsoft.Jet.OleDb.4.0;Data Source=" & MSH)
        MSHconn.Open()
        Dim ComputeVehicleValues As Boolean = True
        Dim VehConn As New OleDbConnection
        If Not File.Exists(FlVeh) Then
            ComputeVehicleValues = False
        Else
            VehConn = New OleDbConnection("Provider=Microsoft.Jet.OleDb.4.0;Data Source=" & FlVeh)
            VehConn.Open()
        End If
        'Create Indexes if they don't already exist
        Dim restrictions(4) As String, Index_Check_Table As New DataTable, cmd As OleDbCommand
        restrictions(2) = "CensusBlock"
        Try
            restrictions(4) = "hzBldgCountOccupB"
            Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
            If Index_Check_Table.Rows.Count = 0 Then
                cmd = New OleDbCommand("Create INDEX CensusBlock ON hzBldgCountOccupB (CensusBlock)", bndryconn)
                cmd.ExecuteNonQuery()
            End If
            restrictions(4) = "hzDemographicsB"
            Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
            If Index_Check_Table.Rows.Count = 0 Then
                cmd = New OleDbCommand("Create INDEX CensusBlock ON hzDemographicsB (CensusBlock)", bndryconn)
                cmd.ExecuteNonQuery()
            End If
            restrictions(4) = "hzCensusBlock"
            Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
            If Index_Check_Table.Rows.Count = 0 Then
                cmd = New OleDbCommand("Create INDEX CensusBlock ON hzCensusBlock (CensusBlock)", bndryconn)
                cmd.ExecuteNonQuery()
            End If
            restrictions(4) = "hzExposureOccupB"
            Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
            If Index_Check_Table.Rows.Count = 0 Then
                cmd = New OleDbCommand("Create INDEX CensusBlock ON hzExposureOccupB (CensusBlock)", bndryconn)
                cmd.ExecuteNonQuery()
            End If
            restrictions(4) = "hzExposureContentOccupB"
            Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
            If Index_Check_Table.Rows.Count = 0 Then
                cmd = New OleDbCommand("Create INDEX CensusBlock ON hzExposureContentOccupB (CensusBlock)", bndryconn)
                cmd.ExecuteNonQuery()
            End If
            restrictions(2) = "Tract"
            restrictions(4) = "hzCensusBlock"
            Index_Check_Table = bndryconn.GetSchema("INDEXES", restrictions)
            If Index_Check_Table.Rows.Count = 0 Then
                cmd = New OleDbCommand("Create INDEX Tract ON hzCensusBlock (Tract)", bndryconn)
                cmd.ExecuteNonQuery()
            End If
            '
            restrictions(2) = "CensusBlock"
            restrictions(4) = "flNightVehicleInv"
            Index_Check_Table = VehConn.GetSchema("INDEXES", restrictions)
            If Index_Check_Table.Rows.Count = 0 Then
                cmd = New OleDbCommand("Create INDEX CensusBlock ON flNightVehicleInv (CensusBlock)", VehConn)
                cmd.ExecuteNonQuery()
            End If
        Catch
            'MsgBox("Error with indexes")
            RaiseEvent ReportErrorMessage("Error with indexes", _structureInventoryname)
            Return Nothing
        End Try
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Prepare study area shapefile.
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        _bw.ReportProgress(0, "Determining Census Blocks")
        Dim StudyAreaShapeReader As New ShapefileReader(PolygonShapefile)
        'Check shape type
        Dim lngShapeType As Int32 = StudyAreaShapeReader.ShapeType
        If lngShapeType = 5 Or lngShapeType = 15 Or lngShapeType = 25 Then
        Else
            MsgBox("shapefile is not a polygon shapefile.")
            Return Nothing
        End If
        Dim StudyAreaProjection As Projection
        If File.Exists(Path.ChangeExtension(PolygonShapefile, ".prj")) Then
            StudyAreaProjection = New GDALAssist.ESRIProjection(Path.ChangeExtension(PolygonShapefile, ".prj"))
        Else
            MsgBox("Bounding polygon shapefile does not have a projection and must contain projection information.")
            Return Nothing
        End If
        'Reproject study area polygon
        Dim StudyAreaPolygons As PolygonFeatures = CType(StudyAreaShapeReader.ToFeatures, PolygonFeatures)
        Dim HazusProjection As New EPSGProjection(4269)
        If StudyAreaProjection.IsValid <> GDALAssist.SRSValidation.Corrupt Then
            If StudyAreaProjection.IsEqual(HazusProjection) = False Then
                StudyAreaPolygons.Reproject(StudyAreaProjection, HazusProjection)
            End If
        End If
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Limit the search to census tracts that have an overlapping extent.
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim Tree As New RTree.RTree(Of Int32)
        Dim OverLappingTracts As New List(Of String)
        Dim Extent(3) As Double
        '
        Using Command As OleDbCommand = New OleDbCommand("select SHAPE,Tract from hzTract", bndryconn)
            Using TractReader As OleDbDataReader = Command.ExecuteReader
                If TractReader.HasRows Then
                    While TractReader.Read
                        Buffer.BlockCopy(CType(TractReader.Item(0), Byte()), 4, Extent, 0, 32)
                        For Each poly As PolygonFeature In StudyAreaPolygons.Polygons
                            If poly.Extent.Overlaps(Extent(0), Extent(2), Extent(1), Extent(3)) Then
                                OverLappingTracts.Add(TractReader.Item(1).ToString)
                                Exit For
                            End If
                        Next
                    End While
                End If
            End Using
        End Using
        '
        Dim CensusBlockFeature As PolygonFeature
        'Dim CensusBlockFeatures As New PolygonFeatures
        '
        Dim CBShape() As Byte
        Dim Nparts As Int32, Npoints As Int32
        Dim parts() As Int32, PartLength As Int32, part() As Double
        '
        Dim IntersectingCensusBlocks As New Dictionary(Of String, LifeSimGIS.PolygonFeature)
        Dim DummyIntersectingFeature As PolygonFeature = Nothing
        For Each OverlappingTract As String In OverLappingTracts
            Using Command As OleDbCommand = New OleDbCommand("SELECT SHAPE,CensusBlock from hzCensusBlock WHERE Tract = '" & OverlappingTract & "'", bndryconn)
                Using CBReader As OleDbDataReader = Command.ExecuteReader
                    If CBReader.HasRows Then
                        While CBReader.Read
                            CBShape = CType(CBReader.Item(0), Byte())
                            CensusBlockFeature = New PolygonFeature
                            Buffer.BlockCopy(CBShape, 4, Extent, 0, 32)
                            CensusBlockFeature.Extent = New Extent(Extent(2), Extent(0), Extent(3), Extent(1))
                            Dim IntersectsFeature As Boolean = False
                            For Each poly As PolygonFeature In StudyAreaPolygons.Polygons
                                If poly.Extent.Overlaps(Extent(0), Extent(2), Extent(1), Extent(3)) Then
                                    IntersectsFeature = True
                                    Exit For
                                End If
                            Next
                            If IntersectsFeature = False Then Continue While
                            '
                            Nparts = BitConverter.ToInt32(CBShape, 36) : ReDim parts(Nparts - 1)
                            Npoints = BitConverter.ToInt32(CBShape, 40)
                            Buffer.BlockCopy(CBShape, 44, parts, 0, 4 * Nparts) 'this defines the location in the point array where each part begins

                            For partcount As Int32 = 0 To parts.Count - 1
                                'get the part
                                If partcount = parts.Count - 1 Then
                                    PartLength = (Npoints - parts(partcount)) * 2 : ReDim part(PartLength - 1)
                                Else
                                    PartLength = (parts(partcount + 1) - parts(partcount)) * 2 : ReDim part(PartLength - 1)
                                End If
                                Buffer.BlockCopy(CBShape, 44 + 4 * Nparts + parts(partcount) * 16, part, 0, PartLength * 8) '_ShapefileReader.ReadBytes(8 * PartLength), 0, part, 0, 8 * PartLength)
                                '
                                CensusBlockFeature.PolygonFeature.Add(New LifeSimGIS.Polygon(part))
                            Next
                            '
                            For Each poly As PolygonFeature In StudyAreaPolygons.Polygons
                                If poly.IntersectsFeature(CensusBlockFeature, DummyIntersectingFeature) Then
                                    If IntersectingCensusBlocks.ContainsKey(CBReader.Item(1).ToString) = False Then IntersectingCensusBlocks.Add(CBReader.Item(1).ToString, CensusBlockFeature)
                                End If
                            Next
                        End While
                    End If
                End Using
            End Using
        Next
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Create Structure Inventory.
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Dimension the variables required for generating the structure inventory
        Dim SI_Points As New List(Of PointD)
        Dim HZBldgCountRec, HZCensusRec As OleDbDataReader
        Dim StructureTotal As Int32
        Dim PctElders As Double
        Dim HazusPopulations(9) As Int32
        '
        Dim UnassignedDayPopU65, UnassignedDayPopO65, UnassignedNightPopU65, UnassignedNightPopO65 As Int32
        Dim HAZUS_Structures As List(Of HazusStructureAttributes)
        Dim AverageVehicleValue As Single
        'Used for redistributing excess population
        Dim TotalHouseholds As Double
        Dim HouseHoldWeights(0) As Double
        Dim PopulationDistribution() As Int32
        '
        Dim dt As New DataTable
        dt.Columns.Add("St_Name", GetType(String))
        dt.Columns.Add("DamCat", GetType(String))
        dt.Columns.Add("OccType", GetType(String))
        dt.Columns.Add("N_Stories", GetType(Int16))
        dt.Columns.Add("Basement", GetType(String))
        dt.Columns.Add("BldgType", GetType(String))
        dt.Columns.Add("Found_Ht", GetType(Single))
        dt.Columns.Add("Pop2amU65", GetType(Int32))
        dt.Columns.Add("Pop2amO65", GetType(Int32))
        dt.Columns.Add("Pop2pmU65", GetType(Int32))
        dt.Columns.Add("Pop2pmO65", GetType(Int32))
        dt.Columns.Add("Val_Struct", GetType(Single))
        dt.Columns.Add("Val_Cont", GetType(Single))
        dt.Columns.Add("Val_Other", GetType(Single))
        dt.Columns.Add("Val_Vehic", GetType(Single))
        dt.Columns.Add("MedYrBlt", GetType(Int32))
        dt.Columns.Add("FipsEntry", GetType(Int32))
        dt.Columns.Add("Found_Type", GetType(String))
        dt.Columns.Add("PostFirm", GetType(Byte))
        dt.Columns.Add("FFE", GetType(Single))
        dt.Columns.Add("Ground_Ht", GetType(Single))
        dt.Columns.Add("UseFFE", GetType(Boolean))
        dt.Columns.Add("UseDBF_GE", GetType(Boolean))
        dt.Columns.Add("Mod_Name", GetType(String))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Loop through each census block and generate stuctures with required attributes.
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim i As Int32 = 0
        For Each cb As KeyValuePair(Of String, LifeSimGIS.PolygonFeature) In IntersectingCensusBlocks

            If i Mod (100) = 0 Then
                If i = 0 Then
                    _bw.ReportProgress((i / IntersectingCensusBlocks.Count) * 100, "Censusblock " & i + 1 & " of " & IntersectingCensusBlocks.Count)
                Else
                    _bw.ReportProgress((i / IntersectingCensusBlocks.Count) * 100, "Censusblock " & i & " of " & IntersectingCensusBlocks.Count)
                End If

                RaiseEvent ReportProgress(i / IntersectingCensusBlocks.Count)
            End If
            Try
                'First gather structure information
                cmd = New OleDbCommand("SELECT * from hzBldgCountOccupB WHERE CensusBlock = '" & cb.Key & "'", bndryconn)
                HZBldgCountRec = cmd.ExecuteReader
                If HZBldgCountRec.HasRows Then
                    HZBldgCountRec.Read()

                    'calculate the total number of structures
                    StructureTotal = 0
                    For j = 2 To HZBldgCountRec.FieldCount - 1
                        StructureTotal += CInt(HZBldgCountRec.Item(j))
                    Next
                    If StructureTotal > 0 Then
                        'Reset
                        UnassignedDayPopU65 = 0
                        UnassignedDayPopO65 = 0
                        UnassignedNightPopU65 = 0
                        UnassignedNightPopO65 = 0
                        'Get Percentage of population that is elders
                        PctElders = GetPctElders(bndryconn, cb.Key)
                        '
                        'For HazusPopulations indexes (Population, ResidDay, ResidNight, Hotel, Visitor, WorkingCom, WorkingInd, Commuting5PM, SchoolEnrollmentKto12, SchoolEnrollmentCollege)
                        HazusPopulations = GetHazusPopulations(bndryconn, cb.Key)
                        'Get Census Block reader for this census block
                        Using Command As OleDbCommand = New OleDbCommand("SELECT BldgSchemesId, BlockType, PctWithBasemnt, Pct1StoryRes1, Pct2StoryRes1, Pct3StoryRes1, PctSplitLvlRes1, Pct1to2StryRes3, Pct3to4StryRes3, Pct5StryplusRes3 from hzCensusBlock WHERE CensusBlock = '" & cb.Key & "'", bndryconn)
                            HZCensusRec = Command.ExecuteReader
                            If HZCensusRec.HasRows Then
                                HZCensusRec.Read()
                            Else
                                Continue For
                            End If
                        End Using
                        'blocktype
                        'block type
                        Dim blocktype As String = HZCensusRec.Item("BlockType")

                        Dim medianyear As Integer = GetMedianYear(bndryconn, cb.Key)
                        Dim sIDandEY As SchemeIDAndEntryYear = GetFirmEntryYear(MSHconn, cb.Key)
                        Dim FipEntry As Integer = sIDandEY.EntryYear
                        Dim SchemeID As String = sIDandEY.SchemeID
                        If SchemeID.Substring(2, 1) <> blocktype Then
                            SchemeID = SchemeID.Remove(2, 1).Insert(2, blocktype)
                        End If

                        Dim prefirm As Byte = 0
                        If medianyear >= FipEntry Then prefirm = 1

                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Residential Structures and populate them
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        HAZUS_Structures = GetResidentialStructures(cb.Key, HazusPopulations, PctElders, blocktype, prefirm, SchemeID, HZBldgCountRec, HZCensusRec, MSHconn, bndryconn)
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Test for Unassigned population
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        If HAZUS_Structures.Count = 0 Then
                            If HazusPopulations(1) > 0 Or HazusPopulations(2) > 0 Then
                                UnassignedNightPopU65 = CInt(Math.Round(0.99 * HazusPopulations(2) * (1 - PctElders), 0)) 'ResidentialUnder65
                                UnassignedNightPopO65 = CInt(Math.Round(0.99 * HazusPopulations(2) * PctElders, 0)) 'ResidentialOver65
                                '
                                UnassignedDayPopU65 = CInt(Math.Round(0.75 * HazusPopulations(1) * (1 - PctElders), 0)) 'ResidentialUnder65
                                UnassignedDayPopO65 = CInt(Math.Round(0.75 * HazusPopulations(1) * PctElders, 0)) 'ResidentialOver65
                                RaiseEvent ReportErrorMessage("Unassigned Population in census block " & cb.Key & ": ResNightU65: " & UnassignedNightPopU65 & " ResNightO65: " & UnassignedNightPopO65 & " ResDaytU65: " & UnassignedDayPopU65 & " ResNightO65: " & UnassignedDayPopO65, _structureInventoryname)
                                'Debug.Print("Unassigned Population: ResNightU65: " & UnassignedNightPopU65 & " ResNightO65: " & UnassignedNightPopO65 & " ResDaytU65: " & UnassignedDayPopU65 & " ResNightO65: " & UnassignedDayPopO65)
                            End If
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Commercial Structures and populate them
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Dim CommercialStructures As List(Of HazusStructureAttributes) = GetCommercialStructures(cb.Key, HazusPopulations, PctElders, blocktype, prefirm, SchemeID, HZBldgCountRec, HZCensusRec, MSHconn, bndryconn)
                        If CommercialStructures.Count = 0 Then
                            If HazusPopulations(1) > 0 Or HazusPopulations(3) > 0 Or HazusPopulations(4) > 0 Or HazusPopulations(5) > 0 Then
                                UnassignedNightPopU65 += CInt(Math.Round(0.02 * HazusPopulations(5), 0)) 'Under65
                                UnassignedDayPopU65 += CInt(Math.Round(0.98 * HazusPopulations(5) + 0.2 * HazusPopulations(1) * (1 - PctElders) + 0.8 * HazusPopulations(3) + HazusPopulations(4), 0)) 'Under65
                                UnassignedDayPopO65 += CInt(Math.Round(0.2 * HazusPopulations(1) * PctElders, 0)) 'Over65
                            End If
                        Else
                            HAZUS_Structures.AddRange(CommercialStructures)
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Industrial Structures and populate them
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Dim IndustrialStructures As List(Of HazusStructureAttributes) = GetIndustrialStructures(cb.Key, HazusPopulations, PctElders, blocktype, prefirm, SchemeID, HZBldgCountRec, HZCensusRec, MSHconn, bndryconn)
                        If IndustrialStructures.Count = 0 Then
                            If HazusPopulations(6) > 0 Then
                                UnassignedNightPopU65 += CInt(Math.Round(0.1 * HazusPopulations(6), 0)) 'Under65
                                UnassignedDayPopU65 += CInt(Math.Round(0.8 * HazusPopulations(6), 0)) 'Under65
                            End If
                        Else
                            HAZUS_Structures.AddRange(IndustrialStructures)
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Educational Structures and populate them
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Dim EducationalStructures As List(Of HazusStructureAttributes) = GetEducationalStructures(cb.Key, HazusPopulations, PctElders, blocktype, prefirm, SchemeID, HZBldgCountRec, HZCensusRec, MSHconn, bndryconn)
                        If EducationalStructures.Count = 0 Then
                            If HazusPopulations(8) > 0 Or HazusPopulations(9) > 0 Then
                                UnassignedDayPopU65 += CInt(Math.Round(0.8 * HazusPopulations(8) + HazusPopulations(9), 0)) 'Under65
                            End If
                        Else
                            HAZUS_Structures.AddRange(EducationalStructures)
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Hotel Structures and populate them
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Dim HotelStructures As List(Of HazusStructureAttributes) = GetHotelStructures(cb.Key, HazusPopulations, PctElders, blocktype, prefirm, SchemeID, HZBldgCountRec, HZCensusRec, MSHconn, bndryconn)
                        If HotelStructures.Count = 0 Then
                            If HazusPopulations(3) > 0 Then
                                UnassignedDayPopU65 += CInt(Math.Round(0.2 * HazusPopulations(3), 0)) 'Under65
                                UnassignedNightPopU65 += CInt(Math.Round(HazusPopulations(3), 0)) 'Under65
                            End If
                        Else
                            HAZUS_Structures.AddRange(HotelStructures)
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Assign Any Excess Population
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        TotalHouseholds = 0
                        ReDim HouseHoldWeights(HAZUS_Structures.Count - 1)
                        'Unassigned daytime population
                        If UnassignedDayPopO65 > 0 Or UnassignedDayPopU65 > 0 Then
                            For Each HazusStructure As HazusStructureAttributes In HAZUS_Structures
                                TotalHouseholds += HazusStructure.HouseHoldsDay
                            Next
                            For j As Int32 = 0 To HAZUS_Structures.Count - 1
                                HouseHoldWeights(j) = HAZUS_Structures(j).HouseHoldsDay / TotalHouseholds
                            Next
                            '
                            If UnassignedDayPopO65 > 0 Then
                                PopulationDistribution = ValueArray(HouseHoldWeights, UnassignedDayPopO65)
                                For j As Int32 = 0 To HouseHoldWeights.Count - 1
                                    HAZUS_Structures(j).pop2pmo65 += PopulationDistribution(j)
                                Next
                            End If
                            If UnassignedDayPopU65 > 0 Then
                                PopulationDistribution = ValueArray(HouseHoldWeights, UnassignedDayPopU65)
                                For j As Int32 = 0 To HouseHoldWeights.Count - 1
                                    HAZUS_Structures(j).pop2pmu65 += PopulationDistribution(j)
                                Next
                            End If
                        End If
                        'Unassigned nighttime population
                        TotalHouseholds = 0
                        If UnassignedNightPopO65 > 0 Or UnassignedNightPopU65 > 0 Then
                            For Each HazusStructure As HazusStructureAttributes In HAZUS_Structures
                                TotalHouseholds += HazusStructure.HouseHoldsNight
                            Next
                            For j As Int32 = 0 To HAZUS_Structures.Count - 1
                                HouseHoldWeights(j) = HAZUS_Structures(j).HouseHoldsNight / TotalHouseholds
                            Next
                            '
                            If UnassignedNightPopO65 > 0 Then
                                PopulationDistribution = ValueArray(HouseHoldWeights, UnassignedNightPopO65)
                                For j As Int32 = 0 To HouseHoldWeights.Count - 1
                                    HAZUS_Structures(j).pop2amo65 += PopulationDistribution(j)
                                Next
                            End If
                            If UnassignedNightPopU65 > 0 Then
                                PopulationDistribution = ValueArray(HouseHoldWeights, UnassignedNightPopU65)
                                For j As Int32 = 0 To HouseHoldWeights.Count - 1
                                    HAZUS_Structures(j).pop2amu65 += PopulationDistribution(j)
                                Next
                            End If
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Get Vehicle Values and Assing Foundation Heights
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        If ComputeVehicleValues = True Then
                            Using NightVehiclesCommand As OleDbCommand = New OleDbCommand("SELECT TotalExp FROM flNightVehicleInv WHERE CensusBlock = '" & cb.Key & "'", VehConn)
                                Using NightVehicleReader As OleDbDataReader = NightVehiclesCommand.ExecuteReader
                                    If NightVehicleReader.HasRows Then NightVehicleReader.Read()
                                    AverageVehicleValue = CSng(NightVehicleReader.GetDouble(0) / HAZUS_Structures.Count)
                                    System.Threading.Tasks.Parallel.ForEach(HAZUS_Structures, Sub(HAZUSStructure)
                                                                                                  HAZUSStructure.vehicval = AverageVehicleValue
                                                                                              End Sub)
                                End Using
                            End Using
                        End If
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        'Store census block data for writing out at the end.
                        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
                        Dim structurecount As Integer = HAZUS_Structures.Count
                        Dim successes As Integer = 0
                        Try
                            SI_Points.AddRange(GetMultiPolyPointsTesselate(cb.Value, HAZUS_Structures.Count))
                            'update datatable for dbf
                            For Each HAZUS_Structure As HazusStructureAttributes In HAZUS_Structures
                                With HAZUS_Structure
                                    dt.Rows.Add({.Name, .DamCat, .OccType, .Stories, .basement, .bldgtype, .foundationheight, .pop2amu65, .pop2amo65, .pop2pmu65, .pop2pmo65, .StructVal, .contentval, 0, .vehicval, medianyear, FipEntry, .FoundationType, prefirm, .foundationheight, 0, False, False, "Base"})
                                End With
                            Next
                        Catch ex As Exception
                            'place missing points into the point shapefile...
                            RaiseEvent ReportErrorMessage("There were " & successes & " successful points, and " & structurecount - successes & " unsuccessful points in the census block " & cb.Key & "." & vbNewLine & "Attempting alternative structure placement methodology", _structureInventoryname)
                            ' Debug.Print(i.ToString & ", There were " & successes & " successful points, and " & structurecount - successes & " unsuccessful points in the census block " & IntersectingCensusBlocks(i) & "." & vbNewLine & "Attempting alternative structure placement methodology")
                            If successes = 0 Then
                                Dim GridPoints As List(Of PointD) = GetPointsGridCells(cb.Value, HAZUS_Structures.Count)
                                If GridPoints.Count <> HAZUS_Structures.Count Then
                                    RaiseEvent ReportErrorMessage("Alternative placement failed in the census block " & cb.Key & ".", _structureInventoryname)
                                Else
                                    SI_Points.AddRange(GridPoints)
                                    For Each HAZUS_Structure As HazusStructureAttributes In HAZUS_Structures
                                        With HAZUS_Structure
                                            dt.Rows.Add({.Name, .DamCat, .OccType, .Stories, .basement, .bldgtype, .foundationheight, .pop2amu65, .pop2amo65, .pop2pmu65, .pop2pmo65, .StructVal, .contentval, 0, .vehicval, medianyear, FipEntry, .FoundationType, prefirm, .foundationheight, 0, False, False, "Base"})
                                        End With
                                    Next
                                    RaiseEvent ReportErrorMessage("Alternative placement methodology Successful", _structureInventoryname)
                                    'Debug.Print("Alternative placement methodology Successful")
                                End If
                            Else
                                'some structures already placed, so dont re place them... 
                                RaiseEvent ReportErrorMessage("This Should never happen. " & "Census Block: " & cb.Key, _structureInventoryname)
                            End If


                        End Try
                    End If
                End If
                HZBldgCountRec.Close()
                If i Mod 1000 = 0 Then System.GC.Collect()
            Catch ex As Exception
                RaiseEvent ReportErrorMessage(ex.ToString & vbNewLine & "Census Block " & cb.Key, _structureInventoryname)
                'Debug.Print(i.ToString)
            End Try
            i += 1
        Next

        bndryconn.Close() : bndryconn.Dispose()
        Dim StructurePoints As New PointFeatures(SI_Points.ToArray)
        StructurePoints.Reproject(HazusProjection, StudyAreaProjection)
        Dim NewPntShape As New ShapefileWriter(OutFilename)
        Try
            NewPntShape.WriteFeatures(StructurePoints, dt, StudyAreaProjection)
        Catch ex As Exception
            MsgBox(ex.Message)
            Return Nothing
        End Try
        Return dt
    End Function
    Private Function GetHazusPopulations(ByVal bndryconn As OleDbConnection, ByVal cb As String) As Int32()
        Dim cmd As New OleDbCommand, hzdemobrec As OleDbDataReader
        cmd = New OleDbCommand("SELECT Population, ResidDay, ResidNight, Hotel, Visitor, WorkingCom, WorkingInd, Commuting5PM, SchoolEnrollmentKto12, SchoolEnrollmentCollege, MedianYearBuilt FROM HZDemographicsB WHERE CensusBlock = '" & cb & "'", bndryconn)
        hzdemobrec = cmd.ExecuteReader
        Dim pop(9) As Int32
        If hzdemobrec.HasRows Then
            hzdemobrec.Read()
            For n As Int32 = 0 To 9
                pop(n) = hzdemobrec.GetInt32(n)
            Next
        End If
        hzdemobrec.Close()
        Return pop
    End Function
    Private Function GetPctElders(ByVal bndryconn As OleDbConnection, ByVal cb As String) As Double
        Dim HZDemoBRec As OleDbDataReader, cmd As OleDbCommand
        cmd = New OleDbCommand("SELECT MaleLess16, Male16to65, MaleOver65, FemaleLess16, Female16to65, FemaleOver65 FROM HZDemographicsB WHERE CensusBlock = '" & cb & "'", bndryconn)
        HZDemoBRec = cmd.ExecuteReader
        If HZDemoBRec.HasRows Then HZDemoBRec.Read()
        Dim Total As Int32 = HZDemoBRec.GetInt32(0) + HZDemoBRec.GetInt32(1) + HZDemoBRec.GetInt32(2) + HZDemoBRec.GetInt32(3) + HZDemoBRec.GetInt32(4) + HZDemoBRec.GetInt32(5)
        Dim ElderPct As Double = 0
        If Total > 0 Then ElderPct = (HZDemoBRec.GetInt32(2) + HZDemoBRec.GetInt32(5)) / Total
        HZDemoBRec.Close()
        Return ElderPct
    End Function
    Private Function GetMedianYear(ByVal bndryconn As OleDbConnection, ByVal cb As String) As Integer
        Dim HZDemoBRec As OleDbDataReader, cmd As OleDbCommand
        cmd = New OleDbCommand("SELECT MedianYearBuilt FROM HZDemographicsB WHERE CensusBlock = '" & cb & "'", bndryconn)
        HZDemoBRec = cmd.ExecuteReader
        If HZDemoBRec.HasRows Then HZDemoBRec.Read()
        Dim Total As Int32 = CInt(HZDemoBRec.GetValue(0))
        HZDemoBRec.Close()
        If Total > 0 Then Return Total
        RaiseEvent ReportErrorMessage("Median Year was not greater than zero, overriding with 2010", _structureInventoryname)
        RaiseEvent ReportNSIErrorMessage("Median Year was not greater than zero, overriding with 2010, " & cb, _structureInventoryname)
        'Debug.Print("Median Year was not greater than zero, overriding with 2010")
        Return 2010 'just in case?
    End Function
    Private Function GetFirmEntryYear(ByVal mshconn As OleDbConnection, ByVal cb As String) As SchemeIDAndEntryYear
        Dim HZflschememapping As OleDbDataReader
        Dim cmd As OleDbCommand
        cmd = New OleDbCommand("SELECT SchemeID, EntryDate FROM flSchemeMapping WHERE CensusBlock = '" & cb & "'", mshconn)
        HZflschememapping = cmd.ExecuteReader
        If HZflschememapping.HasRows Then HZflschememapping.Read()
        Dim entryyear As Integer = CInt(HZflschememapping.GetValue(1))
        Dim schemeID As String = HZflschememapping.GetString(0)
        'if there are more than rows i am screwed.
        Dim s As New SchemeIDAndEntryYear
        s.SchemeID = schemeID
        If entryyear > 0 Then
            s.EntryYear = entryyear
        Else
            RaiseEvent ReportErrorMessage("Entry Year was less than zero, substituting with 2010", _structureInventoryname)
            RaiseEvent ReportNSIErrorMessage("Entry Year was not greater than zero, overriding with 2010, " & cb, _structureInventoryname)
            'Debug.Print("Entry Year was less than zero, substituting with 2010")
            s.EntryYear = 2010
        End If

        Return s
    End Function
    Private Class RectangularComparer
        Implements IComparer
        ' maintain a reference to the 2-dimensional array being sorted

        Private sortArray() As Single

        ' constructor initializes the sortArray reference
        Public Sub New(ByVal theArray() As Single)
            sortArray = theArray
        End Sub

        Public Function Compare(ByVal x As Object, ByVal y As Object) As Integer Implements IComparer.Compare
            ' x and y are integer row numbers into the sortArray
            Dim i1 As Integer = DirectCast(x, Integer)
            Dim i2 As Integer = DirectCast(y, Integer)

            ' compare the items in the sortArray (i1 compare to i2 for descending, i2 compare to i1 for ascending)
            Return sortArray(i2).CompareTo(sortArray(i1))
        End Function
    End Class
    Public Function ValueArray(ByVal weights() As Double, ByVal NValues As Int32) As Int32()
        Dim result(weights.Count - 1) As Int32, count As Int32 = 0
        Dim errors(weights.Count - 1) As Single
        Dim errortags(weights.Count - 1) As Int32
        'first shot
        For i As Int32 = 0 To weights.Count - 1
            result(i) = CInt(Math.Floor(weights(i) * NValues))
            count += result(i)
        Next
        'get errors sorted on the dictionary key
        For i As Int32 = 0 To weights.Count - 1
            errors(i) = CSng(weights(i) * NValues - result(i))
            errortags(i) = i
        Next
        Dim myComparer As New RectangularComparer(errors)
        Array.Sort(errortags, myComparer)
        'distribute excess among the first shot of largest errors and call it good.
        For i As Int32 = 0 To weights.Count - 1
            If count = NValues Then Exit For
            result(errortags(i)) += 1
            count += 1
        Next
        'done
        Return result
    End Function
    Private Function GetConstructionTypes(ByVal NumberOfStructures As Int32, ByVal BldgSchemesId As String, ByVal Occupancy As String, ByVal MSHConnection As OleDbConnection) As List(Of String)
        Dim ConstructionWeights(4) As Double
        ConstructionWeights(0) = 1 'Default to all wood construction if data reader doesn't find anything
        Using MSHCommand As New OleDbCommand("SELECT WPct, CPct, SPct, MPct, HPct FROM hzGenBldgScheme WHERE BldgSchemesId = '" & BldgSchemesId & "' AND Occupancy = '" & Occupancy & "'", MSHConnection)
            Using MSHReader As OleDbDataReader = MSHCommand.ExecuteReader
                If MSHReader.HasRows Then
                    MSHReader.Read()
                    ConstructionWeights(0) = CShort(MSHReader.Item("WPct")) / 100 'Percent Wood Construction
                    ConstructionWeights(1) = CShort(MSHReader.Item("CPct")) / 100 'Percent Concrete Construction
                    ConstructionWeights(2) = CShort(MSHReader.Item("SPct")) / 100 'Percent Steel Construction
                    ConstructionWeights(3) = CShort(MSHReader.Item("MPct")) / 100 'Percent Masonry Construction
                    ConstructionWeights(4) = CShort(MSHReader.Item("HPct")) / 100 'Percent Manufactured Homes
                End If
            End Using
        End Using
        '
        Dim ConstructionTypes As New List(Of String)
        Dim ConstructionValues() As Int32 = ValueArray(ConstructionWeights, NumberOfStructures)
        For i As Int32 = 1 To ConstructionValues(0)
            ConstructionTypes.Add("Wood")
        Next
        For i As Int32 = 1 To ConstructionValues(1)
            ConstructionTypes.Add("Concrete")
        Next
        For i As Int32 = 1 To ConstructionValues(2)
            ConstructionTypes.Add("Steel")
        Next
        For i As Int32 = 1 To ConstructionValues(3)
            ConstructionTypes.Add("Masonry")
        Next
        For i As Int32 = 1 To ConstructionValues(4)
            ConstructionTypes.Add("Manufactured")
        Next
        '
        Return ConstructionTypes
    End Function
    Private Function GetFoundationTypeandHeight(ByVal NumberOfStructures As Int32, ByVal BlockType As String, ByVal Occupancy As String, ByVal prefirm As Byte, ByVal SchemeID As String, ByVal MSHConnection As OleDbConnection, ByVal bndryGrbsConnection As OleDbConnection) As List(Of FoundationHeightAndType)
        Dim table As String = "flSchemeRiverine"
        Dim Distribution As String = "PostFirmDist"
        Dim Type As String = "FoundationType"
        Dim height As String = "PostFirmHt"
        Select Case BlockType
            Case "R"
                table = "flSchemeRiverine"
                Select Case prefirm
                    Case 1
                        Distribution = "PostFirmDist"
                        height = "PostFirmHt"
                    Case 0
                        Distribution = "PreFirmDist"
                        height = "PreFirmHt"
                End Select
            Case "C"
                table = "flSchemeCoastal"
                Select Case prefirm
                    Case 1
                        Distribution = "PostFirmDistAZone"
                        height = "PostFirmHtAZone"
                    Case 0
                        Distribution = "PreFirmDist"
                        height = "PreFirmHt"
                End Select
            Case "L"
                table = "flSchemeGLakes"
                Select Case prefirm
                    Case 1
                        Distribution = "PostFirmDist"
                        height = "PostFirmHt"
                    Case 0
                        Distribution = "PreFirmDist"
                        height = "PreFirmHt"
                End Select
            Case Else
                table = "flSchemeRiverine"
                Select Case prefirm
                    Case 1
                        Distribution = "PostFirmDist"
                        height = "PostFirmHt"
                    Case 0
                        Distribution = "PreFirmDist"
                        height = "PreFirmHt"
                End Select
        End Select
        Dim heights As New List(Of Double)
        Dim types As New List(Of String)
        Dim rates As New List(Of Double)
        Using MSHCommand As New OleDbCommand("SELECT " & Type & ", " & Distribution & ", " & height & " FROM " & table & " WHERE SchemeId = '" & SchemeID & "' AND Soccup = '" & Occupancy & "'", MSHConnection)
            Using MSHReader As OleDbDataReader = MSHCommand.ExecuteReader
                If MSHReader.HasRows Then
                    While MSHReader.HasRows
                        MSHReader.Read()
                        'gather information
                        If rates.Sum <> 1 OrElse rates.Count < 7 Then
                            rates.Add(CShort(MSHReader.Item(Distribution)) / 100)
                            heights.Add(CDbl(MSHReader.Item(height)))
                            types.Add(MSHReader.Item(Type))
                        Else
                            Exit While
                        End If

                    End While
                End If
            End Using
        End Using
        '

        Dim foundationheightsandtypes As New List(Of FoundationHeightAndType)
        Dim ConstructionValues() As Int32 = ValueArray(rates.ToArray, NumberOfStructures)
        For j As Int32 = 0 To ConstructionValues.Count - 1
            For i As Int32 = 1 To ConstructionValues(j)
                Dim fh As New FoundationHeightAndType
                fh.FoundationHeight = heights(j)
                fh.FoundationType = types(j)
                foundationheightsandtypes.Add(fh)
            Next
        Next

        '
        Return foundationheightsandtypes
    End Function
    Private Function GetTotalStructureValues(ByVal NumberOfStructures As Int32, ByVal CB_ID As String, ByVal OccupancyClass As String, ByVal bndryconn As OleDbConnection) As Single()
        Dim TotalStructureValue As Double
        '
        Using Command As New OleDbCommand("SELECT " & OccupancyClass & " FROM hzExposureOccupB WHERE CensusBlock = '" & CB_ID & "'", bndryconn)
            Using Reader As OleDbDataReader = Command.ExecuteReader
                If Reader.HasRows Then
                    Reader.Read()
                    TotalStructureValue = CInt(Reader.GetInt32(0)) * 1000
                End If
            End Using
        End Using
        '
        Dim ValuePerStructure As Single = CSng(TotalStructureValue / NumberOfStructures)
        Dim StructureValues(NumberOfStructures - 1) As Single
        System.Threading.Tasks.Parallel.For(0, NumberOfStructures, Sub(i As Int32)
                                                                       StructureValues(i) = ValuePerStructure
                                                                   End Sub)
        '
        Return StructureValues
    End Function
    Private Function GetTotalContentValues(ByVal NumberOfStructures As Int32, ByVal CB_ID As String, ByVal OccupancyClass As String, ByVal bndryconn As OleDbConnection) As Single()
        Dim TotalContentValue As Double
        '
        Using Command As New OleDbCommand("SELECT " & OccupancyClass & " FROM hzExposureContentOccupB WHERE CensusBlock = '" & CB_ID & "'", bndryconn)
            Using Reader As OleDbDataReader = Command.ExecuteReader
                If Reader.HasRows Then
                    Reader.Read()
                    TotalContentValue = Reader.GetInt32(0) * 1000
                End If
            End Using
        End Using
        '
        Dim ContentValuePerStructure As Single = CSng(TotalContentValue / NumberOfStructures)
        Dim ContentValues(NumberOfStructures - 1) As Single
        System.Threading.Tasks.Parallel.For(0, NumberOfStructures, Sub(i As Int32)
                                                                       ContentValues(i) = ContentValuePerStructure
                                                                   End Sub)
        '
        Return ContentValues
    End Function
    Private Function GetResidentialStructures(ByVal CB_ID As String, ByVal PopulationTable() As Int32, ByVal PercentElders As Double, ByVal blocktype As String, ByVal prefirm As Byte, ByVal schemeID As String, ByVal HZBldgCountRec As OleDbDataReader, ByVal HZCensusRec As OleDbDataReader, ByVal MSHConnection As OleDbConnection, ByVal bndryconn As OleDbConnection) As List(Of HazusStructureAttributes)
        Dim HAZUSStructures As New List(Of HazusStructureAttributes)
        '
        'Household for RES1=1,RES2=1,RES3A=2,RES3B=3.5,RES3C=7,RES3D=14.5,RES3E=34.5,RES3F=50,RES5=50,RES6=50 same for all times of day
        'Dim HouseHolds() As Single = New Single() {1, 1, 2, 3.5, 7, 14.5, 34.5, 50, 50, 50}
        Dim NumberOfStructures As Int16
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES1 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES1I"))
        Dim RES1StoryWeights(3) As Double
        RES1StoryWeights(0) = CShort(HZCensusRec.Item("Pct1StoryRes1")) / 100 'Percent 1-story RES1
        RES1StoryWeights(1) = CShort(HZCensusRec.Item("Pct2StoryRes1")) / 100 'Percent 2-story RES1
        RES1StoryWeights(2) = CShort(HZCensusRec.Item("Pct3StoryRes1")) / 100 'Percent 2-story RES1
        RES1StoryWeights(3) = CShort(HZCensusRec.Item("PctSplitLvlRes1")) / 100 'Percent Split Level RES1
        Dim RES1StoryValues() As Int32 = ValueArray(RES1StoryWeights, NumberOfStructures)
        '
        For i As Int32 = 1 To RES1StoryValues(0)
            HAZUSStructures.Add(New HazusStructureAttributes With {.DamCat = "Residential", .OccType = "RES1-1S", .Stories = 1, .HouseHoldsDay = 1, .HouseHoldsNight = 1})
        Next
        For i As Int32 = 1 To RES1StoryValues(1)
            HAZUSStructures.Add(New HazusStructureAttributes With {.DamCat = "Residential", .OccType = "RES1-2S", .Stories = 2, .HouseHoldsDay = 1, .HouseHoldsNight = 1})
        Next
        For i As Int32 = 1 To RES1StoryValues(2)
            HAZUSStructures.Add(New HazusStructureAttributes With {.DamCat = "Residential", .OccType = "RES1-3S", .Stories = 3, .HouseHoldsDay = 1, .HouseHoldsNight = 1})
        Next
        For i As Int32 = 1 To RES1StoryValues(3)
            HAZUSStructures.Add(New HazusStructureAttributes With {.DamCat = "Residential", .OccType = "RES1-SL", .Stories = 2, .HouseHoldsDay = 1, .HouseHoldsNight = 1})
        Next
        '
        Dim foundtypeandheight As List(Of FoundationHeightAndType) = GetFoundationTypeandHeight(NumberOfStructures, blocktype, "RES1", prefirm, schemeID, MSHConnection, bndryconn)
        Dim res1withbasement As Int32 = 0
        'For i = 0 To foundtypeandheight.Count - 1
        '    If foundtypeandheight(i).FoundationType = "Basement" Then res1withbasement += 1
        'Next
        'Dim NumberRes1WithBasement As Int32 = CInt(Math.Round(NumberOfStructures * CShort(HZCensusRec.Item("PctWithBasemnt")) / 100))
        'If NumberRes1WithBasement <> res1withbasement Then RaiseEvent ReportErrorMessage("Conflict in count of residential structures with basement, on census block " & CB_ID & " difference is " & NumberRes1WithBasement - res1withbasement & " of " & NumberOfStructures, _structureInventoryname)
        System.Threading.Tasks.Parallel.For(0, HAZUSStructures.Count, Sub(i As Int32)
                                                                          If foundtypeandheight(i).FoundationType = "Basement" Then
                                                                              HAZUSStructures(i).basement = "Yes"
                                                                              HAZUSStructures(i).OccType &= "WB"
                                                                              HAZUSStructures(i).Name = HAZUSStructures(i).OccType & " " & _counter + i
                                                                          Else
                                                                              HAZUSStructures(i).basement = "No"
                                                                              HAZUSStructures(i).OccType &= "NB"
                                                                              HAZUSStructures(i).Name = HAZUSStructures(i).OccType & " " & _counter + i
                                                                          End If
                                                                          HAZUSStructures(i).FoundationType = foundtypeandheight(i).FoundationType
                                                                          HAZUSStructures(i).foundationheight = foundtypeandheight(i).FoundationHeight
                                                                      End Sub)
        '
        _counter += HAZUSStructures.Count
        Dim ConstructionTypesResult As List(Of String) = GetConstructionTypes(NumberOfStructures, HZCensusRec.Item("BldgSchemesId").ToString, "RES1", MSHConnection)
        Dim StructureValues() As Single = GetTotalStructureValues(NumberOfStructures, CB_ID, "RES1I", bndryconn)
        Dim ContentValues() As Single = GetTotalContentValues(NumberOfStructures, CB_ID, "RES1I", bndryconn)
        System.Threading.Tasks.Parallel.For(0, HAZUSStructures.Count, Sub(i As Int32)
                                                                          With HAZUSStructures(i)
                                                                              .bldgtype = ConstructionTypesResult(i)
                                                                              .StructVal = StructureValues(i)
                                                                              .contentval = ContentValues(i)
                                                                          End With
                                                                      End Sub)


        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES2 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES2I"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Residential", 1, 1, 1, "RES2I", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES3A Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES3AI"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Residential", 1, 2, 2, "RES3AI", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES3B Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES3BI"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Residential", 2, 3.5, 3.5, "RES3BI", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES3C Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES3CI"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Residential", 2, 7, 7, "RES3CI", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES3D Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES3DI"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Residential", 3, 14.5, 14.5, "RES3DI", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES3E Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES3EI"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Residential", 3, 34.5, 34.5, "RES3EI", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES3F Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES3FI"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Residential", 4, 50, 50, "RES3FI", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES5 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES5I"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Residential", 4, 50, 50, "RES5I", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES6 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES6I"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Residential", 1, 50, 50, "RES6I", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Get Total Residential Population
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        If HAZUSStructures.Count = 0 Then Return HAZUSStructures

        Dim Total2amUnder65, Total2pmUnder65, Total2amOver65, Total2pmOver65 As Int32
        'For PopulationTable indexes (Population, ResidDay, ResidNight, Hotel, Visitor, WorkingCom, WorkingInd, Commuting5PM, SchoolEnrollmentKto12, SchoolEnrollmentCollege)
        Total2amUnder65 = CInt(Math.Round(0.99 * PopulationTable(2) * (1 - PercentElders), 0)) 'ResidentialUnder65
        Total2amOver65 = CInt(Math.Round(0.99 * PopulationTable(2) * PercentElders, 0)) 'ResidentialOver65
        '
        Total2pmUnder65 = CInt(Math.Round(0.75 * PopulationTable(1) * (1 - PercentElders), 0)) 'ResidentialUnder65
        Total2pmOver65 = CInt(Math.Round(0.75 * PopulationTable(1) * PercentElders, 0)) 'ResidentialOver65
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Populate the structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim TotalResidentialHouseholds As Double
        For Each ResHazus As HazusStructureAttributes In HAZUSStructures
            TotalResidentialHouseholds += ResHazus.HouseHoldsDay
        Next
        Dim HouseHoldWeights(HAZUSStructures.Count - 1) As Double
        System.Threading.Tasks.Parallel.For(0, HAZUSStructures.Count, Sub(i As Int32)
                                                                          HouseHoldWeights(i) = HAZUSStructures(i).HouseHoldsDay / TotalResidentialHouseholds
                                                                      End Sub)
        '
        Dim ResidentialPopDistribution2amU() As Int32 = ValueArray(HouseHoldWeights, Total2amUnder65)
        Dim ResidentialPopDistribution2amO() As Int32 = ValueArray(HouseHoldWeights, Total2amOver65)
        Dim ResidentialPopDistribution2pmU() As Int32 = ValueArray(HouseHoldWeights, Total2pmUnder65)
        Dim ResidentialPopDistribution2pmO() As Int32 = ValueArray(HouseHoldWeights, Total2pmOver65)
        System.Threading.Tasks.Parallel.For(0, HAZUSStructures.Count, Sub(i As Int32)
                                                                          HAZUSStructures(i).pop2amu65 = ResidentialPopDistribution2amU(i)
                                                                          HAZUSStructures(i).pop2amo65 = ResidentialPopDistribution2amO(i)
                                                                          HAZUSStructures(i).pop2pmu65 = ResidentialPopDistribution2pmU(i)
                                                                          HAZUSStructures(i).pop2pmo65 = ResidentialPopDistribution2pmO(i)
                                                                      End Sub)
        '
        Return HAZUSStructures
    End Function
    Private Function GetCommercialStructures(ByVal CB_ID As String, ByVal PopulationTable() As Int32, ByVal PercentElders As Double, ByVal blocktype As String, ByVal prefirm As Byte, ByVal schemeid As String, ByVal HZBldgCountRec As OleDbDataReader, ByVal HZCensusRec As OleDbDataReader, ByVal MSHConnection As OleDbConnection, ByVal bndryconn As OleDbConnection) As List(Of HazusStructureAttributes)
        Dim HAZUSStructures As New List(Of HazusStructureAttributes)
        Dim NumberOfStructures As Int16
        'Households for Commercial Structures = {0.1, 2, 2} 'for times of day 2am, 2pm, 5pm
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'COM1-10 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        For i As Int32 = 1 To 10
            NumberOfStructures = CShort(HZBldgCountRec.Item("COM" & i & "I"))
            Select Case i
                Case 4, 7, 10
                    HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Commercial", 4, 2, 0.1, "COM" & i & "I", CB_ID, blocktype, prefirm, schemeid, MSHConnection, HZCensusRec, bndryconn))
                Case 5, 6
                    HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Commercial", 2, 2, 0.1, "COM" & i & "I", CB_ID, blocktype, prefirm, schemeid, MSHConnection, HZCensusRec, bndryconn))
                Case Else
                    HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Commercial", 1, 2, 0.1, "COM" & i & "I", CB_ID, blocktype, prefirm, schemeid, MSHConnection, HZCensusRec, bndryconn))
            End Select
        Next
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'AGR1 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("AGR1I"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Commercial", 1, 2, 0.1, "AGR1I", CB_ID, blocktype, prefirm, schemeid, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'REL1 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("REL1I"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Public", 1, 2, 0.1, "REL1I", CB_ID, blocktype, prefirm, schemeid, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'GOV1-2 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("GOV1I"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Public", 1, 2, 0.1, "GOV1I", CB_ID, blocktype, prefirm, schemeid, MSHConnection, HZCensusRec, bndryconn))
        NumberOfStructures = CShort(HZBldgCountRec.Item("GOV2I"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Public", 1, 2, 0.1, "GOV2I", CB_ID, blocktype, prefirm, schemeid, MSHConnection, HZCensusRec, bndryconn))
        '
        If HAZUSStructures.Count = 0 Then Return HAZUSStructures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Get Total Commercial Population
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim Total2amUnder65, Total2pmUnder65, Total2amOver65, Total2pmOver65 As Int32
        'For PopulationTable indexes (Population, ResidDay, ResidNight, Hotel, Visitor, WorkingCom, WorkingInd, Commuting5PM, SchoolEnrollmentKto12, SchoolEnrollmentCollege)
        Total2amUnder65 = CInt(Math.Round(0.02 * PopulationTable(5), 0)) 'Under65
        Total2amOver65 = 0 'Over65
        '
        Total2pmUnder65 = CInt(Math.Round(0.98 * PopulationTable(5) + 0.2 * PopulationTable(1) * (1 - PercentElders) + 0.8 * PopulationTable(3) + PopulationTable(4), 0)) 'Under65
        Total2pmOver65 = CInt(Math.Round(0.2 * PopulationTable(1) * PercentElders, 0)) 'Over65
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Populate the structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim PopulationDistributed2amU() As Int32 = EquallyDistributePopulation(Total2amUnder65, HAZUSStructures.Count)
        Dim PopulationDistributed2pmU() As Int32 = EquallyDistributePopulation(Total2pmUnder65, HAZUSStructures.Count)
        Dim PopulationDistributed2pmO() As Int32 = EquallyDistributePopulation(Total2pmOver65, HAZUSStructures.Count)
        System.Threading.Tasks.Parallel.For(0, HAZUSStructures.Count, Sub(i As Int32)
                                                                          HAZUSStructures(i).pop2amu65 = PopulationDistributed2amU(i)
                                                                          HAZUSStructures(i).pop2amo65 = 0
                                                                          HAZUSStructures(i).pop2pmu65 = PopulationDistributed2pmU(i)
                                                                          HAZUSStructures(i).pop2pmo65 = PopulationDistributed2pmO(i)
                                                                      End Sub)
        '
        Return HAZUSStructures
    End Function
    Private Function GetIndustrialStructures(ByVal CB_ID As String, ByVal PopulationTable() As Int32, ByVal PercentElders As Double, ByVal blocktype As String, ByVal prefirm As Byte, ByVal schemeID As String, ByVal HZBldgCountRec As OleDbDataReader, ByVal HZCensusRec As OleDbDataReader, ByVal MSHConnection As OleDbConnection, ByVal bndryconn As OleDbConnection) As List(Of HazusStructureAttributes)
        Dim HAZUSStructures As New List(Of HazusStructureAttributes)
        Dim NumberOfStructures As Int16
        'Households for Industrial Structures = {1, 5, 5} 'for times of day 2am, 2pm, 5pm
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'IND1-6 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        For i As Int32 = 1 To 6
            NumberOfStructures = CShort(HZBldgCountRec.Item("IND" & i & "I"))
            HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Industrial", 1, 5, 1, "IND" & i & "I", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        Next
        If HAZUSStructures.Count = 0 Then Return HAZUSStructures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Get Total Industrial Population
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim Total2amUnder65, Total2pmUnder65 As Int32
        'For PopulationTable indexes (Population, ResidDay, ResidNight, Hotel, Visitor, WorkingCom, WorkingInd, Commuting5PM, SchoolEnrollmentKto12, SchoolEnrollmentCollege)
        Total2amUnder65 = CInt(Math.Round(0.1 * PopulationTable(6), 0)) 'Under65
        '
        Total2pmUnder65 = CInt(Math.Round(0.8 * PopulationTable(6), 0)) 'Under65
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Populate the structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim PopulationDistributed2amU() As Int32 = EquallyDistributePopulation(Total2amUnder65, HAZUSStructures.Count)
        Dim PopulationDistributed2pmU() As Int32 = EquallyDistributePopulation(Total2pmUnder65, HAZUSStructures.Count)
        System.Threading.Tasks.Parallel.For(0, HAZUSStructures.Count, Sub(i As Int32)
                                                                          HAZUSStructures(i).pop2amu65 = PopulationDistributed2amU(i)
                                                                          HAZUSStructures(i).pop2amo65 = 0
                                                                          HAZUSStructures(i).pop2pmu65 = PopulationDistributed2pmU(i)
                                                                          HAZUSStructures(i).pop2pmo65 = 0
                                                                      End Sub)
        '
        Return HAZUSStructures
    End Function
    Private Function GetEducationalStructures(ByVal CB_ID As String, ByVal PopulationTable() As Int32, ByVal PercentElders As Double, ByVal blocktype As String, ByVal prefirm As Byte, ByVal schemeID As String, ByVal HZBldgCountRec As OleDbDataReader, ByVal HZCensusRec As OleDbDataReader, ByVal MSHConnection As OleDbConnection, ByVal bndryconn As OleDbConnection) As List(Of HazusStructureAttributes)
        Dim HAZUSStructures As New List(Of HazusStructureAttributes)
        Dim NumberOfStructures As Int16
        'Households for Educational Structures = {0.1, 5, 5} 'for times of day 2am, 2pm, 5pm
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'EDU1-2 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        For i As Int32 = 1 To 2
            NumberOfStructures = CShort(HZBldgCountRec.Item("EDU" & i & "I"))
            HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Public", 1, 5, 0.1, "EDU" & i & "I", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        Next

        If HAZUSStructures.Count = 0 Then Return HAZUSStructures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Get Total Education Population
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'For PopulationTable indexes (Population, ResidDay, ResidNight, Hotel, Visitor, WorkingCom, WorkingInd, Commuting5PM, SchoolEnrollmentKto12, SchoolEnrollmentCollege)
        Dim Total2pmUnder65 As Int32 = CInt(Math.Round(0.8 * PopulationTable(8) + PopulationTable(9), 0)) 'Under65
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Populate the structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim PopulationDistributed() As Int32 = EquallyDistributePopulation(Total2pmUnder65, HAZUSStructures.Count)
        System.Threading.Tasks.Parallel.For(0, HAZUSStructures.Count, Sub(i As Int32)
                                                                          HAZUSStructures(i).pop2amu65 = 0
                                                                          HAZUSStructures(i).pop2amo65 = 0
                                                                          HAZUSStructures(i).pop2pmu65 = PopulationDistributed(i)
                                                                          HAZUSStructures(i).pop2pmo65 = 0
                                                                      End Sub)
        '
        Return HAZUSStructures
    End Function
    Private Function GetHotelStructures(ByVal CB_ID As String, ByVal PopulationTable() As Int32, ByVal PercentElders As Double, ByVal blocktype As String, ByVal prefirm As Byte, ByVal schemeID As String, ByVal HZBldgCountRec As OleDbDataReader, ByVal HZCensusRec As OleDbDataReader, ByVal MSHConnection As OleDbConnection, ByVal bndryconn As OleDbConnection) As List(Of HazusStructureAttributes)
        Dim HAZUSStructures As New List(Of HazusStructureAttributes)
        Dim NumberOfStructures As Int16
        'Households for Hotel Structures = {50, 2, 2} 'for times of day 2am, 2pm, 5pm
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'RES4 Structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        NumberOfStructures = CShort(HZBldgCountRec.Item("RES4I"))
        HAZUSStructures.AddRange(GetBasicStructureData(NumberOfStructures, "Residential", 4, 2, 50, "RES4I", CB_ID, blocktype, prefirm, schemeID, MSHConnection, HZCensusRec, bndryconn))
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Get Total Hotel Population
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        If HAZUSStructures.Count = 0 Then
            Return HAZUSStructures
        End If
        'For PopulationTable indexes (Population, ResidDay, ResidNight, Hotel, Visitor, WorkingCom, WorkingInd, Commuting5PM, SchoolEnrollmentKto12, SchoolEnrollmentCollege)
        Dim Total2amUnder65 As Int32 = CInt(Math.Round(PopulationTable(3), 0)) 'Under65
        Dim Total2pmUnder65 As Int32 = CInt(Math.Round(0.2 * PopulationTable(3), 0)) 'Under65
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Populate the structures
        ''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim PopulationDistributed2amU() As Int32 = EquallyDistributePopulation(Total2amUnder65, HAZUSStructures.Count)
        Dim PopulationDistributed2pmU() As Int32 = EquallyDistributePopulation(Total2pmUnder65, HAZUSStructures.Count)
        System.Threading.Tasks.Parallel.For(0, HAZUSStructures.Count, Sub(i As Int32)
                                                                          HAZUSStructures(i).pop2amu65 = PopulationDistributed2amU(i)
                                                                          HAZUSStructures(i).pop2amo65 = 0
                                                                          HAZUSStructures(i).pop2pmu65 = PopulationDistributed2pmU(i)
                                                                          HAZUSStructures(i).pop2pmo65 = 0
                                                                      End Sub)
        '
        Return HAZUSStructures
    End Function
    Private Function EquallyDistributePopulation(ByVal Population As Int32, ByVal NumberOfStructures As Int32) As Int32()
        If NumberOfStructures = 0 Then Return Nothing
        '
        Dim Result(NumberOfStructures - 1) As Int32
        If Population = 0 Then Return Result
        Dim PopulationCounter As Int32 = 0
        'expected
        Dim ExpectedPerStructure As Int32 = CInt(Math.Floor(Population / NumberOfStructures))
        For i As Int32 = 0 To NumberOfStructures - 1
            Result(i) = ExpectedPerStructure
            PopulationCounter += Result(i)
        Next
        'leftovers
        For j As Int32 = 0 To (Population - PopulationCounter) - 1
            Result(j) += 1
        Next

        Return Result
    End Function
    Private Function GetBasicStructureData(ByVal NumberOfStructures As Int32, ByVal DamageCategory As String, ByVal NumberOfStories As Int16, ByVal HouseholdsDay As Single, ByVal HouseHoldsNight As Single, ByVal OccType As String, ByVal CB_ID As String, ByVal blocktype As String, ByVal prefirm As Byte, ByVal SchemeID As String, ByVal MSHConnection As OleDbConnection, ByVal HZCensusRec As OleDbDataReader, ByVal bndryconn As OleDbConnection) As HazusStructureAttributes()
        Dim HazusStructures(NumberOfStructures - 1) As HazusStructureAttributes
        '
        Dim OccTypeWithoutI As String = OccType.Substring(0, OccType.Length - 1)
        Dim ConstructionTypesResult As List(Of String) = GetConstructionTypes(NumberOfStructures, HZCensusRec.Item("BldgSchemesId").ToString, OccTypeWithoutI, MSHConnection)
        Dim FoundationTypesResult As List(Of FoundationHeightAndType) = GetFoundationTypeandHeight(NumberOfStructures, blocktype, OccTypeWithoutI, prefirm, SchemeID, MSHConnection, bndryconn)
        Dim StructureValues() As Single = GetTotalStructureValues(NumberOfStructures, CB_ID, OccType, bndryconn)
        Dim ContentValues() As Single = GetTotalContentValues(NumberOfStructures, CB_ID, OccType, bndryconn)

        System.Threading.Tasks.Parallel.For(0, NumberOfStructures, Sub(i As Int32)

                                                                       If FoundationTypesResult(i).FoundationType = "Basement" Then
                                                                           HazusStructures(i) = New HazusStructureAttributes With {.Name = OccTypeWithoutI & " " & _counter + i, .DamCat = DamageCategory, .OccType = OccTypeWithoutI, .Stories = NumberOfStories, .basement = "Yes", .HouseHoldsDay = HouseholdsDay, _
                                                                                                                               .HouseHoldsNight = HouseHoldsNight, .bldgtype = ConstructionTypesResult(i), .FoundationType = FoundationTypesResult(i).FoundationType, .foundationheight = FoundationTypesResult(i).FoundationHeight, .StructVal = StructureValues(i), .contentval = ContentValues(i)}
                                                                       Else
                                                                           HazusStructures(i) = New HazusStructureAttributes With {.Name = OccTypeWithoutI & " " & _counter + i, .DamCat = DamageCategory, .OccType = OccTypeWithoutI, .Stories = NumberOfStories, .basement = "No", .HouseHoldsDay = HouseholdsDay, _
                                                                                                                                                                                                          .HouseHoldsNight = HouseHoldsNight, .bldgtype = ConstructionTypesResult(i), .FoundationType = FoundationTypesResult(i).FoundationType, .foundationheight = FoundationTypesResult(i).FoundationHeight, .StructVal = StructureValues(i), .contentval = ContentValues(i)}
                                                                       End If
                                                                       If Left(OccType, 4) = "RES3" Then
                                                                           HazusStructures(i).OccType = OccType
                                                                       End If
                                                                   End Sub)
        _counter += NumberOfStructures
        Return HazusStructures
    End Function
    'Private Function GetMultiPolyPoints(ByVal PolygonFeature As PolygonFeature, ByVal Nstructures As Int32, ByVal index As Int32, ByRef successfulpoints As Integer) As List(Of PointD)
    '    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    '    'Dimension Variables
    '    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    '    Dim Result As New List(Of PointD), randy As New Random(1)
    '    Dim TriangleWeights(0) As Double, TriangleValues(0) As Int32, PolyWeights(0) As Double, PolyAreas(0) As Double, PolyStructures(0) As Int32
    '    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    '    'Triangulate the polygon feature
    '    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    '    Dim Points As New List(Of PointD)
    '    For Each Polygon As LifeSimGIS.Polygon In PolygonFeature.PolygonFeature
    '        Points.AddRange(Polygon.LinePoints)
    '    Next

    '    Dim TriangulatedPolygons As PolygonSet = PolygonFeature.TriangulateFeature

    '    Dim PolyArea As Double = 0
    '    Dim I0, I1, I2 As Int32
    '    ReDim PolyAreas(TriangulatedPolygons.Polygons.Count - 1)
    '    Dim TriAreas(TriangulatedPolygons.Polygons.Count - 1) As List(Of Double), a As Double, b As Double
    '    For i As Int32 = 0 To TriangulatedPolygons.Polygons.Count - 1
    '        TriAreas(i) = New List(Of Double)
    '        For Each Tindex As Int32() In TriangulatedPolygons.Polygons(i).TriangleIndices
    '            I0 = Tindex(0)
    '            I1 = Tindex(1)
    '            I2 = Tindex(2)

    '            a = (Points(I0).X - Points(I2).X) * (Points(I1).Y - Points(I0).Y)
    '            b = (Points(I0).X - Points(I1).X) * (Points(I2).Y - Points(I0).Y)
    '            TriAreas(i).Add(0.5 * Math.Abs(a - b))
    '            PolyAreas(i) += TriAreas(i).Last
    '        Next
    '        PolyArea += PolyAreas(i)
    '    Next
    '    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    '    'set and distribute the points.
    '    '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
    '    'calculate the Number of structures per polygon
    '    ReDim PolyStructures(TriangulatedPolygons.Polygons.Count - 1)
    '    ReDim PolyWeights(TriangulatedPolygons.Polygons.Count - 1)
    '    For i As Int32 = 0 To TriangulatedPolygons.Polygons.Count - 1
    '        PolyWeights(i) = PolyAreas(i) / PolyArea
    '    Next
    '    PolyStructures = ValueArray(PolyWeights, Nstructures)

    '    'generate the points
    '    For i As Int32 = 0 To TriangulatedPolygons.Polygons.Count - 1
    '        ReDim TriangleWeights(TriangulatedPolygons.Polygons(i).TriangleIndices.Count - 1)
    '        ReDim TriangleValues(TriangulatedPolygons.Polygons(i).TriangleIndices.Count - 1)
    '        'get cdf of weights
    '        For j As Int32 = 0 To TriangulatedPolygons.Polygons(i).TriangleIndices.Count - 1
    '            TriangleWeights(j) = TriAreas(i)(j) / PolyAreas(i)
    '        Next
    '        TriangleValues = ValueArray(TriangleWeights, PolyStructures(i))
    '        For j As Int32 = 0 To TriangulatedPolygons.Polygons(i).TriangleIndices.Count - 1
    '            I0 = TriangulatedPolygons.Polygons(i).TriangleIndices(j)(0)
    '            I1 = TriangulatedPolygons.Polygons(i).TriangleIndices(j)(1)
    '            I2 = TriangulatedPolygons.Polygons(i).TriangleIndices(j)(2)

    '            For k As Int32 = 1 To TriangleValues(j)
    '                Dim SamplePoint As PointD = RandPointInTriangle(randy, Points(I0), Points(I1), Points(I2))
    '                successfulpoints += 1
    '                Result.Add(SamplePoint)
    '            Next
    '        Next
    '    Next
    '    '
    '    Return Result
    'End Function
    Private Function GetMultiPolyPointsTesselate(ByVal PolygonFeature As PolygonFeature, ByVal Nstructures As Int32) As List(Of PointD)
        '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Dimension Variables
        '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim Result As New List(Of PointD), randy As New Random(1)
        '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'Triangulate the polygon feature
        '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim Points As New List(Of PointD)
        For Each Polygon As LifeSimGIS.Polygon In PolygonFeature.PolygonFeature
            Points.AddRange(Polygon.LinePoints)
        Next
        '
        Dim Tesselate() As Int32 = PolygonFeature.TessellateFeature
        If Tesselate.Count = 0 Then Throw New Exception("Polygon tessellation did not generate any triangles.")
        '
        Dim a, b, PolyArea As Double
        Dim TriangleAreas As New List(Of Double)
        For i As Int32 = 0 To Tesselate.Count - 1 Step 3
            a = (Points(Tesselate(i)).X - Points(Tesselate(i + 2)).X) * (Points(Tesselate(i + 1)).Y - Points(Tesselate(i)).Y)
            b = (Points(Tesselate(i)).X - Points(Tesselate(i + 1)).X) * (Points(Tesselate(i + 2)).Y - Points(Tesselate(i)).Y)
            TriangleAreas.Add(0.5 * Math.Abs(a - b))
            PolyArea += TriangleAreas.Last
        Next
        '
        Dim TriangleWeights(TriangleAreas.Count - 1) As Double
        For i As Int32 = 0 To TriangleAreas.Count - 1
            TriangleWeights(i) = TriangleAreas(i) / PolyArea
        Next
        '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        'set and distribute the points.
        '''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''''
        Dim TriangleStructures() As Int32 = ValueArray(TriangleWeights, Nstructures)
        Dim Point1, Point2, Point3 As PointD
        For i As Int32 = 0 To TriangleStructures.Count - 1
            Point1 = Points(Tesselate(i * 3))
            Point2 = Points(Tesselate(i * 3 + 1))
            Point3 = Points(Tesselate(i * 3 + 2))
            For j As Int32 = 1 To TriangleStructures(i)
                Result.Add(RandPointInTriangle(randy, Point1, Point2, Point3))
            Next
        Next
        '
        Return Result
    End Function
    Private Function GetPointsGridCells(ByVal PolygonFeature As PolygonFeature, ByVal Nstructures As Int32) As List(Of PointD)
        Dim GridCellList As New List(Of PointD)
        Dim Density As Double = Nstructures / PolygonFeature.Area
        Dim AreaPerStructure As Double = 1 / Density
        '
        Dim GridLocation As PointD = New PointD
        Dim GridCellWidth, GridCellHeight As Double
        Dim TestPoint As PointD
        Dim Counter As Int32 = 0
        While GridCellList.Count < Nstructures
            GridCellWidth = Math.Sqrt(AreaPerStructure)
            GridCellHeight = GridCellWidth
            '
            GridCellList.Clear()
            '
            For x As Double = PolygonFeature.Extent.MinX To (PolygonFeature.Extent.MaxX - GridCellWidth) Step GridCellWidth
                For y As Double = PolygonFeature.Extent.MinY To (PolygonFeature.Extent.MaxY - GridCellHeight) Step GridCellHeight
                    TestPoint = New PointD(x + GridCellWidth / 2, y + GridCellHeight / 2)
                    If PolygonFeature.PointInPolygonFeature(TestPoint) Then GridCellList.Add(TestPoint)
                Next
            Next
            '
            AreaPerStructure *= 0.97 'incase we did not create the correct number of gridcells, make the area/structure smaller
            Counter += 1
            If Counter = 1000 And GridCellList.Count = 0 Then
                Return GridCellList
            End If
        End While
        '
        Dim remove As Integer = GridCellList.Count - Nstructures
        Dim r As New Random(1234)
        For i = 0 To remove - 1
            GridCellList.RemoveAt(r.Next(Nstructures - 1))
        Next
        Return GridCellList
        '
    End Function
    Private Function RandPointInTriangle(ByRef randy As Random, ByVal p1 As PointD, ByVal p2 As PointD, ByVal p3 As PointD) As PointD
        'Dim randy As New Random(1)
        Dim r As Double = randy.NextDouble, s As Double = randy.NextDouble
        'If r + s > 1 Then
        '    r = 1 - r
        '    s = 1 - s
        'End If

        Dim X As Double = (1 - Math.Sqrt(r)) * p1.X + (Math.Sqrt(r) * (1 - s)) * p2.X + (s * Math.Sqrt(r)) * p3.X
        Dim Y As Double = (1 - Math.Sqrt(r)) * p1.Y + (Math.Sqrt(r) * (1 - s)) * p2.Y + (s * Math.Sqrt(r)) * p3.Y

        'Dim X As Double = p1.X + r * (p2.X - p1.X) + s * (p3.X - p1.X)
        'Dim Y As Double = p1.Y + r * (p2.Y - p1.Y) + s * (p3.Y - p1.Y)

        Return New PointD(CSng(X), CSng(Y))
    End Function
    Private Class FoundationHeightAndType
        Public FoundationType As String
        Public FoundationHeight As Single
    End Class
    Private Class SchemeIDAndEntryYear
        Public SchemeID As String
        Public EntryYear As Integer
    End Class
    Private Class HazusStructureAttributes
        Public Name As String
        Public FoundationType As String
        Public DamCat As String, OccType As String, Stories As Int16, basement As String, bldgtype As String, foundationheight As Single
        Public pop2amu65 As Int32, pop2amo65 As Int32, pop2pmu65 As Int32, pop2pmo65 As Int32
        Public StructVal, contentval, otherval, vehicval As Single
        Public HouseHoldsDay, HouseHoldsNight As Single
    End Class
End Class
