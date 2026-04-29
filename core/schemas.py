# core/schemas.py

"""
CATÁLOGO DE METADATOS DE TERADATA
Este archivo es la única fuente de la verdad para la estructura del diccionario de datos.
"""

# ==========================================
# 1. DBC.StatsV
# ==========================================
"""
DDL ORIGINAL:

REPLACE VIEW DBC.StatsV AS
SELECT  DBC.DBase.DatabaseName (NAMED DatabaseName)
       ,DBC.TVM.TVMName (NAMED TableName)
       ,DBC.StatsTbl.ExpressionList (NAMED ColumnName)
       ,DBC.StatsTbl.FieldIdList
       ,DBC.StatsTbl.StatsName
       ,DBC.StatsTbl.ExpressionCount
       ,DBC.StatsTbl.StatsId
       ,DBC.StatsTbl.StatsType
       ,DBC.StatsTbl.StatsSource
       ,DBC.StatsTbl.ValidStats
       ,DBC.StatsTbl.DBSVersion
       ,DBC.StatsTbl.IndexNumber
       ,COALESCE(DBC.StatsTbl.SampleSignature 
                ,'Global Default')  (NAMED SampleSignature)
       ,DBC.StatsTbl.SampleSizePct
       ,COALESCE(DBC.StatsTbl.ThresholdSignature
                ,'Global Default')  (NAMED ThresholdSignature)
       ,DBC.StatsTbl.MaxIntervals
       ,DBC.StatsTbl.MaxValueLength
       ,DBC.StatsTbl.RowCount
       ,DBC.StatsTbl.UniqueValueCount
       ,DBC.StatsTbl.PNullUniqueValueCount
       ,DBC.StatsTbl.NullCount
       ,DBC.StatsTbl.AllNullCount
       ,DBC.StatsTbl.HighModeFreq
       ,DBC.StatsTbl.PNullHighModeFreq
       ,DBC.StatsTbl.StatsSkipCount
       ,DBC.StatsTbl.CreateTimeStamp
       ,COALESCE(DBC.StatsTbl.LastCollectTimeStamp
                ,DBC.StatsTbl.CreateTimeStamp) (NAMED LastCollectTimeStamp)
       ,COALESCE(DBC.StatsTbl.LastAlterTimeStamp
                ,DBC.StatsTbl.CreateTimeStamp) (NAMED LastAlterTimeStamp)
       ,DBC.StatsTbl.BLCCompRatio
       ,DBC.StatsTbl.Reserved3 (NAMED AvgRowSize)
       ,DBC.StatsTbl.Reserved4 (NAMED BLCCompFactor)

FROM     DBC.StatsTbl
        ,DBC.Dbase
        ,DBC.TVM

WHERE    DBC.StatsTbl.DatabaseId = DBC.DBASE.DatabaseId
AND      DBC.StatsTbl.ObjectId = DBC.TVM.TVMId
AND      DBC.StatsTbl.StatsType IN
                ('T'  /*Tables*/
               , 'I' /*Join Index*/
               , 'N' /*HashIndex*/
               , 'B' /*Base Temporary Table*/
               , 'V') /*View Stats*/
AND      DBC.TVM.TableKind IN 
                ('T'  /*Perm and Base Temp Tables*/
               , 'I'  /*Join Index*/
               , 'N'  /*HashIndex*/
               , 'O'  /*NOPI Table*/
               , 'Q'  /*Queue Table*/
               , 'V') /*View Stats*/
UNION ALL
/*For Materialized temp tables*/
SELECT  DBC.DBase.DatabaseName (NAMED DatabaseName)
       ,DBC.TVM.TVMName (NAMED TableName)
       ,DBC.StatsTbl.ExpressionList (NAMED ColumnName)
       ,DBC.StatsTbl.FieldIdList
       ,DBC.StatsTbl.StatsName
       ,DBC.StatsTbl.ExpressionCount
       ,DBC.StatsTbl.StatsId
       ,DBC.StatsTbl.StatsType
       ,DBC.StatsTbl.StatsSource
       ,DBC.StatsTbl.ValidStats
       ,DBC.StatsTbl.DBSVersion
       ,DBC.StatsTbl.IndexNumber
       ,COALESCE(DBC.StatsTbl.SampleSignature
                ,'Global Default')  (NAMED SampleSignature)
       ,DBC.StatsTbl.SampleSizePct
       ,COALESCE(DBC.StatsTbl.ThresholdSignature
                ,'Global Default')  (NAMED ThresholdSignature)
       ,DBC.StatsTbl.MaxIntervals
       ,DBC.StatsTbl.MaxValueLength
       ,DBC.StatsTbl.RowCount
       ,DBC.StatsTbl.UniqueValueCount
       ,DBC.StatsTbl.PNullUniqueValueCount
       ,DBC.StatsTbl.NullCount
       ,DBC.StatsTbl.AllNullCount
       ,DBC.StatsTbl.HighModeFreq
       ,DBC.StatsTbl.PNullHighModeFreq
       ,DBC.StatsTbl.StatsSkipCount
       ,DBC.StatsTbl.CreateTimeStamp
       ,COALESCE(DBC.StatsTbl.LastCollectTimeStamp
                ,DBC.StatsTbl.CreateTimeStamp) (NAMED LastCollectTimeStamp)
       ,COALESCE(DBC.StatsTbl.LastAlterTimeStamp
                ,DBC.StatsTbl.CreateTimeStamp) (NAMED LastAlterTimeStamp)
       ,DBC.StatsTbl.BLCCompRatio
       ,DBC.StatsTbl.Reserved3 (NAMED AvgRowSize)
       ,DBC.StatsTbl.Reserved4 (NAMED BLCCompFactor)

FROM     DBC.StatsTbl
        ,DBC.TempTables
        ,DBC.Dbase
        ,DBC.TVM

WHERE    DBC.StatsTbl.DatabaseId = DBC.DBASE.DatabaseId
AND      DBC.StatsTbl.ObjectId = DBC.TempTables.TableId
AND      DBC.TempTables.BaseTableId = TVM.TVMId
AND      DBC.TempTables.SessionNo = SESSION /*Only show tables for the current session*/
AND      DBC.StatsTbl.StatsType = 'M'  /*Materialized Temp Tables*/
AND      (DBC.TVM.TableKind ='T'     /*Tables*/
          OR DBC.TVM.TableKind ='O'     /*NOPI Tables*/
          OR DBC.TVM.TableKind ='Q')    /*QUEUE Tables*/
;
"""


# ==========================================
# 2. DBC.TablesV
# ==========================================
"""
DDL ORIGINAL:
REPLACE VIEW DBC.TablesV
AS SELECT dbase.DatabaseName  (NAMED DataBaseName),
          tvm.TVMName  (NAMED TableName),
          tvm.Version(FORMAT 'zzzz(9)'),
          CAST(tvm.TableKind AS CHAR(1)) (NAMED TableKind),
          tvm.ProtectionType,
          tvm.JournalFlag,
          coalesce(DB1.DatabaseName ,DBC.tvm.CreatorName )(named CreatorName),
          tvm.RequestText,
          tvm.CommentString,
          tvm.ParentCount,            
          tvm.ChildCount,           
          tvm.NamedTblCheckCount,     
          tvm.UnnamedTblCheckExist,   
          tvm.PrimaryKeyIndexId,
          tvm.TblStatus(NAMED RepStatus),
          tvm.CreateTimeStamp,
          DB2.DatabaseName (named LastAlterName),
          tvm.LastAlterTimeStamp,
          tvm.RequestTxtOverFlow,
          OU.UserAccessCnt AS AccessCount,
          OU.LastAccessTimeStamp,
          tvm.UtilVersion (FORMAT '-----9'),
          tvm.QueueFlag,
          tvm.CommitOpt,
          tvm.TransLog,
          tvm.CheckOpt,
          tvm.TemporalProperty, 
          tvm.ResolvedCurrent_Date, 
          tvm.ResolvedCurrent_Timestamp, 
          tvm.SystemDefinedJI, 
          tvm.VTQualifier, 
          tvm.TTQualifier,
          tvm.PIColumnCount,
          tvm.PartitioningLevels,
          tvm.LoadProperty,
          tvm.CurrentLoadId,
          tvm.LoadIdLayout,
          tvm.DelayedJI,
          tvm.LastArchiveId,
          tvm.LastFullArchiveId,
          tvm.BlockSize (FORMAT 'Z,ZZZ,ZZZ,ZZ9'),
          tvm.FreeSpacePercent (FORMAT 'Z9%'),
          tvm.MergeBlockRatio (FORMAT 'ZZ9%'),
          tvm.CheckSum,
          (CASE tvm.BlockCompression 
            WHEN 0 THEN 'DEFAULT'
            WHEN 1 THEN 'MANUAL'
            WHEN 2 THEN 'AUTOTEMP' 
            WHEN 3 THEN 'NEVER'
            WHEN 4 THEN 'ALWAYS'
              ELSE NULL 
          END) (NAMED BlockCompression),
          (CASE tvm.BlockCompressionAlgorithm 
            WHEN 0 THEN 'DEFAULT'
            WHEN 1 THEN 'ZLIB'
            WHEN 2 THEN 'ELZS_H' 
              ELSE NULL 
              END) (NAMED BlockCompressionAlgorithm),
          (CASE tvm.BlockCompressionLevel
            WHEN 0 THEN 'DEFAULT'
            WHEN 1 THEN '1'
            WHEN 2 THEN '2'
            WHEN 3 THEN '3'
            WHEN 4 THEN '4'
            WHEN 5 THEN '5'
            WHEN 6 THEN '6'
            WHEN 7 THEN '7'
            WHEN 8 THEN '8'
            WHEN 9 THEN '9'
              ELSE NULL
              END) (NAMED BlockCompressionLevel),
          tvm.TableHeaderFormat,
          (CASE WHEN tvm.RowSizeFormat IS NULL THEN '0'
             ELSE tvm.RowSizeFormat END)(NAMED RowSizeFormat),
          Maps.MapName,
          tvm.ColocationName,           
          tvm.TVMFlavor (named TVMFlavor),
          tvm.FastAlterTable (NAMED FastAlterTable),
          tvm.IncrementalRestoreEnabled,
          tvm.AuthName
FROM DBC.tvm
         LEFT OUTER JOIN DBC.Dbase DB1
                      ON DBC.tvm.CreatorName = DB1.DatabaseNameI
         LEFT OUTER JOIN DBC.Dbase DB2
                      ON DBC.tvm.LastAlterUID = DB2.DatabaseID
         LEFT OUTER JOIN DBC.ObjectUsage OU
                      ON OU.DatabaseId = DBC.TVM.DatabaseId
                     AND OU.ObjectId = DBC.TVM.TVMId
                     AND OU.FieldId IS NULL
                     AND OU.IndexNumber IS NULL
         LEFT OUTER JOIN DBC.Maps 
                      ON DBC.TVM.MapNo = DBC.Maps.MapNo,
         DBC.dbase
WHERE tvm.DatabaseId = dbase.DatabaseId 
  AND tvm.tvmid NOT IN ( '00C001000000'xb, '00C002000000'xb,
                         '00C009000000'xb, '00C010000000'xb,
                         '00C017000000'xb)
WITH CHECK OPTION;
"""


# ==========================================
# 3. DBC.TableSizeV
# ==========================================
"""
DDL ORIGINAL:
REPLACE VIEW DBC.TableSizeV
AS SELECT
         DataBaseSpace.Vproc,
         Dbase.DatabaseName  (NAMED DataBaseName),
         Dbase.AccountName  (NAMED AccountName),
         TVM.TVMName  (NAMED TableName),
         DataBaseSpace.CurrentPermSpace(NAMED CurrentPerm),
         DataBaseSpace.PeakPermSpace(NAMED PeakPerm)
FROM  DBC.Dbase, DBC.DataBaseSpace, DBC.TVM
WHERE DataBaseSpace.TableID <> '000000000000'XB
 AND  DataBaseSpace.TableID = TVM.tvmid
 AND  TVM.DatabaseId = Dbase.DatabaseId
 AND  TVM.TableKind NOT IN ('G','M','V')
   WITH CHECK OPTION;
"""



# ==========================================
# 4. DBC.ObjectUsage
# ==========================================
"""
DDL ORIGINAL:
CREATE SET TABLE DBC.ObjectUsage ,FALLBACK ,
     NO BEFORE JOURNAL,
     NO AFTER JOURNAL,
     CHECKSUM = DEFAULT,
     DEFAULT MERGEBLOCKRATIO,
     MAP = TD_DATADICTIONARYMAP
     (
      DatabaseId BYTE(4) NOT NULL,
      ObjectId BYTE(6),
      FieldId INTEGER FORMAT '--,---,---,--9',
      IndexNumber SMALLINT FORMAT '---,--9',
      UsageType CHAR(3) CHARACTER SET LATIN UPPERCASE NOT CASESPECIFIC NOT NULL,
      UserAccessCnt BIGINT FORMAT '--,---,---,---,---,---,--9',
      SysAccessCnt BIGINT FORMAT '--,---,---,---,---,---,--9',
      UserUpdateCnt BIGINT FORMAT '--,---,---,---,---,---,--9',
      UserDeleteCnt BIGINT FORMAT '--,---,---,---,---,---,--9',
      UserInsertCnt BIGINT FORMAT '--,---,---,---,---,---,--9',
      SysUpdateCnt BIGINT FORMAT '--,---,---,---,---,---,--9',
      SysDeleteCnt BIGINT FORMAT '--,---,---,---,---,---,--9',
      SysInsertCnt BIGINT FORMAT '--,---,---,---,---,---,--9',
      LastAccessTimeStamp TIMESTAMP(0),
      LastUsrAccessCntResetTimeStamp TIMESTAMP(0),
      LastSysAccessCntResetTimeStamp TIMESTAMP(0),
      LastUsrUDIResetTimeStamp TIMESTAMP(0),
      LastSysUDIResetTimeStamp TIMESTAMP(0))
PRIMARY INDEX ( DatabaseId ,ObjectId );
"""

# ==========================================
# 5. DBC.DBQLogTbl_Hst
# ==========================================
"""
DDL ORIGINAL:
REPLACE VIEW PDCRINFO.DBQLogTbl_Hst AS
  LOCKING ROW FOR ACCESS
 SELECT
 LogDate,
ProcID,
CollectTimeStamp,
QueryID,
UserID,
ZoneID,
AcctString,
ExpandAcctString,
SessionID,
LogicalHostID,
RequestNum,
InternalRequestNum,
LogonDateTime,
AcctStringTime,
AcctStringHour,
AcctStringDate,
LogonSource,
AppID,
ClientID,
ClientAddr,
QueryBand,
ProfileID,
StartTime,
FirstStepTime,
FirstRespTime,
LastStateChange,
NumSteps,
NumStepswPar,
MaxStepsInPar,
NumResultRows,
NumResultOneMBRows,             /*DR153296-cy120587-01*/
MaxOneMBRowSize,                /*DR153296-cy120587-01*/
TotalIOCount,
AMPCPUTime,
ParserCPUTime,
UtilityByteCount,
UtilityRowCount,
ErrorCode,
ErrorText,
WarningOnly,
DelayTime,
AbortFlag,
CacheFlag,
StatementType,
QueryText,
NumOfActiveAMPs,
MaxAMPCPUTime,
MaxCPUAmpNumber,
MinAmpCPUTime,
MaxAmpIO,
MaxIOAmpNumber,
MinAmpIO,
SpoolUsage,
WDID,
OpEnvID,
SysConID,
WDName,
OpEnvName,
SysConName,
FinalWDName,
LSN,
NoClassification,
WDOverride,
ResponseTimeMet,
ExceptionValue,
FinalWDID,
TDWMEstMaxRows,
TDWMEstLastRows,
TDWMEstTotalTime,
TDWMAllAmpFlag,
TDWMConfLevelUsed,
TDWMRuleID,
UserName,
DefaultDatabase,
AMPCPUTimeNorm,
ParserCPUTimeNorm,
MaxAMPCPUTimeNorm,
MaxCPUAmpNumberNorm,
MinAmpCPUTimeNorm,
EstResultRows,
EstProcTime,
EstMaxRowCount,
ProxyUser,
ProxyRole,
StatementGroup,
SessionTemporalQualifier,
CalendarName,
SessionWDID,
DataCollectAlg,
ParserExpReq,
CallNestingLevel,
NumRequestCtx,
KeepFlag,
QueryRedriven,
ReDriveKind,
CPUDecayLevel,
IODecayLevel,
TacticalCPUException,
TacticalIOException,
SeqRespTime,
ReqIOKB,
ReqPhysIO,
ReqPhysIOKB,
NumFragments,               
CheckpointNum,              
UnityTime,                  
LockDelay,                  
LastRespTime,               
DisCPUTime,                 
Statements,                 
DisCPUTimeNorm,             
TxnMode,                    
RequestMode,                
UtilityInfoAvailable,       
UnitySQL,                   
ThrottleBypassed,           
FlexThrottle,
DBQLStatus,                 
IterationCount,             
VHLogicalIO,               
VHPhysIO,                   
VHLogicalIOKB,              
VHPhysIOKB,                 
TDWMEstMemUsage,            
MaxStepMemory,
TotalServerByteCount,
ProxyUserID,              
TxnUniq,                    
LockLevel,                  
TTGranularity,
ProfileName,
EstMaxStepTime,
ParamQuery,
RemoteQuery,
PersistentSpool,
MinRespHoldTime,
TotalFirstRespTime,
MaxNumMapAMPs,
MinNumMapAMPs,
SysDefNumMapAMPs,
FeatureUsage,
ReqMaxSpool,
NumAmpsImpacted,
UsedIota,
SessionWDName,
MaxAmpsMapNo, 
AutoDBAData,      /* -> tn122476-DR189772-01: DBQLogTbl  */
UAFName,
UnityQueryType,
TacticalRequest,
DefaultDBCacheUsed,
ReqAWTTime,
MaxReqAwtTime,
MaxReqAWTTimeAmpNum,
MinReqAWTTime,
UDFVMData,
UDFVMPeak,
TotalUDFMemUsage,
MaxReqUDFMemUsage,
MaxReqUDFMemUsageAmpNum,
PGRCTimeToGetPlan,
DeferTime,
NosRecordsReturned,
NosRecordsSkipped,
NosPhysReadIO,
NosPhysReadIOKB,
NosRecordsReturnedKB,
NosTotalIOWaitTime,
NosMaxIOWaitTime,
NosCPUTime,
NosTables,
NosFiles,
NosFilesSkipped,
DeferRuleID,
StepCacheHash,
TDWMMSRCount,
TDWMAdmissionTime,
StmtDMLRowCount,
UnityQueryForeignInfo, 
NumJoinSteps,
NumSumSteps,
PGRCTgtPENum,               /* <- tn122476-DR189772-01  */
ExtraField1,                
ExtraField2,                
ExtraField3,                
ExtraField4,                
ExtraField5,                
ExtraField6,                
ExtraField7,                
ExtraField8,                
ExtraField9,                
ExtraField10,               
ExtraField11,               
ExtraField12,               
ExtraField13,               
ExtraField14,               
ExtraField15,               
ExtraField16,               
ExtraField17,               
ExtraField18,               
ExtraField19,               
ExtraField20,               
ExtraField21,               
ExtraField22,               
ExtraField23,               
ExtraField24,               
ExtraField25,               
ExtraField26,               
ExtraField27,               
ExtraField28,               
ExtraField29,               
ExtraField30,               
ExtraField31,               
ExtraField32,               
ExtraField33,               
ExtraField34,               
ExtraField35,               
ExtraField36,               
ExtraField37,               
ExtraField38,               
ExtraField39,               
ExtraField40,               
ExtraField41,               
ExtraField42,               
ExtraField43,               
ExtraField44,               
ExtraField45,               
ExtraField46,               
ExtraField47,               
ExtraField48,               
ExtraField49,               
ExtraField50,               
ExtraField51,               
ExtraField52,               
ExtraField53,               
ExtraField54,
ExtraField55,
ExtraField56,  /* tn122476-DR189772-01 -> */
ExtraField57,
ExtraField58,
ExtraField59,
ExtraField60,
ExtraField61,
ExtraField62,
ExtraField63   /* <- tn122476-DR189772-01  */
From PDCRDATA.DBQLogTbl_Hst;
"""

# ==========================================
# 6. DBC.dbqlsqltbl_hst
# ==========================================
"""
DDL ORIGINAL:
REPLACE VIEW PDCRINFO."DBQLSqlTbl_Hst" AS
 LOCKING ROW FOR ACCESS
SELECT
"LogDate"
,"ProcID"
,"CollectTimeStamp"
,"QueryID"
,"ZoneID"
,"SqlRowNo"
,"SqlTextInfo"
,"ExtraField1"
,"ExtraField2"
,"ExtraField3"
 From PDCRDATA."DBQLSqlTbl_Hst";
"""

# ==========================================
# 7. DBC.DBQLObjTbl_Hst
# ==========================================
"""
DDL ORIGINAL:
REPLACE VIEW PDCRINFO."DBQLObjTbl_Hst" AS
 LOCKING ROW FOR ACCESS
SELECT
"LogDate"
,"ProcID"
,"CollectTimeStamp"
,"QueryID"
,"ZoneID"
,"ObjectDatabaseName"
,"ObjectTableName"
,"ObjectColumnName"
,"ObjectID"
,"ObjectNum"
,"ObjectType"
,"FreqofUse"
,"TypeofUse"
,"ExtraField1"     
,"ExtraField2"    
,"ExtraField3"     
,"ExtraField4"     
,"ExtraField5"
,"ExtraField6"     
 From PDCRDATA."DBQLObjTbl_Hst";
"""


# Función de utilidad para validar DataFrames
def validate_columns(df, expected_columns, view_name):
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        raise ValueError(f"Faltan columnas en {view_name}: {missing}")
    return True