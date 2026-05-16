COLLECT STATISTICS
COLUMN (TvmId)
, COLUMN (UserId)
, COLUMN (DatabaseId)
, COLUMN (FieldId)
, COLUMN (AccessRight)
, COLUMN (GrantorID)
, COLUMN (CreateUID)
, COLUMN (UserId ,DatabaseId)
, COLUMN (TvmId ,DatabaseId)
, COLUMN (TvmId ,UserId)
, COLUMN (DatabaseId, AccessRight)
, COLUMN (TvmId, AccessRight)
, COLUMN (FieldId, AccessRight)
, COLUMN (AccessRight, CreateUID)
, COLUMN (AccessRight, GrantorID)
, COLUMN (TvmId ,DatabaseId, UserId)
, COLUMN (AccessRight, UserId, DatabaseId)
, COLUMN (AccessRight, UserId)
, COLUMN (Partition, UserId)
, COLUMN (UserId, FieldId)
, COLUMN (PARTITION)
ON DBC.AccessRights;

COLLECT STATISTICS
COLUMN (ProfileName)
, COLUMN (ProxyUser)
, COLUMN (TrustUserID)
, COLUMN (TrustUserID,ProxyUser)
, COLUMN (ProxyUserID,GrantStatus)
ON DBC.ConnectRulesTbl ;

COLLECT STATISTICS
COLUMN (DatabaseId)
, COLUMN DatabaseName
, COLUMN (DatabaseNameI)
, COLUMN (OwnerName)
, COLUMN (LastAlterUID)
, COLUMN (JournalId)
, COLUMN (ZoneID)
, COLUMN (ProfileName)
, COLUMN (DatabaseName,LastAlterUID)
ON DBC.Dbase;

COLLECT STATISTICS
COLUMN LogicalHostId
, INDEX ( HostName )
ON DBC.Hosts;

COLLECT STATISTICS
COLUMN OwnerID
, COLUMN (OwneeId)
, COLUMN (OwneeId ,OwnerId)
ON DBC.Owners;

COLLECT STATISTICS
COLUMN (RoleId)
, COLUMN (RoleNameI)
, COLUMN (RoleName) 
ON DBC.Roles;

COLLECT STATISTICS
INDEX (GranteeId)
, COLUMN (RoleId)
, COLUMN (GrantorId)
ON DBC.RoleGrants;

COLLECT STATISTICS
COLUMN (ProfileId)
, COLUMN (ProfileName)
, COLUMN (ProfileNameI)
ON DBC.Profiles ;

COLLECT STATISTICS
COLUMN (TableId)
, COLUMN (FieldId)
, COLUMN (FieldName)
, COLUMN (FieldType)
, COLUMN (DatabaseId)
, COLUMN (CreateUID)
, COLUMN (LastAlterUID)
, COLUMN (UDTName)
, COLUMN (TableId, FieldName)
, COLUMN (TableId, FieldID)
, COLUMN (TableId, FieldID, DatabaseId)
, COLUMN (TableId, DatabaseId)
, COLUMN (UDTypeId)
, COLUMN (ColumnCheck)
ON DBC.TVFields;

COLLECT STATISTICS
COLUMN TVMID
, COLUMN (TvmName)
, COLUMN (TvmNameI)
, COLUMN (DatabaseId)
, COLUMN (TableKind)
, COLUMN (CreateUid)
, COLUMN (CreatorName)
, COLUMN (LastAlterUid)
, COLUMN (CommitOpt)
, COLUMN (DatabaseId, TVMName)
, COLUMN (DatabaseId, TvmId)
, COLUMN (DatabaseId ,TvmNameI)
ON DBC.TVM;


COLLECT STATISTICS
INDEX (TableId)
, COLUMN (FieldId)
, COLUMN (IndexNumber)
, COLUMN (IndexType)
, COLUMN (UniqueFlag)
, COLUMN (CreateUID)
, COLUMN (JoinIndexTableId)
, COLUMN (LastAlterUID)
, COLUMN (TableId, DatabaseId)
, COLUMN (TableId, FieldId)
, COLUMN (UniqueFlag, FieldId)
, COLUMN (UniqueFlag, CreateUID)
, COLUMN (UniqueFlag, LastAlterUID)
, COLUMN (IndexType, SystemDefinedJi)
, COLUMN (IndexType, TableId)
, COLUMN (UniqueFlag, IndexType)
, COLUMN (UniqueFlag, IndexType, TableId)
, COLUMN (UniqueFlag, IndexType, TableId, FieldId)
, COLUMN (TableId, IndexNumber, FieldId, DatabaseId)
, COLUMN (IndexType, TableId, FieldId)
, COLUMN (TableId, IndexNumber, DatabaseId)
ON DBC.Indexes;

COLLECT STATISTICS
COLUMN (IndexNumber)
, COLUMN (StatsType)
ON DBC.StatsTbl;

COLLECT STATISTICS
COLUMN (ObjectId)   
, COLUMN (FieldId)
, COLUMN (DatabaseID)
, COLUMN (UsageType)
, COLUMN (IndexNumber)
, COLUMN (DatabaseId, ObjectId)
, COLUMN (DatabaseId, ObjectId, IndexNumber)
, COLUMN (ObjectId, FieldId)
, COLUMN (DatabaseId, ObjectId, FieldId)
, COLUMN (DatabaseId, ObjectId, FieldId, IndexNumber)
ON DBC.ObjectUsage;

COLLECT STATISTICS
INDEX (FunctionID )
, COLUMN DatabaseId
, COLUMN ( DatabaseId ,FunctionName )
ON DBC.UDFInfo;

COLLECT STATISTICS
COLUMN (TypeName)
, COLUMN (TypeKind)
ON DBC.UDTInfo;

COLLECT STATISTICS
COLUMN (ConstraintType)
, COLUMN (DBaseId)
, COLUMN (TVMId)
ON DBC.TableConstraints ;

COLLECT STATISTICS
COLUMN (BaseTableId)
ON DBC.TempTables ;

COLLECT STATISTICS
COLUMN (DatabaseId)
, COLUMN (SpoolOnly)
, COLUMN (PeakPermSpace)
, COLUMN (DatabaseId)
, COLUMN (TableId)
, INDEX (DatabaseId, TableId)
ON DBC.DatabaseSpace;

COLLECT STATISTICS
COLUMN (DatabaseId)
, COLUMN (SpoolOnly)
, COLUMN (SoftLimitPercent)
ON DBC.GlobalDBSpace;

COLLECT STATISTICS
COLUMN (MapNameI)
, COLUMN (MapNo)
, COLUMN (ParentSystemMapNo)
, COLUMN (CreatorId)
ON DBC.Maps;

COLLECT STATISTICS
COLUMN (GranteeId)
, COLUMN (GrantorId)
, COLUMN (MapNo)
ON DBC.MapGrants;