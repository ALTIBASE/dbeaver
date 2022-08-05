/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2022 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.altibase.model.o2a;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.*;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCCompositeCache;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectLookupCache;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCStructLookupCache;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSEntity;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureContainer;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;
import org.jkiss.dbeaver.model.struct.rdb.DBSSchema;
import org.jkiss.utils.ArrayUtils;
import org.jkiss.utils.CommonUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * AltibaseSchema
 */
public class AltibaseO2ASchema extends AltibaseO2AGlobalObject implements DBSSchema, DBPRefreshableObject, DBPSystemObject, DBSProcedureContainer, DBPObjectStatisticsCollector
{
    private static final Log log = Log.getLog(AltibaseO2ASchema.class);

    // Synonyms read is very expensive. Exclude them from children by default
    // Children are used in auto-completion which must be fast
    private static boolean SYNONYMS_AS_CHILDREN = false;

    final public TableCache tableCache = new TableCache();
    final public ConstraintCache constraintCache = new ConstraintCache();
    final public ForeignKeyCache foreignKeyCache = new ForeignKeyCache();
    final public TriggerCache triggerCache = new TriggerCache();
    final public TableTriggerCache tableTriggerCache = new TableTriggerCache();
    final public IndexCache indexCache = new IndexCache();
    final public DataTypeCache dataTypeCache = new DataTypeCache();
    final public SequenceCache sequenceCache = new SequenceCache();
    final public QueueCache queueCache = new QueueCache();
    final public PackageCache packageCache = new PackageCache();
    final public SynonymCache synonymCache = new SynonymCache();
    final public ProceduresCache proceduresCache = new ProceduresCache();
    private volatile boolean hasStatistics;

    private long id;
    private String name;
    private Date createTime;
    private transient AltibaseO2AUser user;

    public AltibaseO2ASchema(AltibaseO2ADataSource dataSource, long id, String name)
    {
        super(dataSource, id > 0);
        this.id = id;
        this.name = name;
    }

    public AltibaseO2ASchema(@NotNull AltibaseO2ADataSource dataSource, @NotNull ResultSet dbResult)
    {
        super(dataSource, true);
        this.id = JDBCUtils.safeGetLong(dbResult, "USER_ID");
        this.name = JDBCUtils.safeGetString(dbResult, "USER_NAME");
        if (CommonUtils.isEmpty(this.name)) {
            log.warn("Empty schema name fetched");
            this.name = "? " + super.hashCode();
        }
        this.createTime = JDBCUtils.safeGetTimestamp(dbResult, "CREATED");
        SYNONYMS_AS_CHILDREN = CommonUtils.getBoolean(dataSource.getContainer().getConnectionConfiguration().getProviderProperty(AltibaseO2AConstants.PROP_SEARCH_METADATA_IN_SYNONYMS));
    }

    public boolean isPublic()
    {
        return AltibaseO2AConstants.USER_PUBLIC.equals(this.name);
    }

    @Property(order = 200)
    public long getId()
    {
        return id;
    }

    @Property(order = 190)
    public Date getCreateTime() {
        return createTime;
    }

    @NotNull
    @Override
    @Property(viewable = true, editable = true, order = 1)
    public String getName()
    {
        return name;
    }

    public void setName(String name)
    {
        this.name = name;
    }

    @Nullable
    @Override
    public String getDescription()
    {
        return null;
    }

    /**
     * User reference never read directly from database.
     * It is used by managers to create/delete/alter schemas
     * @return user reference or null
     */
    public AltibaseO2AUser getUser()
    {
        return user;
    }

    public void setUser(AltibaseO2AUser user)
    {
        this.user = user;
    }

    @Association
    public Collection<AltibaseO2ATableIndex> getIndexes(DBRProgressMonitor monitor)
        throws DBException
    {
        return indexCache.getObjects(monitor, this, null);
    }

    @Association
    public Collection<AltibaseO2ATable> getTables(DBRProgressMonitor monitor)
        throws DBException
    {
        return tableCache.getTypedObjects(monitor, this, AltibaseO2ATable.class);
    }

    public AltibaseO2ATable getTable(DBRProgressMonitor monitor, String name)
        throws DBException
    {
        return tableCache.getObject(monitor, this, name, AltibaseO2ATable.class);
    }

    @Association
    public Collection<AltibaseO2AView> getViews(DBRProgressMonitor monitor)
        throws DBException
    {
        return tableCache.getTypedObjects(monitor, this, AltibaseO2AView.class);
    }

    public AltibaseO2AView getView(DBRProgressMonitor monitor, String name)
        throws DBException
    {
        return tableCache.getObject(monitor, this, name, AltibaseO2AView.class);
    }

    @Association
    public Collection<AltibaseO2AMaterializedView> getMaterializedViews(DBRProgressMonitor monitor)
        throws DBException
    {
        return tableCache.getTypedObjects(monitor, this, AltibaseO2AMaterializedView.class);
    }

    @Association
    public AltibaseO2AMaterializedView getMaterializedView(DBRProgressMonitor monitor, String name)
        throws DBException
    {
        return tableCache.getObject(monitor, this, name, AltibaseO2AMaterializedView.class);
    }

    public TableCache getTableCache() {
        return tableCache;
    }

    @Association
    public Collection<AltibaseO2ADataType> getDataTypes(DBRProgressMonitor monitor)
        throws DBException
    {
        return dataTypeCache.getAllObjects(monitor, this);
    }

    public AltibaseO2ADataType getDataType(DBRProgressMonitor monitor, String name)
        throws DBException
    {
        AltibaseO2ADataType type = isPublic() ? getTypeBySynonym(monitor, name) : dataTypeCache.getObject(monitor, this, name);
        if (type == null) {
            if (!isPublic()) {
                return getTypeBySynonym(monitor, name);
            }
        }
        return type;
    }

    @Nullable
    private AltibaseO2ADataType getTypeBySynonym(DBRProgressMonitor monitor, String name) throws DBException {
        final AltibaseO2ASynonym synonym = synonymCache.getObject(monitor, this, name);
        if (synonym != null && (synonym.getObjectType() == AltibaseO2AObjectType.TYPE || synonym.getObjectType() == AltibaseO2AObjectType.TYPE_BODY)) {
            Object object = synonym.getObject(monitor);
            if (object instanceof AltibaseO2ADataType) {
                return (AltibaseO2ADataType)object;
            }
        }
        return null;
    }

    @Association
    public Collection<AltibaseO2AQueue> getQueues(DBRProgressMonitor monitor)
        throws DBException
    {
        return queueCache.getAllObjects(monitor, this);
    }

    @Association
    public Collection<AltibaseO2ASequence> getSequences(DBRProgressMonitor monitor)
        throws DBException
    {
        return sequenceCache.getAllObjects(monitor, this);
    }

    @Association
    public Collection<AltibaseO2APackage> getPackages(DBRProgressMonitor monitor)
        throws DBException
    {
        return packageCache.getAllObjects(monitor, this);
    }

    @Association
    public Collection<AltibasevO2AProcedureStandalone> getProceduresOnly(DBRProgressMonitor monitor) throws DBException {
        return getProcedures(monitor)
            .stream()
            .filter(proc -> proc.getProcedureType() == DBSProcedureType.PROCEDURE)
            .collect(Collectors.toList());
    }

    @Association
    public Collection<AltibasevO2AProcedureStandalone> getFunctionsOnly(DBRProgressMonitor monitor) throws DBException {
        return getProcedures(monitor)
            .stream()
            .filter(proc -> proc.getProcedureType() == DBSProcedureType.FUNCTION)
            .collect(Collectors.toList());
    }

    @Association
    public Collection<AltibasevO2AProcedureStandalone> getProcedures(DBRProgressMonitor monitor)
        throws DBException
    {
        return proceduresCache.getAllObjects(monitor, this);
    }

    @Override
    public AltibasevO2AProcedureStandalone getProcedure(DBRProgressMonitor monitor, String uniqueName) throws DBException {
        return proceduresCache.getObject(monitor, this, uniqueName);
    }

    @Association
    public Collection<AltibaseO2ASynonym> getSynonyms(DBRProgressMonitor monitor)
        throws DBException
    {
        return synonymCache.getAllObjects(monitor, this);
    }

    @Association
    public AltibaseO2ASynonym getSynonym(DBRProgressMonitor monitor, String name)
        throws DBException
    {
        return synonymCache.getObject(monitor, this, name);
    }

    @Association
    public Collection<AltibaseO2ASchemaTrigger> getTriggers(DBRProgressMonitor monitor)
        throws DBException
    {
        return triggerCache.getAllObjects(monitor, this);
    }

    @Association
    public Collection<AltibaseO2ATableTrigger> getTableTriggers(DBRProgressMonitor monitor)
            throws DBException
    {
        return tableTriggerCache.getAllObjects(monitor, this);
    }

    @Property(order = 90)
    public AltibaseO2AUser getSchemaUser(DBRProgressMonitor monitor) throws DBException {
        return getDataSource().getUser(monitor, name);
    }

    @Override
    public Collection<DBSObject> getChildren(@NotNull DBRProgressMonitor monitor)
        throws DBException
    {
        List<DBSObject> children = new ArrayList<>();
        children.addAll(tableCache.getAllObjects(monitor, this));
        if (SYNONYMS_AS_CHILDREN) {
            children.addAll(synonymCache.getAllObjects(monitor, this));
        }
        children.addAll(packageCache.getAllObjects(monitor, this));
        return children;
    }

    @Override
    public DBSObject getChild(@NotNull DBRProgressMonitor monitor, @NotNull String childName)
        throws DBException
    {
        final AltibaseO2ATableBase table = tableCache.getObject(monitor, this, childName);
        if (table != null) {
            return table;
        }
        if (SYNONYMS_AS_CHILDREN) {
            AltibaseO2ASynonym synonym = synonymCache.getObject(monitor, this, childName);
            if (synonym != null) {
                return synonym;
            }
        }
        return packageCache.getObject(monitor, this, childName);
    }

    @NotNull
    @Override
    public Class<? extends DBSEntity> getPrimaryChildType(@Nullable DBRProgressMonitor monitor)
        throws DBException
    {
        return AltibaseO2ATable.class;
    }

    @Override
    public synchronized void cacheStructure(@NotNull DBRProgressMonitor monitor, int scope)
        throws DBException
    {
        monitor.subTask("Cache tables");
        tableCache.getAllObjects(monitor, this);
        if ((scope & STRUCT_ATTRIBUTES) != 0) {
            monitor.subTask("Cache table columns");
            tableCache.loadChildren(monitor, this, null);
        }
        if ((scope & STRUCT_ASSOCIATIONS) != 0) {
            monitor.subTask("Cache table indexes");
            indexCache.getObjects(monitor, this, null);
            monitor.subTask("Cache table constraints");
            constraintCache.getObjects(monitor, this, null);
            foreignKeyCache.getObjects(monitor, this, null);
        }
    }

    @Override
    public synchronized DBSObject refreshObject(@NotNull DBRProgressMonitor monitor)
        throws DBException
    {
        hasStatistics = false;
        tableCache.clearCache();
        foreignKeyCache.clearCache();
        constraintCache.clearCache();
        indexCache.clearCache();
        packageCache.clearCache();
        proceduresCache.clearCache();
        triggerCache.clearCache();
        tableTriggerCache.clearCache();
        dataTypeCache.clearCache();
        sequenceCache.clearCache();
        synonymCache.clearCache();
        return this;
    }

    @Override
    public boolean isSystem()
    {
        return ArrayUtils.contains(AltibaseO2AConstants.SYSTEM_SCHEMAS, getName());
    }

    @Override
    public String toString()
    {
        return "Schema " + name;
    }

    void resetStatistics() {
        this.hasStatistics = false;
    }

    private static AltibaseO2ATableColumn getTableColumn(JDBCSession session, AltibaseO2ATableBase parent, ResultSet dbResult,String columnName) throws DBException
    {

        AltibaseO2ATableColumn tableColumn = columnName == null ? null : parent.getAttribute(session.getProgressMonitor(), columnName);
        if (tableColumn == null) {
            log.debug("Column '" + columnName + "' not found in table '" + parent.getName() + "'");
        }
        return tableColumn;
    }

    ///////////////////////////////////
    // Statistics

    @Override
    public boolean isStatisticsCollected() {
        return hasStatistics;
    }

    @Override
    public void collectObjectStatistics(DBRProgressMonitor monitor, boolean totalSizeOnly, boolean forceRefresh) throws DBException {
        if (hasStatistics && !forceRefresh) {
            return;
        }
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load table status")) {
            boolean hasDBA = getDataSource().isViewAvailable(monitor, AltibaseO2AConstants.SCHEMA_SYS, "DBA_SEGMENTS");
            try (JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT SEGMENT_NAME,SUM(bytes) TABLE_SIZE\n" +
                    "FROM " + AltibaseO2AUtils.getSysSchemaPrefix(getDataSource()) + (hasDBA ? "DBA_SEGMENTS" : "USER_SEGMENTS") + " s\n" +
                    "WHERE S.SEGMENT_TYPE='TABLE'"  + (hasDBA ? " AND s.OWNER = ?" : "") + "\n" +
                    "GROUP BY SEGMENT_NAME"))
            {
                if (hasDBA) {
                    dbStat.setString(1, getName());
                }
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        String tableName = dbResult.getString(1);
                        long bytes = dbResult.getLong(2);
                        AltibaseO2ATable table = getTable(monitor, tableName);
                        if (table != null) {
                            table.fetchTableSize(dbResult);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new DBCException("Error reading table statistics", e);
        } finally {
            for (AltibaseO2ATableBase table : tableCache.getCachedObjects()) {
                if (table instanceof AltibaseO2ATable && !((AltibaseO2ATable) table).hasStatistics()) {
                    ((AltibaseO2ATable) table).setTableSize(0L);
                }
            }
            hasStatistics = true;
        }
    }

    public class TableCache extends JDBCStructLookupCache<AltibaseO2ASchema, AltibaseO2ATableBase, AltibaseO2ATableColumn> {

        TableCache()
        {
            super("OBJECT_NAME");
            setListOrderComparator(DBUtils.nameComparator());
        }

        @NotNull
        @Override
        public JDBCStatement prepareLookupStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @Nullable AltibaseO2ATableBase object, @Nullable String objectName) throws SQLException {
            String tableOper = "=";

            boolean hasAllAllTables = owner.getDataSource().isViewAvailable(session.getProgressMonitor(), null, "ALL_ALL_TABLES");
            boolean useAlternativeQuery = CommonUtils.toBoolean(getDataSource().getContainer().getConnectionConfiguration().getProviderProperty(AltibaseO2AConstants.PROP_METADATA_USE_ALTERNATIVE_TABLE_QUERY));
            String tablesSource = hasAllAllTables ? "ALL_TABLES" : "TABLES";
            String tableTypeColumns = hasAllAllTables ? "t.TABLE_TYPE_OWNER,t.TABLE_TYPE" : "NULL as TABLE_TYPE_OWNER, NULL as TABLE_TYPE";

            JDBCPreparedStatement dbStat;
            if (!useAlternativeQuery) {
                dbStat = session.prepareStatement("SELECT FROM AND O.OWNER=? AND O.OBJECT_TYPE IN ('TABLE', 'VIEW', 'MATERIALIZED VIEW')" +
                        (object == null && objectName == null ? "" : " AND O.OBJECT_NAME" + tableOper + "?") +
                        (object instanceof AltibaseO2ATable ? " AND O.OBJECT_TYPE='TABLE'" : "") +
                        (object instanceof AltibaseO2AView ? " AND O.OBJECT_TYPE='VIEW'" : "") +
                        (object instanceof AltibaseO2AMaterializedView ? " AND O.OBJECT_TYPE='MATERIALIZED VIEW'" : ""));
                dbStat.setString(1, owner.getName());
                if (object != null || objectName != null)
                    dbStat.setString(2, object != null ? object.getName() : objectName);
                return dbStat;
            } else {
                return getAlternativeTableStatement(session, owner, object, objectName, tablesSource, tableTypeColumns);
            }
        }

        @Override
        protected AltibaseO2ATableBase fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @NotNull JDBCResultSet dbResult)
            throws SQLException, DBException
        {
            final String tableType = JDBCUtils.safeGetString(dbResult, "OBJECT_TYPE");
            if ("TABLE".equals(tableType)) {
                return new AltibaseO2ATable(session.getProgressMonitor(), owner, dbResult);
            } else if ("MATERIALIZED VIEW".equals(tableType)) {
                return new AltibaseO2AMaterializedView(owner, dbResult);
            } else {
                return new AltibaseO2AView(owner, dbResult);
            }
        }

        @Override
        protected JDBCStatement prepareChildrenStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @Nullable AltibaseO2ATableBase forTable)
            throws SQLException
        {
            String colsView;
            if (!owner.getDataSource().isViewAvailable(session.getProgressMonitor(), AltibaseO2AConstants.SCHEMA_SYS, "ALL_TAB_COLS")) {
                colsView = "TAB_COLUMNS";
            } else {
                colsView = "TAB_COLS";
            }
            StringBuilder sql = new StringBuilder(500);
            sql
                .append("SELECT ");
            if (forTable != null) {
                sql.append(" AND c.TABLE_NAME=?");
            }
            JDBCPreparedStatement dbStat = session.prepareStatement(sql.toString());
            dbStat.setString(1, owner.getName());
            if (forTable != null) {
                dbStat.setString(2, forTable.getName());
            }
            return dbStat;
        }

        @Override
        protected AltibaseO2ATableColumn fetchChild(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @NotNull AltibaseO2ATableBase table, @NotNull JDBCResultSet dbResult)
            throws SQLException, DBException
        {
            return new AltibaseO2ATableColumn(session.getProgressMonitor(), table, dbResult);
        }

        @Override
        protected void cacheChildren(AltibaseO2ATableBase parent, List<AltibaseO2ATableColumn> altibaseTableColumns) {
            altibaseTableColumns.sort(DBUtils.orderComparator());
            super.cacheChildren(parent, altibaseTableColumns);
        }

        @NotNull
        private JDBCStatement getAlternativeTableStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @Nullable AltibaseO2ATableBase object, @Nullable String objectName, String tablesSource, String tableTypeColumns) throws SQLException {
            boolean hasName = object == null && objectName != null;
            JDBCPreparedStatement dbStat;
            StringBuilder sql = new StringBuilder();
            String tableQuery = "SELECT t.OWNER, t.TABLE_NAME AS OBJECT_NAME, 'TABLE' AS OBJECT_TYPE, 'VALID' AS STATUS," + tableTypeColumns + ", t.TABLESPACE_NAME,\n" +
                    "t.PARTITIONED, t.IOT_TYPE, t.IOT_NAME, t.TEMPORARY, t.SECONDARY, t.NESTED, t.NUM_ROWS\n" +
                    "FROM " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), owner.getDataSource(), tablesSource) + " t\n" +
                    "WHERE t.OWNER =?\n" +
                    "AND NESTED = 'NO'\n";
            String viewQuery = "SELECT o.OWNER, o.OBJECT_NAME, 'VIEW' AS OBJECT_TYPE, o.STATUS, NULL, NULL, NULL, 'NO', NULL, NULL, o.TEMPORARY, o.SECONDARY, 'NO', 0\n" +
                    "FROM " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "OBJECTS") + " o\n" +
                    "WHERE o.OWNER =?\n" +
                    "AND o.OBJECT_TYPE = 'VIEW'\n";
            String mviewQuery = "SELECT o.OWNER, o.OBJECT_NAME, 'MATERIALIZED VIEW' AS OBJECT_TYPE, o.STATUS, NULL, NULL, NULL, 'NO', NULL, NULL, o.TEMPORARY, o.SECONDARY, 'NO', 0\n" +
                    "FROM " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "OBJECTS") + " o\n" +
                    "WHERE o.OWNER =?\n" +
                    "AND o.OBJECT_TYPE = 'MATERIALIZED VIEW'";
            String unionAll = "UNION ALL ";
            if (hasName) {
                sql.append("SELECT * FROM (");
            }
            if (object == null) {
                sql.append(tableQuery).append(unionAll).append(viewQuery).append(unionAll).append(mviewQuery);
            } else if (object instanceof AltibaseO2AMaterializedView) {
                sql.append(mviewQuery);
            } else if (object instanceof AltibaseO2AView) {
                sql.append(viewQuery);
            } else {
                sql.append(tableQuery);
            }
            if (hasName) {
                sql.append(") WHERE OBJECT_NAME").append("=?");
            } else if (object != null) {
                if (object instanceof AltibaseO2ATable) {
                    sql.append(" AND t.TABLE_NAME=?");
                } else {
                    sql.append(" AND o.OBJECT_NAME=?");
                }
            }
            dbStat = session.prepareStatement(sql.toString());
            String ownerName = owner.getName();
            dbStat.setString(1, ownerName);
            if (object == null) {
                dbStat.setString(2, ownerName);
                dbStat.setString(3, ownerName);
                if (objectName != null) {
                    dbStat.setString(4, objectName);
                }
            } else {
                dbStat.setString(2, object.getName());
            }
            return dbStat;
        }
    }

    /**
     * Constraint cache implementation
     */
    class ConstraintCache extends JDBCCompositeCache<AltibaseO2ASchema, AltibaseO2ATableBase, AltibaseO2ATableConstraint, AltibaseO2ATableConstraintColumn> {
        ConstraintCache()
        {
            super(tableCache, AltibaseO2ATableBase.class, "TABLE_NAME", "CONSTRAINT_NAME");
        }

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(JDBCSession session, AltibaseO2ASchema owner, AltibaseO2ATableBase forTable)
            throws SQLException
        {
            
            boolean useSimpleConnection = CommonUtils.toBoolean(session.getDataSource().getContainer().getConnectionConfiguration().getProviderProperty(AltibaseO2AConstants.PROP_METADATA_USE_SIMPLE_CONSTRAINTS));

            StringBuilder sql = new StringBuilder(500);
            JDBCPreparedStatement dbStat;
            
            if (owner.getDataSource().isAtLeastV11() && forTable != null && !useSimpleConnection) {
                
                sql.append("SELECT\r\n" + 
                        "    c.TABLE_NAME,\r\n" + 
                        "    c.CONSTRAINT_NAME,\r\n" + 
                        "    c.CONSTRAINT_TYPE,\r\n" + 
                        "    c.STATUS,\r\n" + 
                        "    c.SEARCH_CONDITION,\r\n" + 
                        "    (\r\n" + 
                        "      SELECT LISTAGG(COLUMN_NAME || ':' || POSITION,',') WITHIN GROUP (ORDER BY \"POSITION\") \r\n" + 
                        "      FROM ALL_CONS_COLUMNS col\r\n" + 
                        "      WHERE col.OWNER =? AND col.TABLE_NAME = ? AND col.CONSTRAINT_NAME = c.CONSTRAINT_NAME GROUP BY CONSTRAINT_NAME \r\n"+
                        "    ) COLUMN_NAMES_NUMS\r\n" + 
                        "FROM\r\n" + 
                        "    " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "CONSTRAINTS") + " c\r\n" + 
                        "WHERE\r\n" + 
                        "    c.CONSTRAINT_TYPE <> 'R'\r\n" + 
                        "    AND c.OWNER = ?\r\n" + 
                        "    AND c.TABLE_NAME = ?");   
                // 1- owner
                // 2-table name
                // 3-owner
                // 4-table name
                
                dbStat = session.prepareStatement(sql.toString());
                dbStat.setString(1, AltibaseO2ASchema.this.getName());
                dbStat.setString(2, forTable.getName());
                dbStat.setString(3, AltibaseO2ASchema.this.getName());
                dbStat.setString(4, forTable.getName());
                
            } else if (owner.getDataSource().isAtLeastV10() && forTable != null && !useSimpleConnection) {
                
                 sql.append("SELECT\r\n" + 
                         "    c.TABLE_NAME,\r\n" + 
                         "    c.CONSTRAINT_NAME,\r\n" + 
                         "    c.CONSTRAINT_TYPE,\r\n" + 
                         "    c.STATUS,\r\n" + 
                         "    c.SEARCH_CONDITION,\r\n" + 
                         "    (\r\n" + 
                         "        SELECT LTRIM(MAX(SYS_CONNECT_BY_PATH(cname || ':' || NVL(p,1),','))    KEEP (DENSE_RANK LAST ORDER BY curr),',') \r\n" + 
                         "        FROM   (SELECT \r\n" + 
                         "                       col.CONSTRAINT_NAME cn,col.POSITION p,col.COLUMN_NAME cname,\r\n" + 
                         "                       ROW_NUMBER() OVER (PARTITION BY col.CONSTRAINT_NAME ORDER BY col.POSITION) AS curr,\r\n" + 
                         "                       ROW_NUMBER() OVER (PARTITION BY col.CONSTRAINT_NAME ORDER BY col.POSITION) -1 AS prev\r\n" + 
                         "                FROM   "+ AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "CONS_COLUMNS") +" col \r\n" + 
                         "                WHERE  col.OWNER =? AND col.TABLE_NAME = ? \r\n" + 
                         "                ) WHERE cn = c.CONSTRAINT_NAME  GROUP BY cn CONNECT BY prev = PRIOR curr AND cn = PRIOR cn START WITH curr = 1      \r\n" + 
                         "        ) COLUMN_NAMES_NUMS\r\n" + 
                         "FROM\r\n" + 
                         "    " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "CONSTRAINTS") + " c\r\n" + 
                         "WHERE\r\n" + 
                         "    c.CONSTRAINT_TYPE <> 'R'\r\n" + 
                         "    AND c.OWNER = ?\r\n" + 
                         "    AND c.TABLE_NAME = ?");   
                 // 1- owner
                 // 2-table name
                 // 3-owner
                 // 4-table name
                 
                 dbStat = session.prepareStatement(sql.toString());
                 dbStat.setString(1, AltibaseO2ASchema.this.getName());
                 dbStat.setString(2, forTable.getName());
                 dbStat.setString(3, AltibaseO2ASchema.this.getName());
                 dbStat.setString(4, forTable.getName());
                
            } else {
                sql
                    .append("SELECT ").append(AltibaseO2AUtils.getSysCatalogHint(owner.getDataSource())).append("\n" +
                        "c.TABLE_NAME, c.CONSTRAINT_NAME,c.CONSTRAINT_TYPE,c.STATUS,c.SEARCH_CONDITION," +
                        "col.COLUMN_NAME,col.POSITION\n" +
                        "FROM " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "CONSTRAINTS") +
                        " c, " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "CONS_COLUMNS") + " col\n" +
                        "WHERE c.CONSTRAINT_TYPE<>'R' AND c.OWNER=? AND c.OWNER=col.OWNER AND c.CONSTRAINT_NAME=col.CONSTRAINT_NAME");
                if (forTable != null) {
                    sql.append(" AND c.TABLE_NAME=?");
                }
                sql.append("\nORDER BY c.CONSTRAINT_NAME,col.POSITION");
    
                dbStat = session.prepareStatement(sql.toString());
                dbStat.setString(1, AltibaseO2ASchema.this.getName());
                if (forTable != null) {
                    dbStat.setString(2, forTable.getName());
                }
            }
            return dbStat;
        }

        @Nullable
        @Override
        protected AltibaseO2ATableConstraint fetchObject(JDBCSession session, AltibaseO2ASchema owner, AltibaseO2ATableBase parent, String indexName, JDBCResultSet dbResult)
            throws SQLException, DBException
        {
            return new AltibaseO2ATableConstraint(parent, dbResult);
        }

        @Nullable
        @Override
        protected AltibaseO2ATableConstraintColumn[] fetchObjectRow(
            JDBCSession session,
            AltibaseO2ATableBase parent, AltibaseO2ATableConstraint object, JDBCResultSet dbResult)
            throws SQLException, DBException
        {
            //resultset has field COLUMN_NAMES_NUMS - special query was used
            if (JDBCUtils.safeGetString(dbResult, "COLUMN_NAMES_NUMS") != null) {
                
                List<SpecialPosition>  positions = parsePositions(JDBCUtils.safeGetString(dbResult, "COLUMN_NAMES_NUMS"));
                
                AltibaseO2ATableConstraintColumn[] result = new AltibaseO2ATableConstraintColumn[positions.size()];
                
                for(int idx = 0;idx < positions.size();idx++) {
                    
                    final AltibaseO2ATableColumn column = getTableColumn(session, parent, dbResult,positions.get(idx).getColumn());
                    
                    if (column == null) {
                        continue;
                    }
                    
                    result[idx] =  new AltibaseO2ATableConstraintColumn(
                            object,
                            column,
                            positions.get(idx).getPos());
                }
                
                return result;
                
                
            } else {
                
                final AltibaseO2ATableColumn tableColumn = getTableColumn(session, parent, dbResult, JDBCUtils.safeGetStringTrimmed(dbResult, "COLUMN_NAME"));
                return tableColumn == null ? null : new AltibaseO2ATableConstraintColumn[] { new AltibaseO2ATableConstraintColumn(
                    object,
                    tableColumn,
                    JDBCUtils.safeGetInt(dbResult, "POSITION")) };
            }
        }

        @Override
        protected void cacheChildren(DBRProgressMonitor monitor, AltibaseO2ATableConstraint constraint, List<AltibaseO2ATableConstraintColumn> rows)
        {
            constraint.setColumns(rows);
        }
    }
    
    class SpecialPosition {
        
        private final String column;
        private final int pos;
        
        public SpecialPosition(String value) {
            
            String data[] = value.split(":");
            
            this.column = data[0];
            
            this.pos = data.length == 1 ? 0 : Integer.valueOf(data[1]);
            
            
        }
        
        public SpecialPosition(String column, int pos) {
            this.column = column;
            this.pos = pos;
        }

        public String getColumn() {
            return column;
        }

        public int getPos() {
            return pos;
        }
         
    }
    
    private List<SpecialPosition> parsePositions(String value) {
        
        if (value == null) {
            return Collections.emptyList();
        }
        
        if (value.length()<3) {
            return Collections.emptyList(); 
        }
        
        List<SpecialPosition> result = new ArrayList<>(1);
        
        String data[] = value.split(",");
        
        for(String s : data) {
            
            result.add(new SpecialPosition(s));
            
        }
        
        return result;
        
    }

    class ForeignKeyCache extends JDBCCompositeCache<AltibaseO2ASchema, AltibaseO2ATable, AltibaseO2ATableForeignKey, AltibaseO2ATableForeignKeyColumn> {
                
        ForeignKeyCache()
        {
            super(tableCache, AltibaseO2ATable.class, "TABLE_NAME", "CONSTRAINT_NAME");
           
        }

        @Override
        protected void loadObjects(DBRProgressMonitor monitor, AltibaseO2ASchema schema, AltibaseO2ATable forParent)
            throws DBException
        {
                 
            // Cache schema constraints if not table specified
            if (forParent == null) {
                constraintCache.getAllObjects(monitor, schema);
            }
            super.loadObjects(monitor, schema, forParent);
        }

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(JDBCSession session, AltibaseO2ASchema owner, AltibaseO2ATable forTable)
            throws SQLException
        {
            boolean useSimpleConnection = CommonUtils.toBoolean(session.getDataSource().getContainer().getConnectionConfiguration().getProviderProperty(AltibaseO2AConstants.PROP_METADATA_USE_SIMPLE_CONSTRAINTS));

            StringBuilder sql = new StringBuilder(500);
            JDBCPreparedStatement dbStat;
            String constraintsView = AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "CONSTRAINTS");
            if (owner.getDataSource().isAtLeastV11() && forTable != null && !useSimpleConnection) {
                 sql.append("SELECT \r\n"
                         + "    c.TABLE_NAME,\r\n"
                         + "    c.CONSTRAINT_NAME,\r\n"
                         + "    c.CONSTRAINT_TYPE,\r\n"
                         + "    c.STATUS,\r\n"
                         + "    c.R_OWNER,\r\n"
                         + "    c.R_CONSTRAINT_NAME,\r\n"
                         + "    rc.TABLE_NAME AS R_TABLE_NAME,\r\n"
                         + "    c.DELETE_RULE,\r\n"
                         + "    (\r\n"
                         + "      SELECT LISTAGG(COLUMN_NAME || ':' || POSITION,',') WITHIN GROUP (ORDER BY \"POSITION\") \r\n"
                         + "      FROM ALL_CONS_COLUMNS col\r\n"
                         + "      WHERE col.OWNER =? AND col.TABLE_NAME = ? AND col.CONSTRAINT_NAME = c.CONSTRAINT_NAME GROUP BY CONSTRAINT_NAME \r\n"
                         + "    ) COLUMN_NAMES_NUMS\r\nFROM " + constraintsView + " c\r\n"
                         + "LEFT JOIN " + constraintsView + " rc\r\n"
                         + "ON rc.OWNER = c.r_OWNER AND rc.CONSTRAINT_NAME = c.R_CONSTRAINT_NAME AND rc.CONSTRAINT_TYPE='P'\r\n"
                         + "WHERE c.OWNER = ? AND c.TABLE_NAME = ? AND c.CONSTRAINT_TYPE = 'R'");
                 // 1- owner
                 // 2-table name
                 // 3-owner
                 // 4-table name

                 dbStat = session.prepareStatement(sql.toString());
                 dbStat.setString(1, AltibaseO2ASchema.this.getName());
                 dbStat.setString(2, forTable.getName());
                 dbStat.setString(3, AltibaseO2ASchema.this.getName());
                 dbStat.setString(4, forTable.getName());


            }else {
                String consColumnsView = AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "CONS_COLUMNS");

                if (owner.getDataSource().isAtLeastV10() && forTable != null && !useSimpleConnection) {
                    sql.append("SELECT c.TABLE_NAME,c.CONSTRAINT_NAME,c.CONSTRAINT_TYPE,\r\n"
                            + "    c.STATUS,c.R_OWNER,c.R_CONSTRAINT_NAME,\r\n"
                            + "    (SELECT rc.TABLE_NAME FROM " + constraintsView
                            + " rc WHERE rc.OWNER = c.r_OWNER AND rc.CONSTRAINT_NAME = c.R_CONSTRAINT_NAME) AS R_TABLE_NAME,\r\n"
                            + "    c.DELETE_RULE,\r\n" + "    (\r\n"
                            + "        SELECT LTRIM(MAX(SYS_CONNECT_BY_PATH(cname || ':' || p,','))    KEEP (DENSE_RANK LAST ORDER BY curr),',') \r\n"
                            + "        FROM   (SELECT \r\n"
                            + "                       col.CONSTRAINT_NAME cn,col.POSITION p,col.COLUMN_NAME cname,\r\n"
                            + "                       ROW_NUMBER() OVER (PARTITION BY col.CONSTRAINT_NAME ORDER BY col.POSITION) AS curr,\r\n"
                            + "                       ROW_NUMBER() OVER (PARTITION BY col.CONSTRAINT_NAME ORDER BY col.POSITION) -1 AS prev\r\n"
                            + "                FROM   " + consColumnsView + " col \r\n"
                            + "                WHERE  col.OWNER =? AND col.TABLE_NAME = ? \r\n"
                            + "                )  WHERE cn = c.CONSTRAINT_NAME GROUP BY cn CONNECT BY prev = PRIOR curr AND cn = PRIOR cn START WITH curr = 1      \r\n"
                            + "        ) COLUMN_NAMES_NUMS\r\n" + "FROM " + constraintsView + " c\r\n"
                            + "WHERE c.OWNER = ? AND c.TABLE_NAME = ? AND c.CONSTRAINT_TYPE = 'R'");
                    // 1- owner
                    // 2-table name
                    // 3-owner
                    // 4-table name

                    dbStat = session.prepareStatement(sql.toString());
                    dbStat.setString(1, AltibaseO2ASchema.this.getName());
                    dbStat.setString(2, forTable.getName());
                    dbStat.setString(3, AltibaseO2ASchema.this.getName());
                    dbStat.setString(4, forTable.getName());

                } else {

                    sql.append("SELECT " + AltibaseO2AUtils.getSysCatalogHint(owner.getDataSource()) + " \r\n" +
                        "c.TABLE_NAME, c.CONSTRAINT_NAME,c.CONSTRAINT_TYPE,c.STATUS,c.R_OWNER,c.R_CONSTRAINT_NAME,rc.TABLE_NAME as R_TABLE_NAME,c.DELETE_RULE, \n" +
                        "col.COLUMN_NAME,col.POSITION\r\n" +
                        "FROM " + constraintsView + " c, " + consColumnsView + " col, " + constraintsView + " rc\n" +
                        "WHERE c.CONSTRAINT_TYPE='R' AND c.OWNER=?\n" +
                        "AND c.OWNER=col.OWNER AND c.CONSTRAINT_NAME=col.CONSTRAINT_NAME\n" +
                        "AND rc.OWNER=c.r_OWNER AND rc.CONSTRAINT_NAME=c.R_CONSTRAINT_NAME");
                    if (forTable != null) {
                        sql.append(" AND c.TABLE_NAME=?");
                    }
                    sql.append("\r\nORDER BY c.CONSTRAINT_NAME,col.POSITION");

                    dbStat = session.prepareStatement(sql.toString());
                    dbStat.setString(1, AltibaseO2ASchema.this.getName());
                    if (forTable != null) {
                        dbStat.setString(2, forTable.getName());
                    }
                }
            }
            return dbStat;
        }

        @Nullable
        @Override
        protected AltibaseO2ATableForeignKey fetchObject(JDBCSession session, AltibaseO2ASchema owner, AltibaseO2ATable parent, String indexName, JDBCResultSet dbResult)
            throws SQLException, DBException
        {
            return new AltibaseO2ATableForeignKey(session.getProgressMonitor(), parent, dbResult);
        }

        @Nullable
        @Override
        protected AltibaseO2ATableForeignKeyColumn[] fetchObjectRow(
            JDBCSession session,
            AltibaseO2ATable parent, AltibaseO2ATableForeignKey object, JDBCResultSet dbResult)
            throws SQLException, DBException
        {
           
            //resultset has field COLUMN_NAMES_NUMS - special query was used
            if (JDBCUtils.safeGetString(dbResult, "COLUMN_NAMES_NUMS") != null) {
                
                List<SpecialPosition>  positions = parsePositions(JDBCUtils.safeGetString(dbResult, "COLUMN_NAMES_NUMS"));
                
                AltibaseO2ATableForeignKeyColumn[] result = new AltibaseO2ATableForeignKeyColumn[positions.size()];
                
                for(int idx = 0;idx < positions.size();idx++) {
                    
                    AltibaseO2ATableColumn column = getTableColumn(session, parent, dbResult,positions.get(idx).getColumn());
                    
                    if (column == null) {
                        continue;
                    }
                    
                    result[idx] =  new AltibaseO2ATableForeignKeyColumn(
                            object,
                            column,
                            positions.get(idx).getPos());
                }
                
                return result;
                
                
            } else {
                
                AltibaseO2ATableColumn column = getTableColumn(session, parent, dbResult, JDBCUtils.safeGetStringTrimmed(dbResult, "COLUMN_NAME"));
                
                if (column == null) {
                    return null;
                }
                
                return  new AltibaseO2ATableForeignKeyColumn[] { new AltibaseO2ATableForeignKeyColumn(
                            object,
                            column,
                            JDBCUtils.safeGetInt(dbResult, "POSITION")) };
            }
        }

        @Override
        @SuppressWarnings("unchecked")
        protected void cacheChildren(DBRProgressMonitor monitor, AltibaseO2ATableForeignKey foreignKey, List<AltibaseO2ATableForeignKeyColumn> rows)
        {
            foreignKey.setColumns((List)rows);
        }
    }


    /**
     * Index cache implementation
     */
    class IndexCache extends JDBCCompositeCache<AltibaseO2ASchema, AltibaseO2ATablePhysical, AltibaseO2ATableIndex, AltibaseO2ATableIndexColumn> {
        IndexCache()
        {
            super(tableCache, AltibaseO2ATablePhysical.class, "TABLE_NAME", "INDEX_NAME");
        }

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(JDBCSession session, AltibaseO2ASchema owner, AltibaseO2ATablePhysical forTable)
            throws SQLException
        {
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT ").append(AltibaseO2AUtils.getSysCatalogHint(owner.getDataSource())).append(" " +
                    "i.OWNER,i.INDEX_NAME,i.INDEX_TYPE,i.TABLE_OWNER,i.TABLE_NAME,i.UNIQUENESS,i.TABLESPACE_NAME,i.STATUS,i.NUM_ROWS,i.SAMPLE_SIZE,\n" +
                    "ic.COLUMN_NAME,ic.COLUMN_POSITION,ic.COLUMN_LENGTH,ic.DESCEND,iex.COLUMN_EXPRESSION\n" +
                    "FROM " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "INDEXES") + " i\n" +
                    "JOIN " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "IND_COLUMNS") + " ic " +
                    "ON i.owner = ic.index_owner AND i.index_name = ic.index_name\n" +
                    "LEFT JOIN " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), getDataSource(), "IND_EXPRESSIONS") + " iex " +
                    "ON iex.index_owner = i.owner AND iex.INDEX_NAME = i.INDEX_NAME AND iex.COLUMN_POSITION = ic.COLUMN_POSITION\n" +
                    "WHERE ");
            if (forTable == null) {
                sql.append("i.OWNER=?");
            } else {
                sql.append("i.TABLE_OWNER=? AND i.TABLE_NAME=?");
            }
            sql.append("\nORDER BY i.INDEX_NAME,ic.COLUMN_POSITION");

            JDBCPreparedStatement dbStat = session.prepareStatement(sql.toString());
            if (forTable == null) {
                dbStat.setString(1, AltibaseO2ASchema.this.getName());
            } else {
                dbStat.setString(1, AltibaseO2ASchema.this.getName());
                dbStat.setString(2, forTable.getName());
            }
            return dbStat;
        }

        @Nullable
        @Override
        protected AltibaseO2ATableIndex fetchObject(JDBCSession session, AltibaseO2ASchema owner, AltibaseO2ATablePhysical parent, String indexName, JDBCResultSet dbResult)
            throws SQLException, DBException
        {
            return new AltibaseO2ATableIndex(owner, parent, indexName, dbResult);
        }

        @Nullable
        @Override
        protected AltibaseO2ATableIndexColumn[] fetchObjectRow(
            JDBCSession session,
            AltibaseO2ATablePhysical parent, AltibaseO2ATableIndex object, JDBCResultSet dbResult)
            throws SQLException, DBException
        {
            String columnName = JDBCUtils.safeGetStringTrimmed(dbResult, "COLUMN_NAME");
            int ordinalPosition = JDBCUtils.safeGetInt(dbResult, "COLUMN_POSITION");
            boolean isAscending = "ASC".equals(JDBCUtils.safeGetStringTrimmed(dbResult, "DESCEND"));
            String columnExpression = JDBCUtils.safeGetStringTrimmed(dbResult, "COLUMN_EXPRESSION");

            AltibaseO2ATableColumn tableColumn = columnName == null ? null : parent.getAttribute(session.getProgressMonitor(), columnName);
            if (tableColumn == null) {
                log.debug("Column '" + columnName + "' not found in table '" + parent.getName() + "' for index '" + object.getName() + "'");
                return null;
            }

            return new AltibaseO2ATableIndexColumn[] { new AltibaseO2ATableIndexColumn(
                object,
                tableColumn,
                ordinalPosition,
                isAscending,
                columnExpression) };
        }

        @Override
        protected void cacheChildren(DBRProgressMonitor monitor, AltibaseO2ATableIndex index, List<AltibaseO2ATableIndexColumn> rows)
        {
            index.setColumns(rows);
        }
    }

    /**
     * DataType cache implementation
     */
    static class DataTypeCache extends JDBCObjectCache<AltibaseO2ASchema, AltibaseO2ADataType> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner) throws SQLException
        {
            JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT " + AltibaseO2AUtils.getSysCatalogHint(owner.getDataSource()) + " * " +
                    "FROM " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), owner.getDataSource(), "TYPES") + " " +
                    "WHERE OWNER=? ORDER BY TYPE_NAME");
            dbStat.setString(1, owner.getName());
            return dbStat;
        }

        @Override
        protected AltibaseO2ADataType fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @NotNull JDBCResultSet resultSet) throws SQLException
        {
            return new AltibaseO2ADataType(owner, resultSet);
        }
    }

    /**
     * Sequence cache implementation
     */
    static class SequenceCache extends JDBCObjectCache<AltibaseO2ASchema, AltibaseO2ASequence> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner) throws SQLException
        {
            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT " + AltibaseO2AUtils.getSysCatalogHint(owner.getDataSource()) + " * FROM " +
                    AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), owner.getDataSource(), "SEQUENCES") +
                    " WHERE SEQUENCE_OWNER=? ORDER BY SEQUENCE_NAME");
            dbStat.setString(1, owner.getName());
            return dbStat;
        }

        @Override
        protected AltibaseO2ASequence fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibaseO2ASequence(owner, resultSet);
        }
    }

    /**
     * Queue cache implementation
     */
    static class QueueCache extends JDBCObjectCache<AltibaseO2ASchema, AltibaseO2AQueue> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner) throws SQLException
        {
            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT " + AltibaseO2AUtils.getSysCatalogHint(owner.getDataSource()) + " * " +
                    "FROM " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), owner.getDataSource(), "QUEUES") + " WHERE OWNER=? ORDER BY NAME");
            dbStat.setString(1, owner.getName());
            return dbStat;
        }

        @Override
        protected AltibaseO2AQueue fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibaseO2AQueue(owner, resultSet);
        }
    }

    /**
     * Procedures cache implementation
     */
    static class ProceduresCache extends JDBCObjectLookupCache<AltibaseO2ASchema, AltibasevO2AProcedureStandalone> {

        @NotNull
        @Override
        public JDBCStatement prepareLookupStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @Nullable AltibasevO2AProcedureStandalone object, @Nullable String objectName) throws SQLException {
            JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT " + AltibaseO2AUtils.getSysCatalogHint(owner.getDataSource()) + " * FROM " +
                    AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), owner.getDataSource(), "OBJECTS") + " " +
                    "WHERE OBJECT_TYPE IN ('PROCEDURE','FUNCTION') " +
                    "AND OWNER=? " +
                    (object == null && objectName == null ? "" : "AND OBJECT_NAME=? ") +
                    "ORDER BY OBJECT_NAME");
            dbStat.setString(1, owner.getName());
            if (object != null || objectName != null) dbStat.setString(2, object != null ? object.getName() : objectName);
            return dbStat;
        }

        @Override
        protected AltibasevO2AProcedureStandalone fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @NotNull JDBCResultSet dbResult)
            throws SQLException, DBException
        {
            return new AltibasevO2AProcedureStandalone(owner, dbResult);
        }

    }

    static class PackageCache extends JDBCObjectCache<AltibaseO2ASchema, AltibaseO2APackage> {

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner)
            throws SQLException
        {
            JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT " + AltibaseO2AUtils.getSysCatalogHint(owner.getDataSource()) + " OBJECT_NAME, STATUS FROM " +
                AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), owner.getDataSource(), "OBJECTS") +
                " WHERE OBJECT_TYPE='PACKAGE' AND OWNER=? " +
                " ORDER BY OBJECT_NAME");
            dbStat.setString(1, owner.getName());
            return dbStat;
        }

        @Override
        protected AltibaseO2APackage fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @NotNull JDBCResultSet dbResult)
            throws SQLException, DBException
        {
            return new AltibaseO2APackage(owner, dbResult);
        }

    }

    /**
     * Sequence cache implementation
     */
    static class SynonymCache extends JDBCObjectLookupCache<AltibaseO2ASchema, AltibaseO2ASynonym> {
        @NotNull
        @Override
        public JDBCStatement prepareLookupStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, AltibaseO2ASynonym object, String objectName) throws SQLException
        {
            String synonymTypeFilter = (session.getDataSource().getContainer().getPreferenceStore().getBoolean(AltibaseO2AConstants.PREF_DBMS_READ_ALL_SYNONYMS) ?
                "" :
                "AND O.OBJECT_TYPE NOT IN ('JAVA CLASS','PACKAGE BODY')\n");

            String synonymName = object != null ? object.getName() : objectName;

            StringBuilder sql = new StringBuilder();
            sql.append("SELECT OWNER, SYNONYM_NAME, MAX(TABLE_OWNER) as TABLE_OWNER, MAX(TABLE_NAME) as TABLE_NAME, MAX(DB_LINK) as DB_LINK, MAX(OBJECT_TYPE) as OBJECT_TYPE FROM (\n")
                .append("SELECT S.*, NULL OBJECT_TYPE FROM ")
                .append(AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), owner.getDataSource(), "SYNONYMS"))
                .append(" S WHERE S.OWNER = ?");
            if (synonymName != null) sql.append(" AND S.SYNONYM_NAME = ?");
            sql
                .append("\nUNION ALL\n")
                .append("SELECT S.*,O.OBJECT_TYPE FROM ").append(AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), owner.getDataSource(), "SYNONYMS")).append(" S, ")
                .append(AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), owner.getDataSource(), "OBJECTS")).append(" O\n")
                .append("WHERE S.OWNER = ?\n");
            if (synonymName != null) sql.append(" AND S.SYNONYM_NAME = ? ");
            sql.append(synonymTypeFilter)
                .append("AND O.OWNER=S.TABLE_OWNER AND O.OBJECT_NAME=S.TABLE_NAME AND O.SUBOBJECT_NAME IS NULL\n)\n");
            sql.append("GROUP BY OWNER, SYNONYM_NAME");
            if (synonymName == null) {
                sql.append("\nORDER BY SYNONYM_NAME");
            }

            JDBCPreparedStatement dbStat = session.prepareStatement(sql.toString());
            int paramNum = 1;
            dbStat.setString(paramNum++, owner.getName());
            if (synonymName != null) dbStat.setString(paramNum++, synonymName);
            dbStat.setString(paramNum++, owner.getName());
            if (synonymName != null) dbStat.setString(paramNum++, synonymName);
            return dbStat;
        }

        @Override
        protected AltibaseO2ASynonym fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibaseO2ASynonym(owner, resultSet);
        }

    }

    static class TriggerCache extends JDBCObjectCache<AltibaseO2ASchema, AltibaseO2ASchemaTrigger> {

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema schema) throws SQLException
        {
            JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT *\n" +
                "FROM " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), schema.getDataSource(), "TRIGGERS") + " WHERE OWNER=? AND TRIM(BASE_OBJECT_TYPE) IN ('DATABASE','SCHEMA')\n" +
                "ORDER BY TRIGGER_NAME");
            dbStat.setString(1, schema.getName());
            return dbStat;
        }

        @Override
        protected AltibaseO2ASchemaTrigger fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema altibaseSchema, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibaseO2ASchemaTrigger(altibaseSchema, resultSet);
        }
    }

    class TableTriggerCache extends JDBCCompositeCache<AltibaseO2ASchema, AltibaseO2ATableBase, AltibaseO2ATableTrigger, AltibaseO2ATriggerColumn> {
        protected TableTriggerCache() {
            super(tableCache, AltibaseO2ATableBase.class, "TABLE_NAME", "TRIGGER_NAME");
        }

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(JDBCSession session, AltibaseO2ASchema schema, AltibaseO2ATableBase table) throws SQLException {
            final JDBCPreparedStatement dbStmt = session.prepareStatement(
                "SELECT" + AltibaseO2AUtils.getSysCatalogHint(schema.getDataSource()) + " t.*, c.*, c.COLUMN_NAME AS TRIGGER_COLUMN_NAME" +
                "\nFROM " +
                AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), schema.getDataSource(), "TRIGGERS") + " t, " +
                AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), schema.getDataSource(), "TRIGGER_COLS") + " c" +
                "\nWHERE t.TABLE_OWNER=?" + (table == null ? "" : " AND t.TABLE_NAME=?") +
                " AND t.BASE_OBJECT_TYPE=" + (table instanceof AltibaseO2AView ? "'VIEW'" : "'TABLE'") + " AND t.TABLE_OWNER=c.TABLE_OWNER(+) AND t.TABLE_NAME=c.TABLE_NAME(+)" +
                " AND t.OWNER=c.TRIGGER_OWNER(+) AND t.TRIGGER_NAME=c.TRIGGER_NAME(+)" +
                "\nORDER BY t.TRIGGER_NAME"
            );
            dbStmt.setString(1, schema.getName());
            if (table != null) {
                dbStmt.setString(2, table.getName());
            }
            return dbStmt;
        }

        @Nullable
        @Override
        protected AltibaseO2ATableTrigger fetchObject(JDBCSession session, AltibaseO2ASchema schema, AltibaseO2ATableBase table, String childName, JDBCResultSet resultSet) throws SQLException, DBException {
            return new AltibaseO2ATableTrigger(table, resultSet);
        }

        @Nullable
        @Override
        protected AltibaseO2ATriggerColumn[] fetchObjectRow(JDBCSession session, AltibaseO2ATableBase table, AltibaseO2ATableTrigger trigger, JDBCResultSet resultSet) throws DBException {
            final AltibaseO2ATableBase refTable = AltibaseO2ATableBase.findTable(
                session.getProgressMonitor(),
                table.getDataSource(),
                JDBCUtils.safeGetString(resultSet, "TABLE_OWNER"),
                JDBCUtils.safeGetString(resultSet, "TABLE_NAME")
            );
            if (refTable != null) {
                final String columnName = JDBCUtils.safeGetString(resultSet, "TRIGGER_COLUMN_NAME");
                if (columnName == null) {
                    return null;
                }
                final AltibaseO2ATableColumn tableColumn = refTable.getAttribute(session.getProgressMonitor(), columnName);
                if (tableColumn == null) {
                    log.debug("Column '" + columnName + "' not found in table '" + refTable.getFullyQualifiedName(DBPEvaluationContext.DDL) + "' for trigger '" + trigger.getName() + "'");
                    return null;
                }
                return new AltibaseO2ATriggerColumn[]{
                    new AltibaseO2ATriggerColumn(session.getProgressMonitor(), trigger, tableColumn, resultSet)
                };
            }
            return null;
        }

        @Override
        protected void cacheChildren(DBRProgressMonitor monitor, AltibaseO2ATableTrigger trigger, List<AltibaseO2ATriggerColumn> columns) {
            trigger.setColumns(columns);
        }

        @Override
        protected boolean isEmptyObjectRowsAllowed() {
            return true;
        }
    }

    static class JobCache extends JDBCObjectCache<AltibaseO2ASchema, AltibaseO2AJob> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner) throws SQLException {
            return session.prepareStatement(
                "SELECT * FROM " + AltibaseO2AUtils.getAdminAllViewPrefix(session.getProgressMonitor(), owner.getDataSource(), "JOBS") + " ORDER BY JOB"
            );
        }

        @Override
        protected AltibaseO2AJob fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2ASchema owner, @NotNull JDBCResultSet dbResult) throws SQLException, DBException {
            return new AltibaseO2AJob(owner, dbResult);
        }
    }
}
