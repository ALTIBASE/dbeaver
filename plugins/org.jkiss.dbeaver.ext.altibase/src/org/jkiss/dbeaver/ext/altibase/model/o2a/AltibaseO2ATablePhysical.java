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
import org.jkiss.dbeaver.model.DBConstants;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.AbstractExecutionSource;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCStructCache;
import org.jkiss.dbeaver.model.meta.*;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.runtime.VoidProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSDataContainer;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.DBSObjectLazy;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Altibase physical table
 */
public abstract class AltibaseO2ATablePhysical extends AltibaseO2ATableBase implements DBSObjectLazy<AltibaseO2ADataSource>
{
    private static final Log log = Log.getLog(AltibaseO2ATablePhysical.class);

    //private boolean valid;
    private long rowCount;
    private Long realRowCount;
    private Object tablespace;
    private boolean partitioned;
    private PartitionInfo partitionInfo;
    private PartitionCache partitionCache;

    protected AltibaseO2ATablePhysical(AltibaseO2ASchema schema, String name)
    {
        super(schema, name, false);
    }

    protected AltibaseO2ATablePhysical(
        AltibaseO2ASchema schema,
        ResultSet dbResult)
    {
        super(schema, dbResult);
        this.rowCount = JDBCUtils.safeGetLong(dbResult, "NUM_ROWS");
        //this.valid = "VALID".equals(JDBCUtils.safeGetString(dbResult, "STATUS"));
        this.tablespace = JDBCUtils.safeGetString(dbResult, "TABLESPACE_NAME");

        this.partitioned = JDBCUtils.safeGetBoolean(dbResult, "PARTITIONED", "Y");
        this.partitionCache = partitioned ? new PartitionCache() : null;
    }

    @Property(category = DBConstants.CAT_STATISTICS, viewable = true, order = 20)
    public long getRowCount()
    {
        return rowCount;
    }

    @Property(category = DBConstants.CAT_STATISTICS, viewable = false, expensive = true, order = 21)
    public synchronized Long getRealRowCount(DBRProgressMonitor monitor)
    {
        if (realRowCount != null) {
            return realRowCount;
        }
        if (!isPersisted()) {
            // Do not count rows for views
            return null;
        }

        // Query row count
        try (DBCSession session = DBUtils.openMetaSession(monitor, this, "Read row count")) {
            realRowCount = countData(new AbstractExecutionSource(this, session.getExecutionContext(), this), session, null, DBSDataContainer.FLAG_NONE);
        } catch (DBException e) {
            log.debug("Can't fetch row count", e);
        }
        if (realRowCount == null) {
            realRowCount = -1L;
        }

        return realRowCount;
    }

    @Override
    public Object getLazyReference(Object propertyId)
    {
        return tablespace;
    }

    @Property(viewable = true, order = 22, editable = true, updatable = true, listProvider = TablespaceListProvider.class)
    @LazyProperty(cacheValidator = AltibaseO2ATablespace.TablespaceReferenceValidator.class)
    public Object getTablespace(DBRProgressMonitor monitor) throws DBException
    {
        return AltibaseO2ATablespace.resolveTablespaceReference(monitor, this, null);
    }

    public Object getTablespace() {
        return tablespace;
    }

    public void setTablespace(AltibaseO2ATablespace tablespace) {
        this.tablespace = tablespace;
    }

    @Override
    @Association
    public Collection<AltibaseO2ATableIndex> getIndexes(DBRProgressMonitor monitor)
        throws DBException
    {
        // Read indexes using cache
        return this.getContainer().indexCache.getObjects(monitor, getContainer(), this);
    }

    public AltibaseO2ATableIndex getIndex(DBRProgressMonitor monitor, String name)
        throws DBException
    {
        return this.getContainer().indexCache.getObject(monitor, getContainer(), this, name);
    }

    @PropertyGroup
    @LazyProperty(cacheValidator = PartitionInfoValidator.class)
    public PartitionInfo getPartitionInfo(DBRProgressMonitor monitor) throws DBException
    {
        if (partitionInfo == null && partitioned) {
            try (final JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load partitioning info")) {
                try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT * FROM ALL_PART_TABLES WHERE OWNER=? AND TABLE_NAME=?")) {
                    dbStat.setString(1, getContainer().getName());
                    dbStat.setString(2, getName());
                    try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                        if (dbResult.next()) {
                            partitionInfo = new PartitionInfo(monitor, this.getDataSource(), dbResult);
                        }
                    }
                }
            } catch (SQLException e) {
                throw new DBException(e, getDataSource());
            }
        }
        return partitionInfo;
    }

    @Association
    public Collection<AltibaseO2ATablePartition> getPartitions(DBRProgressMonitor monitor)
        throws DBException
    {
        if (partitionCache == null) {
            return null;
        } else {
            this.partitionCache.getAllObjects(monitor, this);
            this.partitionCache.loadChildren(monitor, this, null);
            return this.partitionCache.getAllObjects(monitor, this);
        }
    }

    @Association
    public Collection<AltibaseO2ATablePartition> getSubPartitions(DBRProgressMonitor monitor, AltibaseO2ATablePartition partition)
        throws DBException
    {
        if (partitionCache == null) {
            return null;
        } else {
            this.partitionCache.getAllObjects(monitor, this);
            return this.partitionCache.getChildren(monitor, this, partition);
        }
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException
    {
        this.getContainer().indexCache.clearObjectCache(this);
        return super.refreshObject(monitor);
    }

    @Override
    public void refreshObjectState(@NotNull DBRProgressMonitor monitor) throws DBCException
    {
        this.valid = AltibaseO2AUtils.getObjectStatus(monitor, this, AltibaseO2AObjectType.TABLE);
    }

    private static class PartitionCache extends JDBCStructCache<AltibaseO2ATablePhysical, AltibaseO2ATablePartition, AltibaseO2ATablePartition> {

        protected PartitionCache()
        {
            super("PARTITION_NAME");
        }

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ATablePhysical table) throws SQLException
        {
            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT * FROM "+ AltibaseO2AUtils.getSysSchemaPrefix(table.getDataSource()) + "ALL_TAB_PARTITIONS " +
                "WHERE TABLE_OWNER=? AND TABLE_NAME=? " +
                "ORDER BY PARTITION_POSITION");
            dbStat.setString(1, table.getContainer().getName());
            dbStat.setString(2, table.getName());
            return dbStat;
        }

        @Override
        protected AltibaseO2ATablePartition fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2ATablePhysical table, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibaseO2ATablePartition(table, false, resultSet);
        }

        @Override
        protected JDBCStatement prepareChildrenStatement(@NotNull JDBCSession session, @NotNull AltibaseO2ATablePhysical table, @Nullable AltibaseO2ATablePartition forObject) throws SQLException
        {
            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT * FROM "+ AltibaseO2AUtils.getSysSchemaPrefix(table.getDataSource()) + "ALL_TAB_SUBPARTITIONS " +
                "WHERE TABLE_OWNER=? AND TABLE_NAME=? " +
                (forObject == null ? "" : "AND PARTITION_NAME=? ") +
                "ORDER BY SUBPARTITION_POSITION");
            dbStat.setString(1, table.getContainer().getName());
            dbStat.setString(2, table.getName());
            if (forObject != null) {
                dbStat.setString(3, forObject.getName());
            }
            return dbStat;
        }

        @Override
        protected AltibaseO2ATablePartition fetchChild(@NotNull JDBCSession session, @NotNull AltibaseO2ATablePhysical table, @NotNull AltibaseO2ATablePartition parent, @NotNull JDBCResultSet dbResult) throws SQLException, DBException
        {
            return new AltibaseO2ATablePartition(table, true, dbResult);
        }

    }

    public static class PartitionInfo extends AltibaseO2APartitionBase.PartitionInfoBase {

        public PartitionInfo(DBRProgressMonitor monitor, AltibaseO2ADataSource dataSource, ResultSet dbResult)
            throws DBException
        {
            super(monitor, dataSource, dbResult);
        }
    }

    public static class PartitionInfoValidator implements IPropertyCacheValidator<AltibaseO2ATablePhysical> {
        @Override
        public boolean isPropertyCached(AltibaseO2ATablePhysical object, Object propertyId)
        {
            return object.partitioned && object.partitionInfo != null;
        }
    }

    public static class TablespaceListProvider implements IPropertyValueListProvider<AltibaseO2ATablePhysical> {
        @Override
        public boolean allowCustomValue()
        {
            return false;
        }
        @Override
        public Object[] getPossibleValues(AltibaseO2ATablePhysical object)
        {
            final List<AltibaseO2ATablespace> tablespaces = new ArrayList<>();
            try {
                tablespaces.addAll(object.getDataSource().getTablespaces(new VoidProgressMonitor()));
            } catch (DBException e) {
                log.error(e);
            }
            tablespaces.sort(DBUtils.<AltibaseO2ATablespace>nameComparator());
            return tablespaces.toArray(new AltibaseO2ATablespace[tablespaces.size()]);
        }
    }
}
