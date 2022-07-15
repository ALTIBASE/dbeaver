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
public abstract class AltibaseTablePhysical extends AltibaseTableBase implements DBSObjectLazy<AltibaseDataSource>
{
    private static final Log log = Log.getLog(AltibaseTablePhysical.class);

    //private boolean valid;
    private long rowCount;
    private Long realRowCount;
    private Object tablespace;
    private boolean partitioned;
    private PartitionInfo partitionInfo;
    private PartitionCache partitionCache;

    protected AltibaseTablePhysical(AltibaseSchema schema, String name)
    {
        super(schema, name, false);
    }

    protected AltibaseTablePhysical(
        AltibaseSchema schema,
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
    @LazyProperty(cacheValidator = AltibaseTablespace.TablespaceReferenceValidator.class)
    public Object getTablespace(DBRProgressMonitor monitor) throws DBException
    {
        return AltibaseTablespace.resolveTablespaceReference(monitor, this, null);
    }

    public Object getTablespace() {
        return tablespace;
    }

    public void setTablespace(AltibaseTablespace tablespace) {
        this.tablespace = tablespace;
    }

    @Override
    @Association
    public Collection<AltibaseTableIndex> getIndexes(DBRProgressMonitor monitor)
        throws DBException
    {
        // Read indexes using cache
        return this.getContainer().indexCache.getObjects(monitor, getContainer(), this);
    }

    public AltibaseTableIndex getIndex(DBRProgressMonitor monitor, String name)
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
    public Collection<AltibaseTablePartition> getPartitions(DBRProgressMonitor monitor)
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
    public Collection<AltibaseTablePartition> getSubPartitions(DBRProgressMonitor monitor, AltibaseTablePartition partition)
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
        this.valid = AltibaseUtils.getObjectStatus(monitor, this, AltibaseObjectType.TABLE);
    }

    private static class PartitionCache extends JDBCStructCache<AltibaseTablePhysical, AltibaseTablePartition, AltibaseTablePartition> {

        protected PartitionCache()
        {
            super("PARTITION_NAME");
        }

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseTablePhysical table) throws SQLException
        {
            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT * FROM "+ AltibaseUtils.getSysSchemaPrefix(table.getDataSource()) + "ALL_TAB_PARTITIONS " +
                "WHERE TABLE_OWNER=? AND TABLE_NAME=? " +
                "ORDER BY PARTITION_POSITION");
            dbStat.setString(1, table.getContainer().getName());
            dbStat.setString(2, table.getName());
            return dbStat;
        }

        @Override
        protected AltibaseTablePartition fetchObject(@NotNull JDBCSession session, @NotNull AltibaseTablePhysical table, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibaseTablePartition(table, false, resultSet);
        }

        @Override
        protected JDBCStatement prepareChildrenStatement(@NotNull JDBCSession session, @NotNull AltibaseTablePhysical table, @Nullable AltibaseTablePartition forObject) throws SQLException
        {
            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT * FROM "+ AltibaseUtils.getSysSchemaPrefix(table.getDataSource()) + "ALL_TAB_SUBPARTITIONS " +
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
        protected AltibaseTablePartition fetchChild(@NotNull JDBCSession session, @NotNull AltibaseTablePhysical table, @NotNull AltibaseTablePartition parent, @NotNull JDBCResultSet dbResult) throws SQLException, DBException
        {
            return new AltibaseTablePartition(table, true, dbResult);
        }

    }

    public static class PartitionInfo extends AltibasePartitionBase.PartitionInfoBase {

        public PartitionInfo(DBRProgressMonitor monitor, AltibaseDataSource dataSource, ResultSet dbResult)
            throws DBException
        {
            super(monitor, dataSource, dbResult);
        }
    }

    public static class PartitionInfoValidator implements IPropertyCacheValidator<AltibaseTablePhysical> {
        @Override
        public boolean isPropertyCached(AltibaseTablePhysical object, Object propertyId)
        {
            return object.partitioned && object.partitionInfo != null;
        }
    }

    public static class TablespaceListProvider implements IPropertyValueListProvider<AltibaseTablePhysical> {
        @Override
        public boolean allowCustomValue()
        {
            return false;
        }
        @Override
        public Object[] getPossibleValues(AltibaseTablePhysical object)
        {
            final List<AltibaseTablespace> tablespaces = new ArrayList<>();
            try {
                tablespaces.addAll(object.getDataSource().getTablespaces(new VoidProgressMonitor()));
            } catch (DBException e) {
                log.error(e);
            }
            tablespaces.sort(DBUtils.<AltibaseTablespace>nameComparator());
            return tablespaces.toArray(new AltibaseTablespace[tablespaces.size()]);
        }
    }
}
