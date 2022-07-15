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
import org.jkiss.dbeaver.ext.altibase.model.source.AltibaseStatefulObject;
import org.jkiss.dbeaver.model.*;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.DBObjectNameCaseTransformer;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCStructCache;
import org.jkiss.dbeaver.model.impl.jdbc.struct.JDBCTable;
import org.jkiss.dbeaver.model.impl.jdbc.struct.JDBCTableColumn;
import org.jkiss.dbeaver.model.meta.*;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.DBSObjectState;
import org.jkiss.dbeaver.model.struct.cache.DBSObjectCache;
import org.jkiss.dbeaver.model.struct.rdb.DBSTableForeignKey;
import org.jkiss.dbeaver.model.struct.rdb.DBSTableIndex;
import org.jkiss.utils.CommonUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * AltibaseTable base
 */
public abstract class AltibaseTableBase extends JDBCTable<AltibaseDataSource, AltibaseSchema>
    implements DBPNamedObject2, DBPRefreshableObject, AltibaseStatefulObject, DBPObjectWithLazyDescription
{
    private static final Log log = Log.getLog(AltibaseTableBase.class);

    public static class TableAdditionalInfo {
        volatile boolean loaded = false;

        boolean isLoaded() { return loaded; }
    }

    public static class AdditionalInfoValidator implements IPropertyCacheValidator<AltibaseTableBase> {
        @Override
        public boolean isPropertyCached(AltibaseTableBase object, Object propertyId)
        {
            return object.getAdditionalInfo().isLoaded();
        }
    }

    public static class CommentsValidator implements IPropertyCacheValidator<AltibaseTableBase> {
        @Override
        public boolean isPropertyCached(AltibaseTableBase object, Object propertyId)
        {
            return object.comment != null;
        }
    }

    private final TablePrivCache tablePrivCache = new TablePrivCache();

    public abstract TableAdditionalInfo getAdditionalInfo();

    protected abstract String getTableTypeName();

    protected boolean valid;
    private String comment;

    protected AltibaseTableBase(AltibaseSchema schema, String name, boolean persisted)
    {
        super(schema, name, persisted);
    }

    protected AltibaseTableBase(AltibaseSchema oracleSchema, ResultSet dbResult)
    {
        super(oracleSchema, true);
        setName(JDBCUtils.safeGetString(dbResult, "OBJECT_NAME"));
        this.valid = "VALID".equals(JDBCUtils.safeGetString(dbResult, "STATUS"));
        //this.comment = JDBCUtils.safeGetString(dbResult, "COMMENTS");
    }

    @Override
    public JDBCStructCache<AltibaseSchema, ? extends JDBCTable, ? extends JDBCTableColumn> getCache()
    {
        return getContainer().tableCache;
    }

    @Override
    @NotNull
    public AltibaseSchema getSchema()
    {
        return super.getContainer();
    }

    @NotNull
    @Override
    @Property(viewable = true, editable = true, valueTransformer = DBObjectNameCaseTransformer.class, order = 1)
    public String getName()
    {
        return super.getName();
    }

    @Nullable
    @Override
    public String getDescription()
    {
        return getComment();
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context)
    {
        return DBUtils.getFullQualifiedName(getDataSource(),
            getContainer(),
            this);
    }

    @Property(viewable = true, editable = true, updatable = true, length = PropertyLength.MULTILINE, order = 100)
    @LazyProperty(cacheValidator = CommentsValidator.class)
    public String getComment(DBRProgressMonitor monitor) {
        if (comment == null) {
            comment = "";
            try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load table comments")) {
                comment = queryTableComment(session);
                if (comment == null) {
                    comment = "";
                }
            } catch (Exception e) {
                log.error("Can't fetch table '" + getName() + "' comment", e);
            }
        }
        return comment;
    }

    @Nullable
    @Override
    public String getDescription(DBRProgressMonitor monitor) {
        return getComment(monitor);
    }

    @Association
    public Collection<AltibaseDependencyGroup> getDependencies(DBRProgressMonitor monitor) {
        return AltibaseDependencyGroup.of(this);
    }

    @Association
    public List<? extends AltibaseTableColumn> getCachedAttributes()
    {
        final DBSObjectCache<AltibaseTableBase, AltibaseTableColumn> childrenCache = getContainer().getTableCache().getChildrenCache(this);
        if (childrenCache != null) {
            return childrenCache.getCachedObjects();
        }
        return Collections.emptyList();
    }

    protected String queryTableComment(JDBCSession session) throws SQLException {
        return JDBCUtils.queryString(
            session,
            "SELECT COMMENTS FROM SYSTEM_.SYS_COMMENTS_ WHERE USER_NAME=? AND TABLE_NAME=? AND COLUMN_NAME IS NULL",
            getSchema().getName(),
            getName(),
            getTableTypeName());
    }

    void loadColumnComments(DBRProgressMonitor monitor) {
        try {
            try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load table column comments")) {
                try (JDBCPreparedStatement stat = session.prepareStatement("SELECT COLUMN_NAME,COMMENTS FROM " +
                		"SYSTEM_.SYS_COMMENTS_ WHERE USER_NAME=? AND TABLE_NAME=? AND COLUMN_NAME IS NOT NULL"))
                {
                    stat.setString(1, getSchema().getName());
                    stat.setString(2, getName());
                    try (JDBCResultSet resultSet = stat.executeQuery()) {
                        while (resultSet.next()) {
                            String colName = resultSet.getString(1);
                            String colComment = resultSet.getString(2);
                            AltibaseTableColumn col = getAttribute(monitor, colName);
                            if (col == null) {
                                log.warn("Column '" + colName + "' not found in table '" + getFullyQualifiedName(DBPEvaluationContext.DDL) + "'");
                            } else {
                                col.setComment(CommonUtils.notEmpty(colComment));
                            }
                        }
                    }
                }
            }
            for (AltibaseTableColumn col : CommonUtils.safeCollection(getAttributes(monitor))) {
                col.cacheComment();
            }
        } catch (Exception e) {
            log.warn("Error fetching table '" + getName() + "' column comments", e);
        }
    }

    public String getComment()
    {
        return comment;
    }

    public void setComment(String comment)
    {
        this.comment = comment;
    }

    @Override
    public List<AltibaseTableColumn> getAttributes(@NotNull DBRProgressMonitor monitor)
        throws DBException
    {
        return getContainer().tableCache.getChildren(monitor, getContainer(), this);
    }

    @Override
    public AltibaseTableColumn getAttribute(@NotNull DBRProgressMonitor monitor, @NotNull String attributeName)
        throws DBException
    {
        return getContainer().tableCache.getChild(monitor, getContainer(), this, attributeName);
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException
    {
        getContainer().constraintCache.clearObjectCache(this);
        getContainer().tableTriggerCache.clearObjectCache(this);

        return getContainer().tableCache.refreshObject(monitor, getContainer(), this);
    }

    @Nullable
    @Association
    public List<AltibaseTableTrigger> getTriggers(@NotNull DBRProgressMonitor monitor)
        throws DBException
    {
        return getSchema().tableTriggerCache.getObjects(monitor, getSchema(), this);
    }

    @Override
    public Collection<? extends DBSTableIndex> getIndexes(DBRProgressMonitor monitor) throws DBException
    {
        return null;
    }

    @Nullable
    @Override
    @Association
    public Collection<AltibaseTableConstraint> getConstraints(@NotNull DBRProgressMonitor monitor)
        throws DBException
    {
        return getContainer().constraintCache.getObjects(monitor, getContainer(), this);
    }

    public AltibaseTableConstraint getConstraint(DBRProgressMonitor monitor, String ukName)
        throws DBException
    {
        return getContainer().constraintCache.getObject(monitor, getContainer(), this, ukName);
    }

    public DBSTableForeignKey getForeignKey(DBRProgressMonitor monitor, String ukName) throws DBException
    {
        return DBUtils.findObject(getAssociations(monitor), ukName);
    }

    @Override
    public Collection<AltibaseTableForeignKey> getAssociations(@NotNull DBRProgressMonitor monitor) throws DBException
    {
        return null;
    }

    @Override
    public Collection<AltibaseTableForeignKey> getReferences(@NotNull DBRProgressMonitor monitor) throws DBException
    {
        return null;
    }

    public String getDDL(DBRProgressMonitor monitor, AltibaseDDLFormat ddlFormat, Map<String, Object> options)
        throws DBException
    {
        return AltibaseUtils.getDDL(monitor, getTableTypeName(), this, ddlFormat, options);
    }

    @NotNull
    @Override
    public DBSObjectState getObjectState()
    {
        return valid ? DBSObjectState.NORMAL : DBSObjectState.INVALID;
    }

    public static AltibaseTableBase findTable(DBRProgressMonitor monitor, AltibaseDataSource dataSource, String ownerName, String tableName) throws DBException
    {
        AltibaseSchema refSchema = dataSource.getSchema(monitor, ownerName);
        if (refSchema == null) {
            log.warn("Referenced schema '" + ownerName + "' not found");
            return null;
        } else {
            AltibaseTableBase refTable = refSchema.tableCache.getObject(monitor, refSchema, tableName);
            if (refTable == null) {
                log.warn("Referenced table '" + tableName + "' not found in schema '" + ownerName + "'");
            }
            return refTable;
        }
    }

    @Association
    public Collection<AltibasePrivTable> getTablePrivs(DBRProgressMonitor monitor) throws DBException
    {
        return tablePrivCache.getAllObjects(monitor, this);
    }

    static class TablePrivCache extends JDBCObjectCache<AltibaseTableBase, AltibasePrivTable> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseTableBase tableBase) throws SQLException
        {
            boolean hasDBA = tableBase.getDataSource().isViewAvailable(session.getProgressMonitor(), AltibaseConstants.SCHEMA_SYS, AltibaseConstants.VIEW_DBA_TAB_PRIVS);
            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT p.*\n" +
                    "FROM " + (hasDBA ? "DBA_TAB_PRIVS p" : "ALL_TAB_PRIVS p") + "\n" +
                    "WHERE p."+ (hasDBA ? "OWNER": "TABLE_SCHEMA") +"=? AND p.TABLE_NAME =?");
            dbStat.setString(1, tableBase.getSchema().getName());
            dbStat.setString(2, tableBase.getName());
            return dbStat;
        }

        @Override
        protected AltibasePrivTable fetchObject(@NotNull JDBCSession session, @NotNull AltibaseTableBase tableBase, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibasePrivTable(tableBase, resultSet);
        }
    }

}
