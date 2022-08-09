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
package org.jkiss.dbeaver.ext.altibase.model;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.altibase.AltibaseConstants;
import org.jkiss.dbeaver.ext.altibase.model.meta.AltibaseMetaObject;
import org.jkiss.dbeaver.model.DBPIdentifierCase;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCConstants;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCStructureAssistant;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.struct.AbstractObjectReference;
import org.jkiss.dbeaver.model.impl.struct.RelationalObjectType;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.DBSObjectReference;
import org.jkiss.dbeaver.model.struct.DBSObjectType;
import org.jkiss.utils.CommonUtils;

import java.sql.SQLException;
import java.util.List;

/**
 * GenericStructureAssistant
 */
public class AltibaseStructureAssistant extends JDBCStructureAssistant<AltibaseExecutionContext> {
    private final AltibaseDataSource dataSource;

    AltibaseStructureAssistant(AltibaseDataSource dataSource)
    {
        this.dataSource = dataSource;
    }

    @Override
    protected AltibaseDataSource getDataSource()
    {
        return dataSource;
    }

    @Override
    public DBSObjectType[] getSupportedObjectTypes()
    {
        return new DBSObjectType[] {
            RelationalObjectType.TYPE_TABLE,
            RelationalObjectType.TYPE_PROCEDURE
            };
    }

    public DBSObjectType[] getHyperlinkObjectTypes()
    {
        return getSupportedObjectTypes();
    }

    @Override
    public DBSObjectType[] getAutoCompleteObjectTypes()
    {
        return getSupportedObjectTypes();
    }

    @Override
    protected void findObjectsByMask(@NotNull AltibaseExecutionContext executionContext, @NotNull JDBCSession session,
                                     @NotNull DBSObjectType objectType, @NotNull ObjectsSearchParams params,
                                     @NotNull List<DBSObjectReference> references) throws DBException, SQLException {
        DBSObject parentObject = params.getParentObject();
        boolean globalSearch = params.isGlobalSearch();
        String objectNameMask = params.getMask();
        AltibaseSchema schema = parentObject instanceof AltibaseSchema ? (AltibaseSchema)parentObject : (params.isGlobalSearch() ? null : executionContext.getDefaultSchema());
        AltibaseCatalog catalog = parentObject instanceof AltibaseCatalog ? (AltibaseCatalog)parentObject :
                schema == null ? (globalSearch ? null : executionContext.getDefaultCatalog()) : schema.getCatalog();

        final AltibaseDataSource dataSource = getDataSource();
        DBPIdentifierCase convertCase = params.isCaseSensitive() ? dataSource.getSQLDialect().storesQuotedCase() : dataSource.getSQLDialect().storesUnquotedCase();
        objectNameMask = convertCase.transform(objectNameMask);

        if (objectType == RelationalObjectType.TYPE_TABLE) {
            findTablesByMask(session, catalog, schema, objectNameMask, params.getMaxResults(), references);
        } else if (objectType == RelationalObjectType.TYPE_PROCEDURE) {
            findProceduresByMask(session, catalog, schema, objectNameMask, params.getMaxResults(), references);
        }
    }

    private void findTablesByMask(JDBCSession session, AltibaseCatalog catalog, AltibaseSchema schema, String tableNameMask, int maxResults, List<DBSObjectReference> objects)
        throws SQLException, DBException
    {
        final AltibaseMetaObject tableObject = getDataSource().getMetaObject(AltibaseConstants.OBJECT_TABLE);
        final DBRProgressMonitor monitor = session.getProgressMonitor();
        try (JDBCResultSet dbResult = session.getMetaData().getTables(
            catalog == null ? null : catalog.getName(),
            schema == null ? null : schema.getName(),
            tableNameMask,
            null)) {
            while (dbResult.next()) {
                if (monitor.isCanceled()) {
                    break;
                }
                String catalogName = GenericUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_CAT);
                String schemaName = GenericUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_SCHEM);
                String tableName = GenericUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_NAME);
                if (CommonUtils.isEmpty(tableName)) {
                    continue;
                }
                objects.add(new TableReference(
                    findContainer(session.getProgressMonitor(), catalog, schema, catalogName, schemaName),
                    tableName,
                    GenericUtils.safeGetString(tableObject, dbResult, JDBCConstants.REMARKS)));
                if (objects.size() >= maxResults) {
                    break;
                }
            }
        }
    }

    private void findProceduresByMask(JDBCSession session, AltibaseCatalog catalog, AltibaseSchema schema, String procNameMask, int maxResults, List<DBSObjectReference> objects)
        throws SQLException, DBException
    {
        final AltibaseMetaObject procObject = getDataSource().getMetaObject(AltibaseConstants.OBJECT_PROCEDURE);
        DBRProgressMonitor monitor = session.getProgressMonitor();
        try (JDBCResultSet dbResult = session.getMetaData().getProcedures(
            catalog == null ? null : catalog.getName(),
            schema == null ? null : JDBCUtils.escapeWildCards(session, schema.getName()),
            procNameMask)) {
            while (dbResult.next()) {
                if (monitor.isCanceled()) {
                    break;
                }
                String catalogName = GenericUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.PROCEDURE_CAT);
                String schemaName = GenericUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.PROCEDURE_SCHEM);
                String procName = GenericUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.PROCEDURE_NAME);
                String uniqueName = GenericUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.SPECIFIC_NAME);
                if (CommonUtils.isEmpty(procName)) {
                    continue;
                }
                if (CommonUtils.isEmpty(uniqueName)) {
                    uniqueName = procName;
                }
                // Some driver return specific name for regular name
                procName = GenericUtils.normalizeProcedureName(procName);

                objects.add(new ProcedureReference(
                    findContainer(session.getProgressMonitor(), catalog, schema, catalogName, schemaName),
                    catalogName,
                    procName,
                    uniqueName));
                if (objects.size() >= maxResults) {
                    break;
                }
            }
        }
    }

    private AltibaseStructContainer findContainer(DBRProgressMonitor monitor, AltibaseCatalog parentCatalog, AltibaseSchema parentSchema, String catalogName, String schemaName) throws DBException
    {
        AltibaseCatalog tableCatalog = parentCatalog != null ? parentCatalog : CommonUtils.isEmpty(catalogName) ? null : dataSource.getCatalog(catalogName);
        if (tableCatalog == null && CommonUtils.isEmpty(catalogName) && !CommonUtils.isEmpty(dataSource.getCatalogs()) && dataSource.getCatalogs().size() == 1) {
            // there is only one catalog - let's use it (PostgreSQL)
            tableCatalog = dataSource.getCatalogs().iterator().next();
        }
        AltibaseSchema tableSchema = parentSchema != null ?
            parentSchema :
            CommonUtils.isEmpty(schemaName) ? null :
                tableCatalog == null ? dataSource.getSchema(schemaName) : tableCatalog.getSchema(monitor, schemaName);
        return tableSchema != null ? tableSchema : tableCatalog != null ? tableCatalog : dataSource;
    }

    private abstract static class ObjectReference extends AbstractObjectReference<DBSObject> {

        ObjectReference(AltibaseStructContainer container, String name, String description, Class<?> objectClass, DBSObjectType type)
        {
            super(name, container, description, objectClass, type);
        }

        @Override
        public AltibaseStructContainer getContainer()
        {
            return (AltibaseStructContainer)super.getContainer();
        }
    }

    private class TableReference extends ObjectReference {

        private TableReference(AltibaseStructContainer container, String tableName, String description)
        {
            super(container, tableName, description, AltibaseTable.class, RelationalObjectType.TYPE_TABLE);
        }

        @Override
        public DBSObject resolveObject(DBRProgressMonitor monitor) throws DBException
        {
            AltibaseTableBase table = getContainer().getTable(monitor, getName());
            if (table == null) {
                throw new DBException("Can't find table '" + getName() + "' in '" + DBUtils.getFullQualifiedName(dataSource, getContainer()) + "'");
            }
            return table;
        }
    }

    private class ProcedureReference extends ObjectReference {

        private final String catalogName;
        private String uniqueName;
        private ProcedureReference(AltibaseStructContainer container, String catalogName, String procedureName, String uniqueName)
        {
            super(container, procedureName, null, AltibaseProcedure.class, RelationalObjectType.TYPE_PROCEDURE);
            this.catalogName = catalogName;
            this.uniqueName = uniqueName;
        }

        @Override
        public DBSObject resolveObject(DBRProgressMonitor monitor) throws DBException
        {
            AltibaseProcedure procedure = null;
            if (getContainer() instanceof AltibaseSchema) {
                // Try to use catalog name as package name (Oracle)
                if (!CommonUtils.isEmpty(catalogName)) {
                    AltibasePackage procPackage = ((AltibaseSchema)getContainer()).getPackage(monitor, catalogName);
                    if (procPackage != null) {
                        procedure = procPackage.getProcedure(monitor, uniqueName);
                    }
                }
            }
            if (procedure == null) {
                procedure = getContainer().getProcedure(monitor, uniqueName);
            }
            if (procedure == null) {
                throw new DBException("Can't find procedure '" + getName() + "' (" + uniqueName + ")" + "' in '" + DBUtils.getFullQualifiedName(dataSource, getContainer()) + "'");
            }
            return procedure;
        }
    }
}
