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
import org.jkiss.dbeaver.ext.altibase.model.source.AltibaseSourceObject;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBPRefreshableObject;
import org.jkiss.dbeaver.model.DBPScriptObjectExt;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.DBSObjectContainer;
import org.jkiss.dbeaver.model.struct.DBSObjectState;
import org.jkiss.dbeaver.model.struct.rdb.DBSPackage;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureContainer;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;
import org.jkiss.utils.CommonUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * GenericProcedure
 */
public class AltibasePackage extends AltibaseSchemaObject
    implements AltibaseSourceObject, DBPScriptObjectExt, DBSObjectContainer, DBSPackage, DBPRefreshableObject, DBSProcedureContainer
{

    private static final Log log = Log.getLog(AltibasePackage.class);

    private final ProceduresCache proceduresCache = new ProceduresCache();
    private boolean valid;
    private String sourceDeclaration;
    private String sourceDefinition;

    public AltibasePackage(
        AltibaseSchema schema,
        ResultSet dbResult)
    {
        super(schema, JDBCUtils.safeGetString(dbResult, "OBJECT_NAME"), true);
        this.valid = "VALID".equals(JDBCUtils.safeGetString(dbResult, "STATUS"));
    }

    public AltibasePackage(AltibaseSchema schema, String name)
    {
        super(schema, name, false);
    }

    @Property(viewable = true, order = 3)
    public boolean isValid()
    {
        return valid;
    }

    @Override
    public AltibaseSourceType getSourceType()
    {
        return AltibaseSourceType.PACKAGE;
    }

    @Override
    @Property(hidden = true, editable = true, updatable = true, order = -1)
    public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBCException
    {
        if (sourceDeclaration == null && monitor != null) {
            sourceDeclaration = AltibaseUtils.getSource(monitor, this, false, true);
        }
        return sourceDeclaration;
    }

    public void setObjectDefinitionText(String sourceDeclaration)
    {
        this.sourceDeclaration = sourceDeclaration;
    }

    @Override
    @Property(hidden = true, editable = true, updatable = true, order = -1)
    public String getExtendedDefinitionText(DBRProgressMonitor monitor) throws DBException
    {
        if (sourceDefinition == null && monitor != null) {
            sourceDefinition = AltibaseUtils.getSource(monitor, this, true, true);
        }
        return sourceDefinition;
    }

    public void setExtendedDefinitionText(String source)
    {
        this.sourceDefinition = source;
    }

    @Association
    public Collection<AltibaseDependencyGroup> getDependencies(DBRProgressMonitor monitor) {
        return AltibaseDependencyGroup.of(this);
    }

    @Association
    public Collection<AltibaseProcedurePackaged> getProceduresOnly(DBRProgressMonitor monitor) throws DBException {
        return getProcedures(monitor)
            .stream()
            .filter(proc -> proc.getProcedureType() == DBSProcedureType.PROCEDURE)
            .collect(Collectors.toList());
    }

    @Association
    public Collection<AltibaseProcedurePackaged> getFunctionsOnly(DBRProgressMonitor monitor) throws DBException {
        return getProcedures(monitor)
            .stream()
            .filter(proc -> proc.getProcedureType() == DBSProcedureType.FUNCTION)
            .collect(Collectors.toList());
    }

    @Association
    public Collection<AltibaseProcedurePackaged> getProcedures(DBRProgressMonitor monitor) throws DBException {
        return proceduresCache.getAllObjects(monitor, this);
    }

    @Override
    public AltibaseProcedurePackaged getProcedure(DBRProgressMonitor monitor, String uniqueName) throws DBException {
        return proceduresCache.getObject(monitor, this, uniqueName);
    }

    @Override
    public Collection<? extends DBSObject> getChildren(@NotNull DBRProgressMonitor monitor) throws DBException
    {
        return proceduresCache.getAllObjects(monitor, this);
    }

    @Override
    public DBSObject getChild(@NotNull DBRProgressMonitor monitor, @NotNull String childName) throws DBException
    {
        return proceduresCache.getObject(monitor, this, childName);
    }

    @NotNull
    @Override
    public Class<? extends DBSObject> getPrimaryChildType(@Nullable DBRProgressMonitor monitor) throws DBException
    {
        return AltibaseProcedurePackaged.class;
    }

    @Override
    public void cacheStructure(@NotNull DBRProgressMonitor monitor, int scope) throws DBException
    {
        proceduresCache.getAllObjects(monitor, this);
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException
    {
        this.proceduresCache.clearCache();
        this.sourceDeclaration = null;
        this.sourceDefinition = null;
        return this;
    }

    @Override
    public void refreshObjectState(@NotNull DBRProgressMonitor monitor) throws DBCException
    {
        this.valid = AltibaseUtils.getObjectStatus(monitor, this, AltibaseObjectType.PACKAGE) &&
        		AltibaseUtils.getObjectStatus(monitor, this, AltibaseObjectType.PACKAGE_BODY);
    }

    @Override
    public DBEPersistAction[] getCompileActions(DBRProgressMonitor monitor)
    {
        List<DBEPersistAction> actions = new ArrayList<>();
        /*if (!CommonUtils.isEmpty(sourceDeclaration)) */{
            actions.add(
                new AltibaseObjectPersistAction(
                    AltibaseObjectType.PACKAGE,
                    "Compile package",
                    "ALTER PACKAGE " + getFullyQualifiedName(DBPEvaluationContext.DDL) + " COMPILE"
                ));
        }
        try {
            if (!CommonUtils.isEmpty(getExtendedDefinitionText(monitor))) {
                actions.add(
                        new AltibaseObjectPersistAction(
                            AltibaseObjectType.PACKAGE_BODY,
                            "Compile package body",
                            "ALTER PACKAGE " + getFullyQualifiedName(DBPEvaluationContext.DDL) + " COMPILE BODY"
                            ));
            }
        } catch (DBException e) {
            log.warn("Unable to retrieve package body, not compiling it", e);
        }
        return actions.toArray(new DBEPersistAction[0]);
    }

    @NotNull
    @Override
    public DBSObjectState getObjectState()
    {
        return valid ? DBSObjectState.NORMAL : DBSObjectState.INVALID;
    }

    static class ProceduresCache extends JDBCObjectCache<AltibasePackage, AltibaseProcedurePackaged> {

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibasePackage owner)
            throws SQLException
        {
            JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT P.*,CASE WHEN A.DATA_TYPE IS NULL THEN 'PROCEDURE' ELSE 'FUNCTION' END as PROCEDURE_TYPE FROM ALL_PROCEDURES P\n" +
                "LEFT OUTER JOIN ALL_ARGUMENTS A ON A.OWNER=P.OWNER AND A.PACKAGE_NAME=P.OBJECT_NAME AND A.OBJECT_NAME=P.PROCEDURE_NAME AND A.ARGUMENT_NAME IS NULL AND A.DATA_LEVEL=0\n" +
                "WHERE P.OWNER=? AND P.OBJECT_NAME=?\n" +
                "ORDER BY P.PROCEDURE_NAME");
            dbStat.setString(1, owner.getSchema().getName());
            dbStat.setString(2, owner.getName());
            return dbStat;
        }

        @Override
        protected AltibaseProcedurePackaged fetchObject(@NotNull JDBCSession session, @NotNull AltibasePackage owner, @NotNull JDBCResultSet dbResult)
            throws SQLException, DBException
        {
            return new AltibaseProcedurePackaged(owner, dbResult);
        }

        @Override
        protected void invalidateObjects(DBRProgressMonitor monitor, AltibasePackage owner, Iterator<AltibaseProcedurePackaged> objectIter)
        {
            Map<String, AltibaseProcedurePackaged> overloads = new HashMap<>();
            while (objectIter.hasNext()) {
                final AltibaseProcedurePackaged proc = objectIter.next();
                if (CommonUtils.isEmpty(proc.getName())) {
                    // Skip procedures with empty names
                    // Altibase 11+ has dummy procedure with subprogram_id=0 and empty name
                    objectIter.remove();
                    continue;
                }
                final AltibaseProcedurePackaged overload = overloads.get(proc.getName());
                if (overload == null) {
                    overloads.put(proc.getName(), proc);
                } else {
                    if (overload.getOverloadNumber() == null) {
                        overload.setOverload(1);
                    }
                    proc.setOverload(overload.getOverloadNumber() + 1);
                    overloads.put(proc.getName(), proc);
                }
            }
        }
    }

}
