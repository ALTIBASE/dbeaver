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
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObjectContainer;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedure;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;
import org.jkiss.utils.IntKeyMap;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Iterator;

/**
 * GenericProcedure
 */
public abstract class AltibaseProcedureBase<PARENT extends DBSObjectContainer> extends AltibaseObject<PARENT> implements DBSProcedure
{
    static final Log log = Log.getLog(AltibaseProcedureBase.class);

    private DBSProcedureType procedureType;
    private final ArgumentsCache argumentsCache = new ArgumentsCache();

    public AltibaseProcedureBase(
        PARENT parent,
        String name,
        long objectId,
        DBSProcedureType procedureType)
    {
        super(parent, name, objectId, true);
        this.procedureType = procedureType;
    }

    @Override
    @Property(viewable = true, editable = true, order = 3)
    public DBSProcedureType getProcedureType()
    {
        return procedureType ;
    }

    public void setProcedureType(DBSProcedureType procedureType) {
        this.procedureType = procedureType;
    }

    @Override
    public DBSObjectContainer getContainer()
    {
        return getParentObject();
    }

    public abstract AltibaseSchema getSchema();

    public abstract Integer getOverloadNumber();

    @Override
    public Collection<AltibaseProcedureArgument> getParameters(DBRProgressMonitor monitor) throws DBException
    {
        return argumentsCache.getAllObjects(monitor, this);
    }

    @Association
    public Collection<AltibaseDependencyGroup> getDependencies(DBRProgressMonitor monitor) {
        return AltibaseDependencyGroup.of(this);
    }

    static class ArgumentsCache extends JDBCObjectCache<AltibaseProcedureBase, AltibaseProcedureArgument> {

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseProcedureBase procedure) throws SQLException
        {
            JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT * FROM "+ AltibaseUtils.getSysSchemaPrefix(procedure.getDataSource()) + "ALL_ARGUMENTS " +
                "WHERE " +
                (procedure.getObjectId() <= 0  ? "OWNER=? AND OBJECT_NAME=? AND PACKAGE_NAME=? " : "OBJECT_ID=? ") +
                (procedure.getOverloadNumber() != null ? "AND OVERLOAD=? " : "AND OVERLOAD IS NULL ") +
                "\nORDER BY SEQUENCE");
            int paramNum = 1;
            if (procedure.getObjectId() <= 0) {
                dbStat.setString(paramNum++, procedure.getSchema().getName());
                dbStat.setString(paramNum++, procedure.getName());
                dbStat.setString(paramNum++, procedure.getContainer().getName());
            } else {
                dbStat.setLong(paramNum++, procedure.getObjectId());
            }
            if (procedure.getOverloadNumber() != null) {
                dbStat.setInt(paramNum, procedure.getOverloadNumber());
            }
            return dbStat;
        }

        @Override
        protected AltibaseProcedureArgument fetchObject(@NotNull JDBCSession session, @NotNull AltibaseProcedureBase procedure, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibaseProcedureArgument(session.getProgressMonitor(), procedure, resultSet);
        }

        @Override
        protected void invalidateObjects(DBRProgressMonitor monitor, AltibaseProcedureBase owner, Iterator<AltibaseProcedureArgument> objectIter)
        {
            IntKeyMap<AltibaseProcedureArgument> argStack = new IntKeyMap<>();
            while (objectIter.hasNext()) {
                AltibaseProcedureArgument argument = objectIter.next();
                final int curDataLevel = argument.getDataLevel();
                argStack.put(curDataLevel, argument);
                if (curDataLevel > 0) {
                    objectIter.remove();
                    AltibaseProcedureArgument parentArgument = argStack.get(curDataLevel - 1);
                    if (parentArgument == null) {
                        log.error("Broken arguments structure for '" + argument.getParentObject().getFullyQualifiedName(DBPEvaluationContext.DDL) + "' - no parent argument for argument " + argument.getSequence());
                    } else {
                        parentArgument.addAttribute(argument);
                    }
                }
            }
        }

    }

}
