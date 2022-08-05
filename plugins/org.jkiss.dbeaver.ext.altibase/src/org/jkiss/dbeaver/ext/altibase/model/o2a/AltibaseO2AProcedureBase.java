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
public abstract class AltibaseO2AProcedureBase<PARENT extends DBSObjectContainer> extends AltibaseO2AObject<PARENT> implements DBSProcedure
{
    static final Log log = Log.getLog(AltibaseO2AProcedureBase.class);

    private DBSProcedureType procedureType;
    private final ArgumentsCache argumentsCache = new ArgumentsCache();

    public AltibaseO2AProcedureBase(
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

    public abstract AltibaseO2ASchema getSchema();

    public abstract Integer getOverloadNumber();

    @Override
    public Collection<AltibaseO2AProcedureArgument> getParameters(DBRProgressMonitor monitor) throws DBException
    {
        return argumentsCache.getAllObjects(monitor, this);
    }

    @Association
    public Collection<AltibaseO2ADependencyGroup> getDependencies(DBRProgressMonitor monitor) {
        return AltibaseO2ADependencyGroup.of(this);
    }

    static class ArgumentsCache extends JDBCObjectCache<AltibaseO2AProcedureBase, AltibaseO2AProcedureArgument> {

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseO2AProcedureBase procedure) throws SQLException
        {
            JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT * FROM "+ AltibaseO2AUtils.getSysSchemaPrefix(procedure.getDataSource()) + "ALL_ARGUMENTS " +
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
        protected AltibaseO2AProcedureArgument fetchObject(@NotNull JDBCSession session, @NotNull AltibaseO2AProcedureBase procedure, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibaseO2AProcedureArgument(session.getProgressMonitor(), procedure, resultSet);
        }

        @Override
        protected void invalidateObjects(DBRProgressMonitor monitor, AltibaseO2AProcedureBase owner, Iterator<AltibaseO2AProcedureArgument> objectIter)
        {
            IntKeyMap<AltibaseO2AProcedureArgument> argStack = new IntKeyMap<>();
            while (objectIter.hasNext()) {
                AltibaseO2AProcedureArgument argument = objectIter.next();
                final int curDataLevel = argument.getDataLevel();
                argStack.put(curDataLevel, argument);
                if (curDataLevel > 0) {
                    objectIter.remove();
                    AltibaseO2AProcedureArgument parentArgument = argStack.get(curDataLevel - 1);
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
