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
import org.jkiss.dbeaver.model.DBPRefreshableObject;
import org.jkiss.dbeaver.model.DBPSaveableObject;
import org.jkiss.dbeaver.model.access.DBAUser;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.sql.SQLException;
import java.util.Collection;

/**
 * AltibaseGrantee
 */
public abstract class AltibaseGrantee extends AltibaseGlobalObject implements DBAUser, DBPSaveableObject, DBPRefreshableObject
{
    private static final Log log = Log.getLog(AltibaseGrantee.class);

    final RolePrivCache rolePrivCache = new RolePrivCache();
    private final SystemPrivCache systemPrivCache = new SystemPrivCache();
    private final ObjectPrivCache objectPrivCache = new ObjectPrivCache();


    public AltibaseGrantee(AltibaseDataSource dataSource) {
        super(dataSource, true);
    }

    @Association
    public Collection<AltibasePrivRole> getRolePrivs(DBRProgressMonitor monitor) throws DBException
    {
        return rolePrivCache.getAllObjects(monitor, this);
    }

    @Association
    public Collection<AltibasePrivSystem> getSystemPrivs(DBRProgressMonitor monitor) throws DBException
    {
        return systemPrivCache.getAllObjects(monitor, this);
    }

    @Association
    public Collection<AltibasePrivObject> getObjectPrivs(DBRProgressMonitor monitor) throws DBException
    {
        return objectPrivCache.getAllObjects(monitor, this);
    }

    @Nullable
    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor) throws DBException {
        rolePrivCache.clearCache();
        systemPrivCache.clearCache();
        objectPrivCache.clearCache();

        return this;
    }

    static class RolePrivCache extends JDBCObjectCache<AltibaseGrantee, AltibasePrivRole> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseGrantee owner) throws SQLException
        {
            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT * FROM DBA_ROLE_PRIVS WHERE GRANTEE=? ORDER BY GRANTED_ROLE");
            dbStat.setString(1, owner.getName());
            return dbStat;
        }

        @Override
        protected AltibasePrivRole fetchObject(@NotNull JDBCSession session, @NotNull AltibaseGrantee owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibasePrivRole(owner, resultSet);
        }
    }

    static class SystemPrivCache extends JDBCObjectCache<AltibaseGrantee, AltibasePrivSystem> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseGrantee owner) throws SQLException
        {
            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT * FROM DBA_SYS_PRIVS WHERE GRANTEE=? ORDER BY PRIVILEGE");
            dbStat.setString(1, owner.getName());
            return dbStat;
        }

        @Override
        protected AltibasePrivSystem fetchObject(@NotNull JDBCSession session, @NotNull AltibaseGrantee owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibasePrivSystem(owner, resultSet);
        }
    }

    static class ObjectPrivCache extends JDBCObjectCache<AltibaseGrantee, AltibasePrivObject> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseGrantee owner) throws SQLException
        {
            boolean hasDBA = owner.getDataSource().isViewAvailable(session.getProgressMonitor(), AltibaseConstants.SCHEMA_SYS, AltibaseConstants.VIEW_DBA_TAB_PRIVS);

            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT p.*,o.OBJECT_TYPE\n" +
                "FROM " + (hasDBA ? "DBA_TAB_PRIVS p, DBA_OBJECTS o" : "ALL_TAB_PRIVS p, ALL_OBJECTS o") + "\n" +
                "WHERE p.GRANTEE=? " +
                "AND o.OWNER=p." + (hasDBA ? "OWNER" : "TABLE_SCHEMA") + " AND o.OBJECT_NAME=p.TABLE_NAME AND o.OBJECT_TYPE<>'PACKAGE BODY'");
            dbStat.setString(1, owner.getName());
            return dbStat;
        }

        @Override
        protected AltibasePrivObject fetchObject(@NotNull JDBCSession session, @NotNull AltibaseGrantee owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException
        {
            return new AltibasePrivObject(owner, resultSet);
        }
    }

}