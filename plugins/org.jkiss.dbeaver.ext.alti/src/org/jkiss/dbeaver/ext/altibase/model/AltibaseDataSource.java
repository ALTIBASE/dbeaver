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

import java.util.List;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.altibase.model.plan.AltibaseExecutionPlan;
import org.jkiss.dbeaver.ext.generic.GenericConstants;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.plan.DBCPlan;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanStyle;
import org.jkiss.dbeaver.model.exec.plan.DBCQueryPlanner;
import org.jkiss.dbeaver.model.exec.plan.DBCQueryPlannerConfiguration;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.utils.CommonUtils;

public class AltibaseDataSource extends GenericDataSource implements DBCQueryPlanner {

    private static final Log log = Log.getLog(AltibaseDataSource.class);

    public AltibaseDataSource(DBRProgressMonitor monitor, DBPDataSourceContainer container, AltibaseMetaModel metaModel)
        throws DBException
    {
        super(monitor, container, metaModel, new AltibaseSQLDialect());
    }
 
    /* FIXME: parameter doesn't work */
    public boolean splitProceduresAndFunctions() {
    	return true;
    }
    
    @Override
    public void initialize(@NotNull DBRProgressMonitor monitor) throws DBException {
    	  /*
        // Read metadata
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Read Altibase metadata")) {
            // Read metadata
            try (JDBCPreparedStatement dbStat = session.prepareStatement("SELECT * FROM RDB$TYPES")) {
                monitor.subTask("Load Firebird types");
                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        if (monitor.isCanceled()) {
                            break;
                        }
                        String fieldName = JDBCUtils.safeGetString(dbResult, "RDB$FIELD_NAME");
                        if (fieldName == null) {
                            continue;
                        }
                        fieldName = fieldName.trim();
                        int fieldType = JDBCUtils.safeGetInt(dbResult, "RDB$TYPE");
                        String typeName = JDBCUtils.safeGetString(dbResult, "RDB$TYPE_NAME");
                        if (typeName == null) {
                            continue;
                        }
                        typeName = typeName.trim();
                        String fieldDescription = JDBCUtils.safeGetString(dbResult, "RDB$SYSTEM_FLAG");
                        IntKeyMap<MetaFieldInfo> metaFields = this.metaFields.get(fieldName);
                        if (metaFields == null) {
                            metaFields = new IntKeyMap<>();
                            this.metaFields.put(fieldName, metaFields);
                        }
                        metaFields.put(fieldType, new MetaFieldInfo(fieldType, typeName, fieldDescription));
                    }
                }
            }

        } catch (SQLException ex) {
            log.error("Error reading FB metadata", ex);
        }
*/

        // Init
        super.initialize(monitor);
    }



    @NotNull
    @Override
    public AltibaseDataSource getDataSource() {
        return this;
    }

    @Override
    public List<AltibaseTable> getPhysicalTables(DBRProgressMonitor monitor) throws DBException {
        return (List<AltibaseTable>) super.getPhysicalTables(monitor);
    }

    @Override
    public List<AltibaseTable> getTables(DBRProgressMonitor monitor) throws DBException {
        return (List<AltibaseTable>) super.getTables(monitor);
    }

    @Override
    public List<AltibaseProcedure> getProcedures(DBRProgressMonitor monitor) throws DBException {
        return (List<AltibaseProcedure>) super.getProcedures(monitor);
    }

    @Override
    public List<AltibaseTableTrigger> getTableTriggers(DBRProgressMonitor monitor) throws DBException {
        return (List<AltibaseTableTrigger>) super.getTableTriggers(monitor);
    }
    
    @NotNull
    @Override
    public Class<? extends DBSObject> getPrimaryChildType(@Nullable DBRProgressMonitor monitor) throws DBException {
        return AltibaseTable.class;
    }
    
    ///////////////////////////////////////////////
    // Plan
	@NotNull
    @Override
	public DBCPlan planQueryExecution(@NotNull DBCSession session, @NotNull String query, @NotNull DBCQueryPlannerConfiguration configuration) throws DBException {
		AltibaseExecutionPlan plan = new AltibaseExecutionPlan(this, (JDBCSession) session, query);
        plan.explain();
        return plan;
	}

	@NotNull
    @Override
	public DBCPlanStyle getPlanStyle() {
		return DBCPlanStyle.PLAN;
	}
}
