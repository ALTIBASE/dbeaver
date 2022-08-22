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

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.altibase.AltibaseMessages;
import org.jkiss.dbeaver.ext.altibase.AltibaseUtils;
import org.jkiss.dbeaver.ext.generic.GenericConstants;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericFunctionResultType;
import org.jkiss.dbeaver.ext.generic.model.GenericObjectContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericPackage;
import org.jkiss.dbeaver.ext.generic.model.GenericProcedure;
import org.jkiss.dbeaver.ext.generic.model.GenericSequence;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericSynonym;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericUtils;
import org.jkiss.dbeaver.ext.generic.model.GenericView;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaModel;
import org.jkiss.dbeaver.ext.generic.model.meta.GenericMetaObject;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBPErrorAssistant;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.DBCFeatureNotSupportedException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCConstants;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.DBStructUtils;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;
import org.jkiss.utils.CommonUtils;

/**
 * FireBirdDataSource
 */
public class AltibaseMetaModel extends GenericMetaModel
{
    //private Pattern ERROR_POSITION_PATTERN = Pattern.compile(" line ([0-9]+), column ([0-9]+)");
    
    //private static final Set<String> INVALID_TABLE_TYPES = new HashSet<>();
    
    private static final Log log = Log.getLog(AltibaseMetaModel.class);
    public static boolean DBMS_METADATA = true;

    private static final Set<String> INVALID_TABLE_TYPES = new HashSet<>();
    
    static {
    	INVALID_TABLE_TYPES.add("VIEW");
        INVALID_TABLE_TYPES.add("MATERIALIZED VIEW");
        INVALID_TABLE_TYPES.add("QUEUE");
        INVALID_TABLE_TYPES.add("SEQUENCE");
        INVALID_TABLE_TYPES.add("SYNONYM");
    }
    
    public AltibaseMetaModel() {
        super();

    }

    @Override
    public GenericDataSource createDataSourceImpl(DBRProgressMonitor monitor, DBPDataSourceContainer container) throws DBException {
        return new AltibaseDataSource(monitor, container, this);
    }

    @Override
    public AltibaseDataTypeCache createDataTypeCache(@NotNull GenericStructContainer container) {
        return new AltibaseDataTypeCache(container);
    }

    public GenericTableBase createTableImpl(@NotNull JDBCSession session, @NotNull GenericStructContainer owner, @NotNull GenericMetaObject tableObject, @NotNull JDBCResultSet dbResult) {
    	String tableName = GenericUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_NAME);
    	String tableType = GenericUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_TYPE);
    	
    	GenericTableBase table = super.createTableImpl(session, owner, tableObject, dbResult);
    	
    	if (table == null) {
    		return null;
    	}
    	
    	switch(tableType) {
	    	case "TABLE":
	    	case "SYSTEM TABLE":
	    		break;
	    	case "VIEW":
	    	case "SYSTEM VIEW":
	    		table = new AltibaseView(owner, tableName, tableType, dbResult);
	    		break;
	    	case "MATERIALIZED VIEW":
	    		table = new AltibaseView(owner, tableName, tableType, dbResult);
	    		break;
	    	case "QUEUE":
	    		table = new AltibaseQueue(owner, tableName, tableType, dbResult);
	    		break;
	    	case "SYNONYM":
	    	case "SEQUENCE":
	    	default:
	    		table = null;
    	}
    	
    	return table;
    }
    
    @Override
    public String getTableDDL(DBRProgressMonitor monitor, GenericTableBase sourceObject, Map<String, Object> options) throws DBException {
    	String ddl = null;
    	
    	if (DBMS_METADATA) {
    		ddl = getDDLFromDbmsMetadata(monitor, sourceObject, sourceObject.getSchemaName(), sourceObject.getTableType());
    	}
    	
    	if (!DBMS_METADATA || AltibaseUtils.isEmpty(ddl)) {
    		ddl = AltibaseMessages.NO_DBMS_METADATA + super.getTableDDL(monitor, sourceObject, options);
    	}
    	
        return ddl;
    }
    
    public String getSynonymDDL(DBRProgressMonitor monitor, AltibaseSynonym sourceObject, Map<String, Object> options) throws DBException {
    	String ddl = null;
    	
    	if (DBMS_METADATA) {
    		ddl = getDDLFromDbmsMetadata(monitor, sourceObject, sourceObject.getSchemaName(), "SYNONYM");
    	}
    	
    	if (!DBMS_METADATA || AltibaseUtils.isEmpty(ddl)) {
    		ddl = AltibaseMessages.NO_DBMS_METADATA + sourceObject.getDdlLocal();
    	}
    	
    	if (!AltibaseUtils.isEmpty(ddl)) {
    		ddl += ";";
    	}
    	
        return ddl;
    }
    
    
    @Override
    public String getViewDDL(DBRProgressMonitor monitor, GenericView sourceObject, Map<String, Object> options) throws DBException {
    	String ddl = null;

    	if (DBMS_METADATA) {
    		ddl = getDDLFromDbmsMetadata(monitor, sourceObject, sourceObject.getSchema().getName(), AltibaseUtils.getDmbsMetaDataObjTypeName(sourceObject.getTableType()));
    	}
    	
    	if (!DBMS_METADATA || AltibaseUtils.isEmpty(ddl)) {
	    	String sql = "SELECT "
					+ " parse "
					+ " FROM "
					+ " SYSTEM_.SYS_VIEW_PARSE_ VP, SYSTEM_.SYS_USERS_ U, SYSTEM_.SYS_TABLES_ T"
					+ " WHERE"
					+ " U.USER_NAME = ?"
					+ " AND T.TABLE_NAME = ?"
					+ " AND T.TABLE_TYPE = 'V'"
					+ " AND VP.USER_ID = U.USER_ID"
					+ " AND VP.VIEW_ID = T.TABLE_ID"
					+ " ORDER BY SEQ_NO ASC";
	    	
	    	ddl = getViewProcDDLFromCatalog(monitor, sourceObject, sourceObject.getSchema().getName(), sql);
    	}
        
        return (ddl.length() < 1)? "-- View definition not available":ddl.toString();
    }

    @Override
    public String getProcedureDDL(DBRProgressMonitor monitor, GenericProcedure sourceObject) throws DBException {
    	String ddl = null;
    	
    	if (DBMS_METADATA) {
    		ddl = getDDLFromDbmsMetadata(monitor, sourceObject, sourceObject.getSchema().getName(), sourceObject.getProcedureType().name());
    	}
    	
    	if (!DBMS_METADATA || AltibaseUtils.isEmpty(ddl)) {
	    	String sql = "SELECT "
	    			+ " parse "
	    			+ " FROM "
	    			+ " SYSTEM_.SYS_PROC_PARSE_ PP, SYSTEM_.SYS_USERS_ U, SYSTEM_.SYS_PROCEDURES_ P"
	    			+ " WHERE"
	    			+ " U.USER_NAME = ?"
	    			+ " AND P.PROC_NAME = ?"
	    			+ " AND PP.USER_ID = U.USER_ID"
	    			+ " AND PP.PROC_OID = P.PROC_OID"
	    			+ " ORDER BY SEQ_NO ASC";
	    	ddl = getViewProcDDLFromCatalog(monitor, sourceObject, sourceObject.getSchema().getName(), sql);
    	}
        
    	if (ddl.length() < 1) {
    		ddl = "-- Source code not available";
    	}
    	else {
    		ddl += ";" + AltibaseUtils.NEW_LINE + "/";
    	}
    	
        return ddl;
    }

    @Override
    public GenericProcedure createProcedureImpl(GenericStructContainer container, String procedureName, String specificName, String remarks, DBSProcedureType procedureType, GenericFunctionResultType functionResultType) {
        return new AltibaseProcedure(container, procedureName, specificName, remarks, procedureType, functionResultType);
    }

    @Override
    public boolean supportsSequences(@NotNull GenericDataSource dataSource) {
        return true;
    }

    @Override
    public JDBCStatement prepareSequencesLoadStatement(@NotNull JDBCSession session, @NotNull GenericStructContainer container) throws SQLException {
        final JDBCPreparedStatement dbStat = session.prepareStatement(
        		"SELECT"
        				+ " TABLE_NAME, CURRENT_SEQ, START_SEQ, INCREMENT_SEQ, CACHE_SIZE, MAX_SEQ, MIN_SEQ, IS_CYCLE"
        			+ " FROM V$SEQ S, SYSTEM_.SYS_TABLES_ T, SYSTEM_.SYS_USERS_ U"
        			+ " WHERE"
        				+ " U.USER_NAME = ?"
        				+ " AND U.USER_ID = T.USER_ID"
        				+ " AND T.TABLE_OID = S.SEQ_OID"
        				+ " AND T.TABLE_TYPE= 'S'"
        			+ " ORDER BY TABLE_NAME ASC");
        dbStat.setString(1, container.getName());
        return dbStat;
    }

    @Override
    public GenericSequence createSequenceImpl(@NotNull JDBCSession session, @NotNull GenericStructContainer container, @NotNull JDBCResultSet dbResult) {
    	return new AltibaseSequence(container, dbResult);
    }

    //////////////////////////////////////////////////////
    // Synonyms

    public boolean supportsSynonyms(@NotNull GenericDataSource dataSource) {
        return true;
    }

    public JDBCStatement prepareSynonymsLoadStatement(@NotNull JDBCSession session, @NotNull GenericStructContainer container) throws SQLException {
        final JDBCPreparedStatement dbStat = session.prepareStatement(
        		"SELECT "
        				+ " SYNONYM_NAME, OBJECT_OWNER_NAME, OBJECT_NAME"
        			+ " FROM "
        				+ " SYSTEM_.SYS_USERS_ U, SYSTEM_.SYS_SYNONYMS_ S "
        			+ " WHERE "
        				+ " U.USER_NAME = ? AND U.USER_ID = S.SYNONYM_OWNER_ID"
        			+ " ORDER BY SYNONYM_NAME");
        dbStat.setString(1, container.getName());
        return dbStat;
    }

    public GenericSynonym createSynonymImpl(@NotNull JDBCSession session, @NotNull GenericStructContainer container, @NotNull JDBCResultSet dbResult) throws DBException {
    	return new AltibaseSynonym(container
    			, JDBCUtils.safeGetString(dbResult, "SYNONYM_NAME")
    			, ""
    			, JDBCUtils.safeGetString(dbResult, "OBJECT_OWNER_NAME")
    			, JDBCUtils.safeGetString(dbResult, "OBJECT_NAME"));
    }
    
    @Override
    public boolean supportsTriggers(@NotNull GenericDataSource dataSource) {
        return true;
    }
    /*
    @Override
    public JDBCStatement prepareTableTriggersLoadStatement(@NotNull JDBCSession session, @NotNull GenericStructContainer container, @Nullable GenericTableBase table) throws SQLException {
        JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT RDB$TRIGGER_NAME AS TRIGGER_NAME, RDB$RELATION_NAME AS OWNER, T.* FROM RDB$TRIGGERS T\n" +
                        "WHERE RDB$RELATION_NAME" + (table == null ? " IS NOT NULL" : "=?"));
        if (table != null) {
            dbStat.setString(1, table.getName());
        }
        return dbStat;
    }

    @Override
    public GenericTrigger createTableTriggerImpl(@NotNull JDBCSession session, @NotNull GenericStructContainer container, @NotNull GenericTableBase parent, String triggerName, @NotNull JDBCResultSet dbResult) throws DBException {
        if (CommonUtils.isEmpty(triggerName)) {
            triggerName = JDBCUtils.safeGetStringTrimmed(dbResult, "RDB$TRIGGER_NAME");
        }
        if (triggerName == null) {
            return null;
        }
        int sequence = JDBCUtils.safeGetInt(dbResult, "RDB$TRIGGER_SEQUENCE");
        int type = JDBCUtils.safeGetInt(dbResult, "RDB$TRIGGER_TYPE");
        String description = JDBCUtils.safeGetStringTrimmed(dbResult, "RDB$DESCRIPTION");
        int systemFlag = JDBCUtils.safeGetInt(dbResult, "RDB$SYSTEM_FLAG");
        boolean isSystem = systemFlag > 0; // System flag value 0 - if user-defined and 1 or more if system

        return new FireBirdTableTrigger(
                parent,
                triggerName,
                description,
                FireBirdTriggerType.getByType(type),
                sequence,
                isSystem);
    }

     */
    
    @Override
    public boolean supportsDatabaseTriggers(@NotNull GenericDataSource dataSource) {
        return true;
    }

    /*
    @Override
    public JDBCStatement prepareContainerTriggersLoadStatement(@NotNull JDBCSession session, @Nullable GenericStructContainer forParent) throws SQLException {
        return session.prepareStatement("SELECT * FROM RDB$TRIGGERS WHERE RDB$RELATION_NAME IS NULL");
    }

    @Override
    public GenericTrigger createContainerTriggerImpl(@NotNull GenericStructContainer container, @NotNull JDBCResultSet dbResult) throws DBException {
        String name = JDBCUtils.safeGetStringTrimmed(dbResult, "RDB$TRIGGER_NAME");
        if (name == null) {
            return null;
        }
        int sequence = JDBCUtils.safeGetInt(dbResult, "RDB$TRIGGER_SEQUENCE");
        int type = JDBCUtils.safeGetInt(dbResult, "RDB$TRIGGER_TYPE");
        String description = JDBCUtils.safeGetStringTrimmed(dbResult, "RDB$DESCRIPTION");
        int systemFlag = JDBCUtils.safeGetInt(dbResult, "RDB$SYSTEM_FLAG");
        boolean isSystem = true;
        if (systemFlag == 0) { // System flag value 0 - if user-defined and 1 or more if system
            isSystem = false;
        }

        return new FireBirdDatabaseTrigger(
                    container,
                    name,
                    description,
                    FireBirdTriggerType.getByType(type),
                    sequence,
                    isSystem);
    }

    @Override
    public List<GenericTrigger> loadTriggers(DBRProgressMonitor monitor, @NotNull GenericStructContainer container, @Nullable GenericTableBase table) throws DBException {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, container, "Read triggers")) {
            try (JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT * FROM RDB$TRIGGERS\n" +
                    "WHERE RDB$RELATION_NAME" + (table == null ? " IS NULL" : "=?"))) {
                if (table != null) {
                    dbStat.setString(1, table.getName());
                }
                List<GenericTrigger> result = new ArrayList<>();

                try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                    while (dbResult.next()) {
                        String name = JDBCUtils.safeGetStringTrimmed(dbResult, "RDB$TRIGGER_NAME");
                        if (name == null) {
                            continue;
                        }
                        int sequence = JDBCUtils.safeGetInt(dbResult, "RDB$TRIGGER_SEQUENCE");
                        int type = JDBCUtils.safeGetInt(dbResult, "RDB$TRIGGER_TYPE");
                        String description = JDBCUtils.safeGetStringTrimmed(dbResult, "RDB$DESCRIPTION");
                        int systemFlag = JDBCUtils.safeGetInt(dbResult, "RDB$SYSTEM_FLAG");
                        boolean isSystem = systemFlag > 0; // System flag value 0 - if user-defined and 1 or more if system
                        FireBirdTableTrigger trigger = new FireBirdTableTrigger(
                            table,
                            name,
                            description,
                            FireBirdTriggerType.getByType(type),
                            sequence,
                            isSystem);
                        result.add(trigger);
                    }
                }
                return result;

            }
        } catch (SQLException e) {
            throw new DBException(e, container.getDataSource());
        }
    }

    @Override
    public String getTriggerDDL(@NotNull DBRProgressMonitor monitor, @NotNull GenericTrigger trigger) throws DBException {
        return AltibaseUtils.getTriggerSource(monitor, (FireBirdTrigger)trigger);
    }

	
    
    @Override
    public DBPErrorAssistant.ErrorPosition getErrorPosition(@NotNull Throwable error) {
        String message = error.getMessage();
        if (!CommonUtils.isEmpty(message)) {
            Matcher matcher = ERROR_POSITION_PATTERN.matcher(message);
            if (matcher.find()) {
                DBPErrorAssistant.ErrorPosition pos = new DBPErrorAssistant.ErrorPosition();
                pos.line = Integer.parseInt(matcher.group(1)) - 1;
                pos.position = Integer.parseInt(matcher.group(2)) - 1;
                return pos;
            }
        }
        return null;
    }
    */

    @Override
    public boolean isTableCommentEditable() {
        return true;
    }

    /*

    @Override
    public GenericTableColumn createTableColumnImpl(@NotNull DBRProgressMonitor monitor, JDBCResultSet dbResult, @NotNull GenericTableBase table, String columnName, String typeName, int valueType, int sourceType, int ordinalPos, long columnSize, long charLength, Integer scale, Integer precision, int radix, boolean notNull, String remarks, String defaultValue, boolean autoIncrement, boolean autoGenerated) throws DBException {
        return new FireBirdTableColumn(monitor, dbResult, table,
            columnName,
            typeName, valueType, sourceType, ordinalPos,
            columnSize,
            charLength, scale, precision, radix, notNull,
            remarks, defaultValue, autoIncrement, autoGenerated
        );
    }
	*/
    
    @Override
    public String getAutoIncrementClause(GenericTableColumn column) {
        return null;
    }

    /*
    @Override
    public JDBCStatement prepareUniqueConstraintsLoadStatement(@NotNull JDBCSession session, @NotNull GenericStructContainer owner, @Nullable GenericTableBase forParent) throws SQLException {
        return session.prepareStatement(
            "select " +
                "RC.RDB$RELATION_NAME TABLE_NAME," +
                "ISGMT.RDB$FIELD_NAME as COLUMN_NAME," +
                "CAST((ISGMT.RDB$FIELD_POSITION + 1) as SMALLINT) as KEY_SEQ," +
                "RC.RDB$CONSTRAINT_NAME as PK_NAME," +
                "RC.RDB$CONSTRAINT_TYPE as CONSTRAINT_TYPE " +
                "FROM " +
                "RDB$RELATION_CONSTRAINTS RC " +
                "INNER JOIN RDB$INDEX_SEGMENTS ISGMT ON RC.RDB$INDEX_NAME = ISGMT.RDB$INDEX_NAME " +
                "where RC.RDB$CONSTRAINT_TYPE IN ('PRIMARY KEY','UNIQUE') " +
                (forParent == null ? "" : "AND RC.RDB$RELATION_NAME = '" + forParent.getName()) + "' " +
                "ORDER BY ISGMT.RDB$FIELD_NAME ");
    }

    @Override
    public DBSEntityConstraintType getUniqueConstraintType(JDBCResultSet dbResult) throws DBException, SQLException {
        String constraintType = JDBCUtils.safeGetString(dbResult, "CONSTRAINT_TYPE");
        return "PRIMARY KEY".equals(constraintType) ? DBSEntityConstraintType.PRIMARY_KEY : DBSEntityConstraintType.UNIQUE_KEY;
    }
     */

    //////////////////////////////////////////////////////
    // Procedure load

    /*
     * Altibase JDBC getProcedures method returns procedures and functions together
     */
    public void loadProcedures(DBRProgressMonitor monitor, @NotNull GenericObjectContainer container)
    		throws DBException
    {
        Map<String, GenericPackage> packageMap = null;

        Map<String, GenericProcedure> funcMap = new LinkedHashMap<>();

        GenericDataSource dataSource = container.getDataSource();
        GenericMetaObject procObject = dataSource.getMetaObject(GenericConstants.OBJECT_PROCEDURE);
        try (JDBCSession session = DBUtils.openMetaSession(monitor, container, "Load procedures")) {
            boolean supportsFunctions = false;
            if (hasFunctionSupport()) {
                try {
                    // Try to read functions (note: this function appeared only in Java 1.6 so it maybe not implemented by many drivers)
                    // Read procedures
                    JDBCResultSet dbResult = session.getMetaData().getFunctions(
                            container.getCatalog() == null ? null : container.getCatalog().getName(),
                            container.getSchema() == null || DBUtils.isVirtualObject(container.getSchema()) ? null : JDBCUtils.escapeWildCards(session, container.getSchema().getName()),
                            dataSource.getAllObjectsPattern());
                    try {
                        supportsFunctions = true;
                        while (dbResult.next()) {
                            if (monitor.isCanceled()) {
                                break;
                            }
                            String functionName = GenericUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.FUNCTION_NAME);
                            if (functionName == null) {
                                //functionName = GenericUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.PROCEDURE_NAME);
                                // Apparently some drivers return the same results for getProcedures and getFunctions -
                                // so let's skip yet another procedure list
                                continue;
                            }
                            String specificName = GenericUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.SPECIFIC_NAME);
                            if (specificName == null && functionName.indexOf(';') != -1) {
                                // [JDBC: SQL Server native driver]
                                specificName = functionName;
                                functionName = functionName.substring(0, functionName.lastIndexOf(';'));
                            }
                            if (container.hasProcedure(functionName)) {
                                // Seems to be a duplicate
                                continue;
                            }
                            int funcTypeNum = GenericUtils.safeGetInt(procObject, dbResult, JDBCConstants.FUNCTION_TYPE);
                            String remarks = GenericUtils.safeGetString(procObject, dbResult, JDBCConstants.REMARKS);
                            GenericFunctionResultType functionResultType;
                            switch (funcTypeNum) {
                                //case DatabaseMetaData.functionResultUnknown: functionResultType = GenericFunctionResultType.UNKNOWN; break;
                                case DatabaseMetaData.functionNoTable:
                                    functionResultType = GenericFunctionResultType.NO_TABLE;
                                    break;
                                case DatabaseMetaData.functionReturnsTable:
                                    functionResultType = GenericFunctionResultType.TABLE;
                                    break;
                                default:
                                    functionResultType = GenericFunctionResultType.UNKNOWN;
                                    break;
                            }

                            final GenericProcedure procedure = createProcedureImpl(
                                    container,
                                    functionName,
                                    specificName,
                                    remarks,
                                    DBSProcedureType.FUNCTION,
                                    functionResultType);
                            container.addProcedure(procedure);

                            funcMap.put(specificName == null ? functionName : specificName, procedure);
                        }
                    } finally {
                        dbResult.close();
                    }
                } catch (Throwable e) {
                    log.debug("Can't read generic functions", e);
                }
            }

            if (hasProcedureSupport()) {
                {
                    // Read procedures
                    JDBCResultSet dbResult = session.getMetaData().getProcedures(
                            container.getCatalog() == null ? null : container.getCatalog().getName(),
                            container.getSchema() == null || DBUtils.isVirtualObject(container.getSchema()) ? null : JDBCUtils.escapeWildCards(session, container.getSchema().getName()),
                            dataSource.getAllObjectsPattern());
                    try {
                        while (dbResult.next()) {
                            if (monitor.isCanceled()) {
                                break;
                            }
                            String procedureCatalog = GenericUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.PROCEDURE_CAT);
                            String procedureName = GenericUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.PROCEDURE_NAME);
                            String specificName = GenericUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.SPECIFIC_NAME);
                            int procTypeNum = GenericUtils.safeGetInt(procObject, dbResult, JDBCConstants.PROCEDURE_TYPE);
                            String remarks = GenericUtils.safeGetString(procObject, dbResult, JDBCConstants.REMARKS);
                            DBSProcedureType procedureType;
                            switch (procTypeNum) {
                                case DatabaseMetaData.procedureNoResult:
                                    procedureType = DBSProcedureType.PROCEDURE;
                                    break;
                                case DatabaseMetaData.procedureReturnsResult:
                                    procedureType = supportsFunctions ? DBSProcedureType.PROCEDURE : DBSProcedureType.FUNCTION;
                                    break;
                                case DatabaseMetaData.procedureResultUnknown:
                                    procedureType = DBSProcedureType.PROCEDURE;
                                    break;
                                default:
                                    procedureType = DBSProcedureType.UNKNOWN;
                                    break;
                            }
                            if (CommonUtils.isEmpty(specificName)) {
                                specificName = procedureName;
                            }
                            GenericProcedure function = funcMap.get(specificName);
                            if (function != null) {
                                continue;
                            }
                            procedureName = GenericUtils.normalizeProcedureName(procedureName);

                            GenericPackage procedurePackage = null;
                            // FIXME: remove as a silly workaround
                            String packageName = getPackageName(dataSource, procedureCatalog, procedureName, specificName);
                            if (packageName != null) {
                                if (!CommonUtils.isEmpty(packageName)) {
                                    if (packageMap == null) {
                                        packageMap = new TreeMap<>();
                                    }
                                    procedurePackage = packageMap.get(packageName);
                                    if (procedurePackage == null) {
                                        procedurePackage = new GenericPackage(container, packageName, true);
                                        packageMap.put(packageName, procedurePackage);
                                        container.addPackage(procedurePackage);
                                    }
                                }
                            }

                            final GenericProcedure procedure = createProcedureImpl(
                                    procedurePackage != null ? procedurePackage : container,
                                    procedureName,
                                    specificName,
                                    remarks,
                                    procedureType,
                                    null);
                            if (procedurePackage != null) {
                                procedurePackage.addProcedure(procedure);
                            } else {
                                container.addProcedure(procedure);
                            }
                        }
                    } finally {
                        dbResult.close();
                    }
                }
            }

        } catch (SQLException e) {
            throw new DBException(e, dataSource);
        }
    }

    private String getViewProcDDLFromCatalog(DBRProgressMonitor monitor, DBSObject sourceObject, String schemaName, String sql) {
    	StringBuilder ddl = new StringBuilder(AltibaseMessages.NO_DBMS_METADATA);
    	String content = null;
    	boolean hasDDL = false;
    	JDBCPreparedStatement jpstmt = null;
    	JDBCResultSet jrs = null;
    	GenericMetaObject metaObject = getMetaObject(GenericConstants.OBJECT_TABLE);

    	try (JDBCSession session = DBUtils.openMetaSession(monitor, sourceObject, "Get DDL from DB")) {
    		jpstmt = session.prepareStatement(sql);
    		jpstmt.setString(1, schemaName);
    		jpstmt.setString(2, sourceObject.getName());

    		jrs = jpstmt.executeQuery();
    		while (jrs.next()) {
    			if (monitor.isCanceled()) {
    				break;
    			}

    			if (hasDDL == true) {
                	ddl.append(AltibaseUtils.NEW_LINE);
                }
                else {
                	hasDDL = true;
                }
                	
                content = GenericUtils.safeGetStringTrimmed(metaObject, jrs, "PARSE");
                if (content != null) {
                	ddl.append(content);
                }
            }
    	} catch (Exception e)
    	{
    		log.warn("Can't read DDL", e);
    	}
    	finally
    	{
    		jrs.close();
    		jpstmt.close();
    	}
    	
    	return ddl.toString();
    }
    
    private String getDDLFromDbmsMetadata(DBRProgressMonitor monitor, DBSObject sourceObject, String schemaName, String objectType) {
    	String ddl = "";
    	CallableStatement cstmt = null;
    	/* Need to use native CallableStatement
    	 * jcstmt = session.prepareCall("exec ? := dbms_metadata.get_ddl(?, ?, ?)");
			java.lang.NullPointerException
				at Altibase.jdbc.driver.AltibaseParameterMetaData.getParameterMode(AltibaseParameterMetaData.java:31)
				at org.jkiss.dbeaver.model.impl.jdbc.exec.JDBCCallableStatementImpl.getOutputParametersFromJDBC(JDBCCallableStatementImpl.java:316)
				at org.jkiss.dbeaver.model.impl.jdbc.exec.JDBCCallableStatementImpl.<init>(JDBCCallableStatementImpl.java:115)
				at org.jkiss.dbeaver.model.impl.jdbc.exec.JDBCFactoryDefault.createCallableStatement(JDBCFactoryDefault.java:48)
    	 */
    	try (JDBCSession session = DBUtils.openMetaSession(monitor, sourceObject, "Get DDL from DBMS_METADATA")) {
    		
    		if (hasDbmsMetadataPacakge(session)) {
        		Connection conn = session.getOriginal();
        		cstmt = conn.prepareCall("exec ? := dbms_metadata.get_ddl(?, ?, ?)");
        		cstmt.registerOutParameter(1, Types.VARCHAR);
        		
        		cstmt.setString(2, objectType);
        		cstmt.setString(3, sourceObject.getName());
        		cstmt.setString(4, schemaName);

        		cstmt.execute();
        		
        		ddl = cstmt.getString(1);
    		}
    	} catch (Exception e) {
    		log.warn("Can't read DDL from DBMS_METADATA", e);
    	}
    	finally {
    		try {
				if (cstmt != null) {
					cstmt.close();
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    	}
    	
    	return ddl;
    }
    
    private boolean hasDbmsMetadataPacakge(JDBCSession session) {
    	boolean hasDbmsMetadataPacakge = false;
    	//JDBCPreparedStatement jpstmt = null;
    	//JDBCResultSet jrs = null;
    	
    	String sql = "SELECT "
    			+ " count(*)"
    			+ " FROM "
    				+ " SYSTEM_.SYS_PACKAGES_ P" 
    			+ " WHERE"
    				+ " PACKAGE_NAME = 'DBMS_METADATA' "
    				+ " AND STATUS = 0"; // valid
    	
    	try (JDBCPreparedStatement jpstmt = session.prepareStatement(sql)){
    		//jpstmt = session.prepareStatement(sql);
    		JDBCResultSet jrs =  jpstmt.executeQuery();
    		if (jrs.next()) {
    			hasDbmsMetadataPacakge = (jrs.getInt(1) == 2);
    		}
    		
    		jrs.close();
    	} catch(Exception e) {
    		log.warn("Can't check DBMS_METADATA", e);
    	}
    	
    	return hasDbmsMetadataPacakge;
    }
}
