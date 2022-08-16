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
package org.jkiss.dbeaver.ext.altibase.model.meta;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.altibase.GenericConstants;
import org.jkiss.dbeaver.ext.altibase.AltibaseMessages;
import org.jkiss.dbeaver.ext.altibase.model.*;
import org.jkiss.dbeaver.ext.altibase.util.StringUtil;
import org.jkiss.dbeaver.model.*;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCFeatureNotSupportedException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCCallableStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.exec.plan.DBCQueryPlanner;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCConstants;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCDataSourceInfo;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCBasicDataTypeCache;
import org.jkiss.dbeaver.model.impl.jdbc.struct.JDBCDataType;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLConstants;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.dbeaver.model.struct.*;
import org.jkiss.dbeaver.model.struct.rdb.DBSForeignKeyDeferability;
import org.jkiss.dbeaver.model.struct.rdb.DBSForeignKeyModifyRule;
import org.jkiss.dbeaver.model.struct.rdb.DBSIndexType;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;
import org.jkiss.utils.CommonUtils;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.*;

/**
 * Altibase meta model
 */
public class AltibaseMetaModel {

    private static final Log log = Log.getLog(AltibaseMetaModel.class);
    private static final String DEFAULT_NULL_SCHEMA_NAME = "DEFAULT";

    // Tables types which are not actually a table
    // This is needed for some strange JDBC drivers which returns not a table objects
    // in DatabaseMetaData.getTables method (PostgreSQL especially)
    private static final Set<String> INVALID_TABLE_TYPES = new HashSet<>();
    
    public static boolean DBMS_METADATA = true;

    static {
    	INVALID_TABLE_TYPES.add("SEQUENCE");
    	INVALID_TABLE_TYPES.add("QUEUE");
    }


    AltibaseMetaModelDescriptor descriptor;

    public AltibaseMetaModel()
    {
    }

    public AltibaseMetaObject getMetaObject(String id) {
        return descriptor == null ? null : descriptor.getObject(id);
    }

    //////////////////////////////////////////////////////
    // Datasource

    public AltibaseDataSource createDataSourceImpl(DBRProgressMonitor monitor, DBPDataSourceContainer container) throws DBException {
        return new AltibaseDataSource(monitor, container, this, new AltibaseSQLDialect());
    }

    //////////////////////////////////////////////////////
    // Misc

    public JDBCBasicDataTypeCache<AltibaseStructContainer, ? extends JDBCDataType> createDataTypeCache(@NotNull AltibaseStructContainer container) {
        return new AltibaseDataTypeCache(container);
    }

    public DBCQueryPlanner getQueryPlanner(@NotNull AltibaseDataSource dataSource) {
        return null;
    }

    public DBPErrorAssistant.ErrorPosition getErrorPosition(@NotNull Throwable error) {
        return null;
    }

    public boolean supportsUpsertStatement() {
        return false;
    }

    /**
     * Returns SQL clause for table column auto-increment.
     * Null if auto-increment is not supported.
     */
    public String getAutoIncrementClause(AltibaseTableColumn column) {
        return null;
    }

    public boolean useCatalogInObjectNames() {
        return true;
    }

    //////////////////////////////////////////////////////
    // Schema load

    // True if schemas can be omitted.
    // App will suppress any error during schema read then
    public boolean isSchemasOptional() {
        return false;
    }

    public boolean isSystemSchema(AltibaseSchema schema) {
        //return (schema.getName().equalsIgnoreCase("SYSTEM_"));
    	return false;
    }

    public List<AltibaseSchema> loadSchemas(JDBCSession session, AltibaseDataSource dataSource, AltibaseCatalog catalog)
        throws DBException
    {
    	//DBMS_METADATA = hasDbmsMetadataPacakge(session);
    	
        if (dataSource.isOmitSchema()) {
            return null;
        }

        try {
            final AltibaseMetaObject schemaObject = getMetaObject(GenericConstants.OBJECT_SCHEMA);

            final List<AltibaseSchema> tmpSchemas = new ArrayList<>();
            JDBCResultSet dbResult = null;
            boolean catalogSchemas = false, schemasFiltered = false;
			
            try {
            	dbResult = session.getMetaData().getSchemas();
            	
                while (dbResult.next()) {
                    if (session.getProgressMonitor().isCanceled()) {
                        break;
                    }
                    
                    String schemaName = AltibaseUtils.safeGetString(schemaObject, dbResult, JDBCConstants.TABLE_SCHEM);

                    boolean nullSchema = false;
                    if (CommonUtils.isEmpty(schemaName)) {
                        if (supportsNullSchemas()) {
                            schemaName = DEFAULT_NULL_SCHEMA_NAME;
                            nullSchema = true;
                        } else {
                            continue;
                        }
                    }
                    
                    /* JDBC error */
                    if (schemaName.equalsIgnoreCase("PUBLIC")) {
                        schemasFiltered = true;
                        continue;
                    }

                    AltibaseSchema schema = createSchemaImpl(dataSource, catalog, schemaName);
                    if (nullSchema) {
                        schema.setVirtual(true);
                    }
                    tmpSchemas.add(schema);
                }
            } finally {
                dbResult.close();
            }
            if (tmpSchemas.isEmpty() && catalogSchemas && !schemasFiltered && dataSource.getCatalogs().size() == 1) {
                // There is just one catalog and empty schema list. Try to read global schemas
                return loadSchemas(session, dataSource, null);
            }
            if (dataSource.isOmitSingleSchema() && catalog == null && tmpSchemas.size() == 1 ) {
                // Only one schema and no catalogs
                // Most likely it is a fake one, let's skip it
                // Anyway using "%" instead is ok
                tmpSchemas.clear();
            }
            return tmpSchemas;
        } catch (UnsupportedOperationException | SQLFeatureNotSupportedException e) {
            // Schemas are not supported
            log.debug("Can't read schema list: " + e.getMessage());
            return null;
        } catch (Throwable ex) {
            if (isSchemasOptional()) {
                // Schemas are not supported - just ignore this error
                log.warn("Can't read schema list", ex);
                return null;
            } else {
                log.error("Can't read schema list", ex);
                throw new DBException(ex, dataSource);
            }
        }
    }

    protected boolean supportsCatalogChange() {
        return false;
    }

    public boolean supportsNullSchemas() {
        return false;
    }

    public AltibaseSchema createSchemaImpl(@NotNull AltibaseDataSource dataSource, @Nullable AltibaseCatalog catalog, @NotNull String schemaName) throws DBException {
        return new AltibaseSchema(dataSource, catalog, schemaName);
    }

    //////////////////////////////////////////////////////
    // Procedure load

    public void loadProcedures(DBRProgressMonitor monitor, @NotNull AltibaseObjectContainer container)
        throws DBException
    {
        Map<String, AltibasePackage> packageMap = null;

        Map<String, AltibaseProcedure> funcMap = new LinkedHashMap<>();

        AltibaseDataSource dataSource = container.getDataSource();
        AltibaseMetaObject procObject = dataSource.getMetaObject(GenericConstants.OBJECT_PROCEDURE);
        try (JDBCSession session = DBUtils.openMetaSession(monitor, container, "Load procedures")) {
            boolean supportsFunctions = false;
            /*
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
*/
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
                            String procedureCatalog = AltibaseUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.PROCEDURE_CAT);
                            String procedureSchema = AltibaseUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.PROCEDURE_SCHEM);
                            String procedureName = AltibaseUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.PROCEDURE_NAME);
                            String specificName = AltibaseUtils.safeGetStringTrimmed(procObject, dbResult, JDBCConstants.SPECIFIC_NAME);
                            int procTypeNum = AltibaseUtils.safeGetInt(procObject, dbResult, JDBCConstants.PROCEDURE_TYPE);
                            String remarks = AltibaseUtils.safeGetString(procObject, dbResult, JDBCConstants.REMARKS);
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
                            AltibaseProcedure function = funcMap.get(specificName);
                            if (function != null) {
                                // Broken driver
                                log.debug("Broken driver [" + session.getDataSource().getContainer().getDriver().getName() + "] - returns the same list for getProcedures and getFunctons");
                                break;
                            }
                            procedureName = AltibaseUtils.normalizeProcedureName(procedureName);

                            AltibasePackage procedurePackage = null;
                            // FIXME: remove as a silly workaround
                            String packageName = getPackageName(dataSource, procedureCatalog, procedureName, specificName);
                            if (packageName != null) {
                                if (!CommonUtils.isEmpty(packageName)) {
                                    if (packageMap == null) {
                                        packageMap = new TreeMap<>();
                                    }
                                    procedurePackage = packageMap.get(packageName);
                                    if (procedurePackage == null) {
                                        procedurePackage = new AltibasePackage(container, packageName, true);
                                        packageMap.put(packageName, procedurePackage);
                                        container.addPackage(procedurePackage);
                                    }
                                }
                            }

                            final AltibaseProcedure procedure = createProcedureImpl(
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

    public AltibaseProcedure createProcedureImpl(
        AltibaseStructContainer container,
        String procedureName,
        String specificName,
        String remarks,
        DBSProcedureType procedureType,
        AltibaseFunctionResultType functionResultType)
    {
        return new AltibaseProcedure(
            container,
            procedureName,
            specificName,
            remarks,
            procedureType,
            functionResultType);
    }

    public String getProcedureDDL(DBRProgressMonitor monitor, AltibaseProcedure sourceObject) throws DBException {
    	String ddl = null;
    	
    	if (DBMS_METADATA) {
    		ddl = getDDLFromDbmsMetadata(monitor, sourceObject, sourceObject.getSchema().getName(), sourceObject.getProcedureType().name());
    	}
    	
    	if (!DBMS_METADATA || StringUtil.isEmpty(ddl)) {
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
        
        return (ddl.length() < 1)? "-- Source code not available":ddl.toString();
    }

    public String getPackageName(AltibaseDataSource dataSource, String catalogName, String procedureName, String specificName) {

        // Caused problems in #6241. Probably we should remove it (for now getPackageName always returns null so it is disabled anyway)
        if (!CommonUtils.isEmpty(catalogName) && CommonUtils.isEmpty(dataSource.getCatalogs())) {
            // Check for packages. Oracle (and may be some other databases) uses catalog name as a storage for package name
            // In fact it is a legacy code from ancient times (before Oracle extension was added).

            // Catalog name specified while there are no catalogs in data source
            //return catalogName;
        }

        return null;
    }



    public boolean supportsOverloadedProcedureNames() {
        return false;
    }

    public boolean showProcedureParamNames() {
        return false;
    }

    //////////////////////////////////////////////////////
    // Catalog load

    // True if catalogs can be omitted.
    // App will suppress any error during catalog read then
    public boolean isCatalogsOptional() {
        return true;
    }

    public AltibaseCatalog createCatalogImpl(@NotNull AltibaseDataSource dataSource, @NotNull String catalogName) {
        return new AltibaseCatalog(dataSource, catalogName);
    }

    //////////////////////////////////////////////////////
    // Tables

    /**
     * Prepares statement which returns results with following columns (the same as in JDBC spec).
     * May also contain any other db-specific columns
     *  <OL>
     *  <LI><B>TABLE_CAT</B> String {@code =>} table catalog (may be <code>null</code>)
     *  <LI><B>TABLE_SCHEM</B> String {@code =>} table schema (may be <code>null</code>)
     *  <LI><B>TABLE_NAME</B> String {@code =>} table name
     *  <LI><B>TABLE_TYPE</B> String {@code =>} table type.  Typical types are "TABLE",
     *                  "VIEW", "SYSTEM TABLE", "GLOBAL TEMPORARY",
     *                  "LOCAL TEMPORARY", "ALIAS", "SYNONYM".
     *  <LI><B>REMARKS</B> String {@code =>} explanatory comment on the table
     *  <LI><B>TYPE_CAT</B> String {@code =>} the types catalog (may be <code>null</code>)
     *  <LI><B>TYPE_SCHEM</B> String {@code =>} the types schema (may be <code>null</code>)
     *  <LI><B>TYPE_NAME</B> String {@code =>} type name (may be <code>null</code>)
     *  <LI><B>SELF_REFERENCING_COL_NAME</B> String {@code =>} name of the designated
     *                  "identifier" column of a typed table (may be <code>null</code>)
     *  <LI><B>REF_GENERATION</B> String {@code =>} specifies how values in
     *                  SELF_REFERENCING_COL_NAME are created. Values are
     *                  "SYSTEM", "USER", "DERIVED". (may be <code>null</code>)
     *  </OL>
     */
    public JDBCStatement prepareTableLoadStatement(@NotNull JDBCSession session, @NotNull AltibaseStructContainer owner, @Nullable AltibaseTableBase object, @Nullable String objectName)
        throws SQLException
    {
        String tableNamePattern;
        if (object == null && objectName == null) {
            final DBSObjectFilter tableFilters = session.getDataSource().getContainer().getObjectFilter(AltibaseTable.class, owner, false);

            if (tableFilters != null && tableFilters.hasSingleMask()) {
                tableNamePattern = tableFilters.getSingleMask();
                if (!CommonUtils.isEmpty(tableNamePattern)) {
                    tableNamePattern = SQLUtils.makeSQLLike(tableNamePattern);
                }
            } else {
                tableNamePattern = owner.getDataSource().getAllObjectsPattern();
            }
        } else {
            tableNamePattern = JDBCUtils.escapeWildCards(session, (object != null ? object.getName() : objectName));
        }

        return session.getMetaData().getTables(
            owner.getCatalog() == null ? null : owner.getCatalog().getName(),
            owner.getSchema() == null || DBUtils.isVirtualObject(owner.getSchema()) ? null : JDBCUtils.escapeWildCards(session, owner.getSchema().getName()),
            tableNamePattern,
            null).getSourceStatement();
    }

    public AltibaseTableBase createTableImpl(@NotNull JDBCSession session, @NotNull AltibaseStructContainer owner, @NotNull AltibaseMetaObject tableObject, @NotNull JDBCResultSet dbResult) {
        String tableName = AltibaseUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_NAME);
        String tableType = AltibaseUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_TYPE);

        String tableSchema = AltibaseUtils.safeGetStringTrimmed(tableObject, dbResult, JDBCConstants.TABLE_SCHEM);
        if (!CommonUtils.isEmpty(tableSchema) && owner.getDataSource().isOmitSchema()) {
            // Ignore tables with schema [Google Spanner]
            log.debug("Ignore table " + tableSchema + "." + tableName + " (schemas are omitted)");
            return null;
        }

        if (CommonUtils.isEmpty(tableName)) {
            log.debug("Empty table name " + (owner == null ? "" : " in container " + owner.getName()));
            return null;
        }

        if (tableType != null && INVALID_TABLE_TYPES.contains(tableType)) {
            // Bad table type. Just skip it
            return null;
        }
        if (DBUtils.isVirtualObject(owner) && !CommonUtils.isEmpty(tableSchema)) {
            // Wrong schema - this may happen with virtual schemas
            return null;
        }
        AltibaseTableBase table = this.createTableImpl(
            owner,
            tableName,
            tableType,
            dbResult);
        if (table == null) {
            return null;
        }

        boolean isSystemTable = table.isSystem();
        if (isSystemTable && !owner.getDataSource().getContainer().getNavigatorSettings().isShowSystemObjects()) {
            return null;
        }
        return table;
    }

    public AltibaseTableBase createTableImpl(
        AltibaseStructContainer container,
        @Nullable String tableName,
        @Nullable String tableType,
        @Nullable JDBCResultSet dbResult)
    {
        if (tableType != null && isView(tableType)) {
            return new AltibaseView(
                container,
                tableName,
                tableType,
                dbResult);
        }

        return new AltibaseTable(
            container,
            tableName,
            tableType,
            dbResult);
    }

    public String getViewDDL(DBRProgressMonitor monitor, AltibaseView sourceObject, Map<String, Object> options) throws DBException {
    	String ddl = null;

    	if (DBMS_METADATA) {
    		ddl = getDDLFromDbmsMetadata(monitor, sourceObject, sourceObject.getSchema().getName(), sourceObject.getTableType());
    	}
    	
    	if (!DBMS_METADATA || StringUtil.isEmpty(ddl)) {
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
    
    public String getTableDDL(DBRProgressMonitor monitor, AltibaseTableBase sourceObject, Map<String, Object> options) throws DBException {
    	String ddl = null;
    	
    	if (DBMS_METADATA) {
    		ddl = getDDLFromDbmsMetadata(monitor, sourceObject, sourceObject.getSchemaName(), sourceObject.getTableType());
    	}
    	
    	if (!DBMS_METADATA || StringUtil.isEmpty(ddl)) {
    		ddl = DBStructUtils.generateTableDDL(monitor, sourceObject, options, false);
    	}
    	
        return ddl;
    }

    public boolean supportsTableDDLSplit(AltibaseTableBase sourceObject) {
        return true;
    }

    // Some database (like Informix) do not support foreign key declaration as nested.
    // DDL for these tables must contain definition of FK outside main brackets (ALTER TABLE ... ADD CONSTRAINT FOREIGN KEY)
    public boolean supportNestedForeignKeys() {
        return true;
    }

    public boolean isSystemTable(AltibaseTableBase table) {
        final String tableType = table.getTableType().toUpperCase(Locale.ENGLISH);
        return tableType.contains("SYSTEM");
    }

    public boolean isView(String tableType) {
        return tableType.toUpperCase(Locale.ENGLISH).contains(GenericConstants.TABLE_TYPE_VIEW);
    }

    //////////////////////////////////////////////////////
    // Table columns

    public JDBCStatement prepareTableColumnLoadStatement(@NotNull JDBCSession session, @NotNull AltibaseStructContainer owner, @Nullable AltibaseTableBase forTable) throws SQLException {
        return session.getMetaData().getColumns(
            owner.getCatalog() == null ? null : owner.getCatalog().getName(),
            owner.getSchema() == null || DBUtils.isVirtualObject(owner.getSchema()) ? null : JDBCUtils.escapeWildCards(session, owner.getSchema().getName()),
            forTable == null ?
                owner.getDataSource().getAllObjectsPattern() :
                JDBCUtils.escapeWildCards(session, forTable.getName()),
            owner.getDataSource().getAllObjectsPattern())
            .getSourceStatement();
    }

    public AltibaseTableColumn createTableColumnImpl(@NotNull DBRProgressMonitor monitor, @Nullable JDBCResultSet dbResult, @NotNull AltibaseTableBase table, String columnName, String typeName, int valueType, int sourceType, int ordinalPos, long columnSize, long charLength, Integer scale, Integer precision, int radix, boolean notNull, String remarks, String defaultValue, boolean autoIncrement, boolean autoGenerated) throws DBException {
        return new AltibaseTableColumn(table,
            columnName,
            typeName, valueType, sourceType, ordinalPos,
            columnSize,
            charLength, scale, precision, radix, notNull,
            remarks, defaultValue, autoIncrement, autoGenerated
        );
    }

    //////////////////////////////////////////////////////
    // Constraints

    public JDBCStatement prepareUniqueConstraintsLoadStatement(@NotNull JDBCSession session, @NotNull AltibaseStructContainer owner, @Nullable AltibaseTableBase forParent)
            throws SQLException, DBException {
        return session.getMetaData().getPrimaryKeys(
            owner.getCatalog() == null ? null : owner.getCatalog().getName(),
            owner.getSchema() == null || DBUtils.isVirtualObject(owner.getSchema()) ? null : owner.getSchema().getName(),
            forParent == null ? owner.getDataSource().getAllObjectsPattern() : forParent.getName())
            .getSourceStatement();
    }

    public DBSEntityConstraintType getUniqueConstraintType(JDBCResultSet dbResult) throws DBException, SQLException {
        return DBSEntityConstraintType.PRIMARY_KEY;
    }

    @NotNull
    public AltibaseTableForeignKey createTableForeignKeyImpl(AltibaseTableBase table, String name, @Nullable String remarks, DBSEntityReferrer referencedKey, DBSForeignKeyModifyRule deleteRule, DBSForeignKeyModifyRule updateRule, DBSForeignKeyDeferability deferability, boolean persisted) {
        return new AltibaseTableForeignKey(table, name, remarks, referencedKey, deleteRule, updateRule, deferability, persisted);
    }

    public JDBCStatement prepareForeignKeysLoadStatement(@NotNull JDBCSession session, @NotNull AltibaseStructContainer owner, @Nullable AltibaseTableBase forParent) throws SQLException {
        return session.getMetaData().getImportedKeys(
                owner.getCatalog() == null ? null : owner.getCatalog().getName(),
                owner.getSchema() == null || DBUtils.isVirtualObject(owner.getSchema()) ? null : owner.getSchema().getName(),
                forParent == null ?
                        owner.getDataSource().getAllObjectsPattern() :
                        forParent.getName())
                .getSourceStatement();
    }

    public boolean isFKConstraintWordDuplicated() {
        return false;
    }

    public String generateOnDeleteFK(DBSForeignKeyModifyRule deleteRule) {
        String deleteClause = deleteRule.getClause();
        if (!CommonUtils.isEmpty(deleteClause)) {
            return "ON DELETE " + deleteClause;
        }
        return null;
    }

    public String generateOnUpdateFK(DBSForeignKeyModifyRule updateRule) {
        String updateClause = updateRule.getClause();
        if (!CommonUtils.isEmpty(updateClause)) {
            return "ON UPDATE " + updateClause;
        }
        return null;
    }

    //////////////////////////////////////////////////////
    // Indexes

    public AltibaseTableIndex createIndexImpl(
        AltibaseTableBase table,
        boolean nonUnique,
        String qualifier,
        long cardinality,
        String indexName,
        DBSIndexType indexType,
        boolean persisted)
    {
        return new AltibaseTableIndex(
            table,
            nonUnique,
            qualifier,
            cardinality,
            indexName,
            indexType,
            persisted);
    }

    public AltibaseUniqueKey createConstraintImpl(AltibaseTableBase table, String constraintName, DBSEntityConstraintType constraintType, JDBCResultSet dbResult, boolean persisted) {
        return new AltibaseUniqueKey(table, constraintName, null, constraintType, persisted);
    }

    public AltibaseTableConstraintColumn[] createConstraintColumnsImpl(JDBCSession session,
                                                                      AltibaseTableBase parent, AltibaseUniqueKey object, AltibaseMetaObject pkObject, JDBCResultSet dbResult) throws DBException {
        String columnName = AltibaseUtils.safeGetStringTrimmed(pkObject, dbResult, JDBCConstants.COLUMN_NAME);
        if (CommonUtils.isEmpty(columnName)) {
            log.debug("Null primary key column for '" + object.getName() + "'");
            return null;
        }
        if ((columnName.startsWith("[") && columnName.endsWith("]")) ||
                (columnName.startsWith(SQLConstants.DEFAULT_IDENTIFIER_QUOTE) && columnName.endsWith(SQLConstants.DEFAULT_IDENTIFIER_QUOTE))) {
            // [JDBC: SQLite] Escaped column name. Let's un-escape it
            columnName = columnName.substring(1, columnName.length() - 1);
        }
        int keySeq = AltibaseUtils.safeGetInt(pkObject, dbResult, JDBCConstants.KEY_SEQ);

        AltibaseTableColumn tableColumn = parent.getAttribute(session.getProgressMonitor(), columnName);
        if (tableColumn == null) {
            log.warn("Column '" + columnName + "' not found in table '" + parent.getFullyQualifiedName(DBPEvaluationContext.DDL) + "' for PK '" + object.getFullyQualifiedName(DBPEvaluationContext.DDL) + "'");
            return null;
        }

        return new AltibaseTableConstraintColumn[] {
                new AltibaseTableConstraintColumn(object, tableColumn, keySeq) };
    }

    //////////////////////////////////////////////////////
    // Sequences

    public boolean supportsSequences(@NotNull AltibaseDataSource dataSource) {
        return true;
    }

    public JDBCStatement prepareSequencesLoadStatement(@NotNull JDBCSession session, @NotNull AltibaseStructContainer container) throws SQLException {
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

    public AltibaseSequence createSequenceImpl(@NotNull JDBCSession session, @NotNull AltibaseStructContainer container, @NotNull JDBCResultSet dbResult) throws DBException {
        return new AltibaseSequence(container, dbResult);
    }

    public boolean handleSequenceCacheReadingError(Exception error) {
        return false;
    }

    //////////////////////////////////////////////////////
    // Synonyms

    public boolean supportsSynonyms(@NotNull AltibaseDataSource dataSource) {
        return false;
    }

    public JDBCStatement prepareSynonymsLoadStatement(@NotNull JDBCSession session, @NotNull AltibaseStructContainer container) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public AltibaseSynonym createSynonymImpl(@NotNull JDBCSession session, @NotNull AltibaseStructContainer container, @NotNull JDBCResultSet dbResult) throws DBException {
        throw new DBCFeatureNotSupportedException();
    }

    //////////////////////////////////////////////////////
    // Triggers

    public boolean supportsTriggers(@NotNull AltibaseDataSource dataSource) {
        return false;
    }

    public JDBCStatement prepareTableTriggersLoadStatement(@NotNull JDBCSession session, @NotNull AltibaseStructContainer genericStructContainer, @Nullable AltibaseTableBase forParent) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public AltibaseTrigger createTableTriggerImpl(@NotNull JDBCSession session, @NotNull AltibaseStructContainer genericStructContainer, @NotNull AltibaseTableBase genericTableBase, String triggerName, @NotNull JDBCResultSet resultSet) throws DBException {
        throw new DBCFeatureNotSupportedException();
    }

    // Container triggers (not supported by default)

    public boolean supportsDatabaseTriggers(@NotNull AltibaseDataSource dataSource) {
        return false;
    }

    public JDBCStatement prepareContainerTriggersLoadStatement(@NotNull JDBCSession session, @Nullable AltibaseStructContainer forParent) throws SQLException {
        throw new SQLFeatureNotSupportedException();
    }

    public AltibaseTrigger createContainerTriggerImpl(@NotNull AltibaseStructContainer container, @NotNull JDBCResultSet resultSet) throws DBException {
        throw new DBCFeatureNotSupportedException();
    }

    public List<? extends AltibaseTrigger> loadTriggers(DBRProgressMonitor monitor, @NotNull AltibaseStructContainer container, @Nullable AltibaseTableBase table) throws DBException {
        return new ArrayList<>();
    }

    public String getTriggerDDL(@NotNull DBRProgressMonitor monitor, @NotNull AltibaseTrigger trigger) throws DBException {
        return "-- Source code not available";
    }

    // Comments

    public boolean isTableCommentEditable() {
        return false;
    }

    public boolean isTableColumnCommentEditable() {
        return false;
    }

    public boolean supportsNotNullColumnModifiers(DBSObject object) {
        return true;
    }

    public boolean isColumnNotNullByDefault() {
        return false;
    }

    public boolean hasProcedureSupport() {
        return true;
    }

    public boolean hasFunctionSupport() {
        return true;
    }

    public boolean supportsCheckConstraints() {
        return true;
    }

    public boolean supportsViews(@NotNull AltibaseDataSource dataSource) {
        DBPDataSourceInfo dataSourceInfo = dataSource.getInfo();
        return !(dataSourceInfo instanceof JDBCDataSourceInfo) ||
            ((JDBCDataSourceInfo) dataSourceInfo).supportsViews();
    }
    
    private String getViewProcDDLFromCatalog(DBRProgressMonitor monitor, DBSObject sourceObject, String schemaName, String sql) {
    	StringBuilder ddl = new StringBuilder(AltibaseMessages.NO_DBMS_METADATA);
    	String content = null;
    	boolean hasDDL = false;
    	JDBCPreparedStatement jpstmt = null;
    	JDBCResultSet jrs = null;
    	AltibaseMetaObject metaObject = getMetaObject(GenericConstants.OBJECT_TABLE);

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
                	ddl.append(StringUtil.NEW_LINE);
                }
                else {
                	hasDDL = true;
                }
                	
                content = AltibaseUtils.safeGetStringTrimmed(metaObject, jrs, "PARSE");
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
    	JDBCPreparedStatement jpstmt = null;
    	JDBCResultSet jrs = null;
    	
    	String sql = "SELECT "
    			+ " count(*)"
    			+ " FROM "
    				+ " SYSTEM_.SYS_PACKAGES_ P" 
    			+ " WHERE"
    				+ " PACKAGE_NAME = 'DBMS_METADATA' "
    				+ " AND STATUS = 0"; // valid
    	
    	try {
    		jpstmt = session.prepareStatement(sql);
    		jrs =  jpstmt.executeQuery();
    		if (jrs.next()) {
    			hasDbmsMetadataPacakge = (jrs.getInt(1) == 2);
    		}
    	} catch(Exception e) {
    		log.warn("Can't check DBMS_METADATA", e);
    	}
    	
    	return hasDbmsMetadataPacakge;
    }
}
