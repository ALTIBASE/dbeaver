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

import java.io.PrintWriter;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.core.runtime.IAdaptable;
import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ModelPreferences;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPDataSourceContainer;
import org.jkiss.dbeaver.model.DBPDataSourceInfo;
import org.jkiss.dbeaver.model.DBPErrorAssistant;
import org.jkiss.dbeaver.model.DBPObjectStatisticsCollector;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.connection.DBPConnectionConfiguration;
import org.jkiss.dbeaver.model.connection.DBPDriver;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.exec.DBCExecutionPurpose;
import org.jkiss.dbeaver.model.exec.DBCExecutionResult;
import org.jkiss.dbeaver.model.exec.DBCQueryTransformType;
import org.jkiss.dbeaver.model.exec.DBCQueryTransformer;
import org.jkiss.dbeaver.model.exec.DBCServerOutputReader;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.DBCStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCDatabaseMetaData;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCDataSource;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCExecutionContext;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCRemoteInstance;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCStructCache;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLConstants;
import org.jkiss.dbeaver.model.sql.SQLState;
import org.jkiss.dbeaver.model.struct.DBSDataType;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.DBSObjectFilter;
import org.jkiss.dbeaver.runtime.DBWorkbench;
import org.jkiss.dbeaver.utils.GeneralUtils;
import org.jkiss.utils.BeanUtils;
import org.jkiss.utils.CommonUtils;
import org.jkiss.utils.StandardConstants;

/**
 * GenericDataSource
 */
public class AltibaseDataSource extends JDBCDataSource implements DBPObjectStatisticsCollector, IAdaptable {
	
    private static final Log log = Log.getLog(AltibaseDataSource.class);

    final public SchemaCache schemaCache = new SchemaCache();
    final DataTypeCache dataTypeCache = new DataTypeCache();
    final TablespaceCache tablespaceCache = null;// = new TablespaceCache();
    final UserCache userCache = null; //new UserCache();
    final ProfileCache profileCache = null;// = new ProfileCache();
    final RoleCache roleCache = null;// = new RoleCache();
    
    private AltibaseOutputReader outputReader;
    private AltibaseSchema publicSchema;
    private boolean isAdmin;
    private boolean isAdminVisible;
    private String planTableName;
    private boolean useRuleHint;
    private boolean hasStatistics;

    private final Map<String, Boolean> availableViews = new HashMap<>();

    public AltibaseDataSource(DBRProgressMonitor monitor, DBPDataSourceContainer container)
        throws DBException {
        super(monitor, container, new AltibaseSQLDialect());
        this.outputReader = new AltibaseOutputReader();
    }

    @Override
    public Object getDataSourceFeature(String featureId) {
        switch (featureId) {
            case DBPDataSource.FEATURE_MAX_STRING_LENGTH:
                return 4000;
        }

        return super.getDataSourceFeature(featureId);
    }

    public boolean isViewAvailable(@NotNull DBRProgressMonitor monitor, @Nullable String schemaName, @NotNull String viewName) {
        viewName = viewName.toUpperCase();
        Boolean available;
        synchronized (availableViews) {
            available = availableViews.get(viewName);
        }
        if (available == null) {
            try {
                try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Check view existence")) {
                    String viewNameQuoted = DBUtils.getQuotedIdentifier(this, viewName);
                    try (final JDBCPreparedStatement dbStat = session.prepareStatement(
                        "SELECT 1 FROM " +
                            (schemaName == null ? viewNameQuoted : DBUtils.getQuotedIdentifier(this, schemaName) + "." + viewNameQuoted) +
                            " WHERE 1<>1"))
                    {
                        dbStat.setFetchSize(1);
                        dbStat.execute();
                        available = true;
                    }
                }
            } catch (Exception e) {
                available = false;
            }
            synchronized (availableViews) {
                availableViews.put(viewName, available);
            }
        }
        return available;
    }

    /* TODO
    private boolean changeExpiredPassword(DBRProgressMonitor monitor, JDBCExecutionContext context, String purpose) {
        // Ref: https://stackoverflow.com/questions/21733300/altibase-password-expiry-and-grace-period-handling-using-java-altibase-jdbc

        DBPConnectionConfiguration connectionInfo = getContainer().getActualConnectionConfiguration();
        DBAPasswordChangeInfo passwordInfo = DBWorkbench.getPlatformUI().promptUserPasswordChange("Password has expired. Set new password.", connectionInfo.getUserName(), connectionInfo.getUserPassword(), true, true);
        if (passwordInfo == null) {
            return false;
        }

        // Obtain connection
        try {
            if (passwordInfo.getNewPassword() == null) {
                throw new DBException("You can't set empty password");
            }
            Properties connectProps = getAllConnectionProperties(monitor, context, purpose, connectionInfo);
            connectProps.setProperty(JDBCConstants.PROP_USER, passwordInfo.getUserName());
            connectProps.setProperty(JDBCConstants.PROP_PASSWORD, passwordInfo.getOldPassword());
            connectProps.setProperty("altibase.jdbc.newPassword", passwordInfo.getNewPassword());

            final String url = getConnectionURL(connectionInfo);
            monitor.subTask("Connecting for expired password change");
            Driver driverInstance = getDriverInstance(monitor);
            try (Connection connection = driverInstance.connect(url, connectProps)) {
                if (connection == null) {
                    throw new DBCException("Null connection returned");
                }
            }

            connectionInfo.setUserPassword(passwordInfo.getNewPassword());
            getContainer().getConnectionConfiguration().setUserPassword(passwordInfo.getNewPassword());
            getContainer().getRegistry().flushConfig();
            return true;
        }
        catch (Exception e) {
            DBWorkbench.getPlatformUI().showError("Error changing password", "Error changing expired password", e);
            return false;
        }
    }
    */

    @Override
    protected JDBCExecutionContext createExecutionContext(JDBCRemoteInstance instance, String type) {
        return new AltibaseExecutionContext(instance, type);
    }

    protected void initializeContextState(@NotNull DBRProgressMonitor monitor, @NotNull JDBCExecutionContext context, JDBCExecutionContext initFrom) throws DBException {
        if (outputReader == null) {
            outputReader = new AltibaseOutputReader();
        }
        // Enable DBMS output
        outputReader.enableServerOutput(
            monitor,
            context,
            outputReader.isServerOutputEnabled());
        if (initFrom != null) {
            ((AltibaseExecutionContext)context).setCurrentSchema(monitor, ((AltibaseExecutionContext)initFrom).getDefaultSchema());
        } else {
            ((AltibaseExecutionContext)context).refreshDefaults(monitor, true);
        }

        {
            DBPConnectionConfiguration connectionInfo = getContainer().getConnectionConfiguration();

            try (JDBCSession session = context.openSession(monitor, DBCExecutionPurpose.META, "Set connection parameters")) {
                try {
                    readDatabaseServerVersion(session.getMetaData());
                } catch (SQLException e) {
                    log.debug("Error reading metadata", e);
                }

                /* TODO: Modify
                // Set session settings
                String sessionLanguage = connectionInfo.getProviderProperty(AltibaseConstants.PROP_SESSION_LANGUAGE);
                if (sessionLanguage != null) {
                    try {
                        JDBCUtils.executeSQL(
                            session,
                            "ALTER SESSION SET NLS_LANGUAGE='" + sessionLanguage + "'");
                    } catch (Throwable e) {
                        log.warn("Can't set session language", e);
                    }
                }
                String sessionTerritory = connectionInfo.getProviderProperty(AltibaseConstants.PROP_SESSION_TERRITORY);
                if (sessionTerritory != null) {
                    try {
                        JDBCUtils.executeSQL(
                            session,
                            "ALTER SESSION SET NLS_TERRITORY='" + sessionTerritory + "'");
                    } catch (Throwable e) {
                        log.warn("Can't set session territory", e);
                    }
                }
                setNLSParameter(session, connectionInfo, "NLS_DATE_FORMAT", AltibaseConstants.PROP_SESSION_NLS_DATE_FORMAT);
                setNLSParameter(session, connectionInfo, "NLS_TIMESTAMP_FORMAT", AltibaseConstants.PROP_SESSION_NLS_TIMESTAMP_FORMAT);
                setNLSParameter(session, connectionInfo, "NLS_LENGTH_SEMANTICS", AltibaseConstants.PROP_SESSION_NLS_LENGTH_FORMAT);
                setNLSParameter(session, connectionInfo, "NLS_CURRENCY", AltibaseConstants.PROP_SESSION_NLS_CURRENCY_FORMAT);

                boolean isMetadataContext = (
                    getContainer().getPreferenceStore().getBoolean(ModelPreferences.META_SEPARATE_CONNECTION) &&
                    !getContainer().isForceUseSingleConnection()
                ) ? JDBCExecutionContext.TYPE_METADATA.equals(context.getContextName()) : JDBCExecutionContext.TYPE_MAIN.equals(context.getContextName());


                if (isMetadataContext) {
                    if (CommonUtils.getBoolean(
                        connectionInfo.getProviderProperty(AltibaseConstants.PROP_USE_META_OPTIMIZER),
                        getContainer().getPreferenceStore().getBoolean(AltibaseConstants.PROP_USE_META_OPTIMIZER))) {
                        // See #5633
                        try {
                            JDBCUtils.executeSQL(session, "ALTER SESSION SET \"_optimizer_push_pred_cost_based\" = FALSE");
                            JDBCUtils.executeSQL(session, "ALTER SESSION SET \"_optimizer_squ_bottomup\" = FALSE");
                            JDBCUtils.executeSQL(session, "ALTER SESSION SET \"_optimizer_cost_based_transformation\" = 'OFF'");
                            if (isServerVersionAtLeast(10, 2)) {
                                JDBCUtils.executeSQL(session, "ALTER SESSION SET OPTIMIZER_FEATURES_ENABLE='10.2.0.5'");
                            }
                        } catch (Throwable e) {
                            log.warn("Can't set session optimizer parameters", e);
                        }
                    }
                }
                */
                
            }
        }
    }

    private void setNLSParameter(JDBCSession session, DBPConnectionConfiguration connectionInfo, String oraNlsName, String paramName) {
        String paramValue = connectionInfo.getProviderProperty(paramName);
        if (!CommonUtils.isEmpty(paramValue)) {
            try {
                JDBCUtils.executeSQL(
                    session,
                    "ALTER SESSION SET "+ oraNlsName + "='" + paramValue + "'");
            } catch (Throwable e) {
                log.warn("Can not set session NLS parameter " + oraNlsName, e);
            }
        }
    }

    public AltibaseSchema getDefaultSchema() {
        return (AltibaseSchema) DBUtils.getDefaultContext(this, true).getContextDefaults().getDefaultSchema();
    }

    @Override
    protected DBPDataSourceInfo createDataSourceInfo(DBRProgressMonitor monitor, @NotNull JDBCDatabaseMetaData metaData) {
        return new AltibaseDataSourceInfo(this, metaData);
    }

    @Override
    public ErrorType discoverErrorType(@NotNull Throwable error) {
        Throwable rootCause = GeneralUtils.getRootCause(error);
        if (rootCause instanceof SQLException) {
            switch (((SQLException) rootCause).getErrorCode()) {
                case AltibaseConstants.EC_NO_RESULTSET_AVAILABLE:
                    return ErrorType.RESULT_SET_MISSING;
                case AltibaseConstants.EC_FEATURE_NOT_SUPPORTED:
                    return ErrorType.FEATURE_UNSUPPORTED;
            }
        }
        return super.discoverErrorType(error);
    }

    @Override
    protected Map<String, String> getInternalConnectionProperties(DBRProgressMonitor monitor, DBPDriver driver, JDBCExecutionContext context, String purpose, DBPConnectionConfiguration connectionInfo) throws DBCException {
        Map<String, String> connectionsProps = new HashMap<>();
        if (!getContainer().getPreferenceStore().getBoolean(ModelPreferences.META_CLIENT_NAME_DISABLE)) {
            // Program name
            String appName = DBUtils.getClientApplicationName(getContainer(), context, purpose);
            appName = appName.replaceAll("[^ a-zA-Z0-9]", "?"); // Replace any special characters - Altibase don't like them
            connectionsProps.put("v$session.program", CommonUtils.truncateString(appName, 48));
        }
        // FIXME: left for backward compatibility. Replaced by auth model. Remove in future.
        if (CommonUtils.toBoolean(connectionInfo.getProviderProperty(AltibaseConstants.OS_AUTH_PROP))) {
            connectionsProps.put("v$session.osuser", System.getProperty(StandardConstants.ENV_USER_NAME));
        }
        return connectionsProps;
    }

    public boolean isAdmin() {
        return isAdmin;
    }

    public boolean isAdminVisible() {
        return isAdmin || isAdminVisible;
    }

    public boolean isUseRuleHint() {
        return useRuleHint;
    }

    @Association
    public Collection<AltibaseSchema> getSchemas(DBRProgressMonitor monitor) throws DBException {
        return schemaCache.getAllObjects(monitor, this);
    }

    public AltibaseSchema getSchema(DBRProgressMonitor monitor, String name) throws DBException {
        if (publicSchema != null && publicSchema.getName().equals(name)) {
            return publicSchema;
        }
        // Schema cache may be null during DataSource initialization
        return schemaCache == null ? null : schemaCache.getObject(monitor, this, name);
    }

    @Association
    public Collection<AltibaseTablespace> getTablespaces(DBRProgressMonitor monitor) throws DBException {
        return tablespaceCache.getAllObjects(monitor, this);
    }

    @Association
    public Collection<AltibaseUser> getUsers(DBRProgressMonitor monitor) throws DBException {
        return userCache.getAllObjects(monitor, this);
    }

    @Association
    public AltibaseUser getUser(DBRProgressMonitor monitor, String name) throws DBException {
        return userCache.getObject(monitor, this, name);
    }

    @Association
    public Collection<AltibaseUserProfile> getProfiles(DBRProgressMonitor monitor) throws DBException {
        return profileCache.getAllObjects(monitor, this);
    }

    @Association
    public Collection<AltibaseRole> getRoles(DBRProgressMonitor monitor) throws DBException {
        return roleCache.getAllObjects(monitor, this);
    }

    public AltibaseGrantee getGrantee(DBRProgressMonitor monitor, String name) throws DBException {
        AltibaseUser user = userCache.getObject(monitor, this, name);
        if (user != null) {
            return user;
        }
        return roleCache.getObject(monitor, this, name);
    }

    @Association
    public Collection<AltibaseSynonym> getPublicSynonyms(DBRProgressMonitor monitor) throws DBException {
        return publicSchema.getSynonyms(monitor);
    }

    public boolean isAtLeastV9() {
        return getInfo().getDatabaseVersion().getMajor() >= 9;
    }

    public boolean isAtLeastV10() {
        return getInfo().getDatabaseVersion().getMajor() >= 10;
    }

    public boolean isAtLeastV11() {
        return getInfo().getDatabaseVersion().getMajor() >= 11;
    }

    public boolean isAtLeastV12() {
        return getInfo().getDatabaseVersion().getMajor() >= 12;
    }

    @Override
    public void initialize(@NotNull DBRProgressMonitor monitor)
        throws DBException {
        super.initialize(monitor);

        DBPConnectionConfiguration connectionInfo = getContainer().getConnectionConfiguration();

        {
            String useRuleHintProp = connectionInfo.getProviderProperty(AltibaseConstants.PROP_USE_RULE_HINT);
            if (useRuleHintProp != null) {
                useRuleHint = CommonUtils.getBoolean(useRuleHintProp, false);
            }
        }

        this.publicSchema = new AltibaseSchema(this, 1, AltibaseConstants.USER_PUBLIC);

        // Cache data types
        dataTypeCache.setCaseSensitive(false);
        {
            List<AltibaseDataType> dtList = new ArrayList<>();
            for (Map.Entry<String, AltibaseDataType.TypeDesc> predefinedType : AltibaseDataType.PREDEFINED_TYPES.entrySet()) {
                AltibaseDataType dataType = new AltibaseDataType(this, predefinedType.getKey(), true);
                dtList.add(dataType);
            }
            this.dataTypeCache.setCache(dtList);
        }
    }

    @Override
    public DBSObject refreshObject(@NotNull DBRProgressMonitor monitor)
        throws DBException {
        super.refreshObject(monitor);

        this.schemaCache.clearCache();
        //this.dataTypeCache.clearCache();
        this.tablespaceCache.clearCache();
        this.userCache.clearCache();
        this.profileCache.clearCache();
        this.roleCache.clearCache();

        this.initialize(monitor);

        return this;
    }

    @Override
    public Collection<AltibaseSchema> getChildren(@NotNull DBRProgressMonitor monitor)
        throws DBException {
        return getSchemas(monitor);
    }

    @Override
    public AltibaseSchema getChild(@NotNull DBRProgressMonitor monitor, @NotNull String childName)
        throws DBException {
        return getSchema(monitor, childName);
    }

    @NotNull
    @Override
    public Class<? extends AltibaseSchema> getPrimaryChildType(@Nullable DBRProgressMonitor monitor)
        throws DBException {
        return AltibaseSchema.class;
    }

    @Override
    public void cacheStructure(@NotNull DBRProgressMonitor monitor, int scope)
        throws DBException {

    }

    /* TODO
    @Nullable
    @Override
    public <T> T getAdapter(Class<T> adapter) {
        if (adapter == DBSStructureAssistant.class) {
            return adapter.cast(new AltibaseStructureAssistant(this));
        } else if (adapter == DBCServerOutputReader.class) {
            return adapter.cast(outputReader);
        } else if (adapter == DBAServerSessionManager.class) {
            return adapter.cast(new AltibaseServerSessionManager(this));
        } else if (adapter == DBCQueryPlanner.class) {
            return adapter.cast(new AltibaseQueryPlanner(this));
        } else if(adapter == DBAUserPasswordManager.class) {
            return adapter.cast(new AltibaseChangeUserPasswordManager(this));
        }
        return super.getAdapter(adapter);
    }
    */

    @Override
    public void cancelStatementExecute(DBRProgressMonitor monitor, JDBCStatement statement) throws DBException {
        if (driverSupportsQueryCancel()) {
            super.cancelStatementExecute(monitor, statement);
        } else {
            try {
                Connection connection = statement.getConnection().getOriginal();
                BeanUtils.invokeObjectMethod(connection, "cancel");
            } catch (Throwable e) {
                throw new DBException("Can't cancel session queries", e, this);
            }
        }
    }

    private boolean driverSupportsQueryCancel() {
        return true;
    }

    @NotNull
    @Override
    public AltibaseDataSource getDataSource() {
        return this;
    }

    @Override
    public Collection<? extends DBSDataType> getLocalDataTypes() {
        return dataTypeCache.getCachedObjects();
    }

    @Override
    public AltibaseDataType getLocalDataType(String typeName) {
        return dataTypeCache.getCachedObject(typeName);
    }

    public DataTypeCache getDataTypeCache() {
        return dataTypeCache;
    }

    @Nullable
    @Override
    public AltibaseDataType resolveDataType(@NotNull DBRProgressMonitor monitor, @NotNull String typeFullName) throws DBException {
        int divPos = typeFullName.indexOf(SQLConstants.STRUCT_SEPARATOR);
        if (divPos == -1) {
            // Simple type name
            return getLocalDataType(typeFullName);
        } else {
            String schemaName = typeFullName.substring(0, divPos);
            String typeName = typeFullName.substring(divPos + 1);
            AltibaseSchema schema = getSchema(monitor, schemaName);
            if (schema == null) {
                return null;
            }
            return schema.getDataType(monitor, typeName);
        }
    }

    @Nullable
    public String getPlanTableName(JDBCSession session)
        throws DBException
    {
        if (planTableName == null) {
            String[] candidateNames;
            String tableName = getContainer().getPreferenceStore().getString(AltibaseConstants.PREF_EXPLAIN_TABLE_NAME);
            if (!CommonUtils.isEmpty(tableName)) {
                candidateNames = new String[]{tableName};
            } else {
                candidateNames = new String[]{"PLAN_TABLE", "TOAD_PLAN_TABLE"};
            }
            for (String candidate : candidateNames) {
                try {
                    JDBCUtils.executeSQL(session, "SELECT 1 FROM " + candidate);
                } catch (SQLException e) {
                    // No such table
                    continue;
                }
                planTableName = candidate;
                break;
            }
            if (planTableName == null) {
                final String newPlanTableName = candidateNames[0];
                // Plan table not found - try to create new one
                if (!DBWorkbench.getPlatformUI().confirmAction(
                    "Altibase PLAN_TABLE missing",
                    "PLAN_TABLE not found in current user's session. " +
                        "Do you want DBeaver to create new PLAN_TABLE (" + newPlanTableName + ")?")) {
                    return null;
                }
                planTableName = createPlanTable(session, newPlanTableName);
            }
        }
        return planTableName;
    }

    private String createPlanTable(JDBCSession session, String tableName) throws DBException {
        try {
            JDBCUtils.executeSQL(session, AltibaseConstants.PLAN_TABLE_DEFINITION.replace("${TABLE_NAME}", tableName));
        } catch (SQLException e) {
            throw new DBException("Error creating PLAN table", e, this);
        }
        return tableName;
    }

    @Nullable
    @Override
    public DBCQueryTransformer createQueryTransformer(@NotNull DBCQueryTransformType type) {
        if (type == DBCQueryTransformType.RESULT_SET_LIMIT) {
            //return new QueryTransformerRowNum();
        }
        return super.createQueryTransformer(type);
    }

    private Pattern ERROR_POSITION_PATTERN = Pattern.compile(".+\\s+line ([0-9]+), column ([0-9]+)");
    private Pattern ERROR_POSITION_PATTERN_2 = Pattern.compile(".+\\s+at line ([0-9]+)");
    private Pattern ERROR_POSITION_PATTERN_3 = Pattern.compile(".+\\s+at position\\: ([0-9]+)");

    @Nullable
    @Override
    public ErrorPosition[] getErrorPosition(@NotNull DBRProgressMonitor monitor, @NotNull DBCExecutionContext context, @NotNull String query, @NotNull Throwable error) {
        while (error instanceof DBException) {
            if (error.getCause() == null) {
                break;
            }
            error = error.getCause();
        }
        String message = error.getMessage();
        if (!CommonUtils.isEmpty(message)) {
            List<ErrorPosition> positions = new ArrayList<>();
            Matcher matcher = ERROR_POSITION_PATTERN.matcher(message);
            while (matcher.find()) {
                DBPErrorAssistant.ErrorPosition pos = new DBPErrorAssistant.ErrorPosition();
                pos.info = matcher.group(1);
                pos.line = Integer.parseInt(matcher.group(1)) - 1;
                pos.position = Integer.parseInt(matcher.group(2)) - 1;
                positions.add(pos);
            }
            if (positions.isEmpty()) {
                matcher = ERROR_POSITION_PATTERN_2.matcher(message);
                while (matcher.find()) {
                    DBPErrorAssistant.ErrorPosition pos = new DBPErrorAssistant.ErrorPosition();
                    pos.info = matcher.group(1);
                    pos.line = Integer.parseInt(matcher.group(1)) - 1;
                    positions.add(pos);
                }
            }
            if (positions.isEmpty()) {
                matcher = ERROR_POSITION_PATTERN_3.matcher(message);
                while (matcher.find()) {
                    DBPErrorAssistant.ErrorPosition pos = new DBPErrorAssistant.ErrorPosition();
                    pos.info = matcher.group(1);
                    pos.position = Integer.parseInt(matcher.group(1)) - 1;
                    positions.add(pos);
                }
            }

            if (!positions.isEmpty()) {
                return positions.toArray(new ErrorPosition[positions.size()]);
            }
        }
        if (error.getCause() != null) {
            // Maybe AltibaseDatabaseException
            try {
                Object errorPosition = BeanUtils.readObjectProperty(error.getCause(), "errorPosition");
                if (errorPosition instanceof Number) {
                    DBPErrorAssistant.ErrorPosition pos = new DBPErrorAssistant.ErrorPosition();
                    pos.position = ((Number) errorPosition).intValue();
                    return new ErrorPosition[]{pos};
                }
            } catch (Exception e) {
                // Nope, its not it
            }

        }
        if (error instanceof SQLException && SQLState.SQL_42000.getCode().equals(((SQLException) error).getSQLState())) {
            try (JDBCSession session = (JDBCSession) context.openSession(monitor, DBCExecutionPurpose.UTIL, "Extract last error position")) {
                try (CallableStatement stat = session.prepareCall(
                    "declare\n" +
                        "  l_cursor integer default dbms_sql.open_cursor; \n" +
                        "begin \n" +
                        "  begin \n" +
                        "  dbms_sql.parse(  l_cursor, ?, dbms_sql.native ); \n" +
                        "    exception \n" +
                        "      when others then ? := dbms_sql.last_error_position; \n" +
                        "    end; \n" +
                        "    dbms_sql.close_cursor( l_cursor );\n" +
                        "end;")) {
                    stat.setString(1, query);
                    stat.registerOutParameter(2, Types.INTEGER);
                    stat.execute();
                    int errorPos = stat.getInt(2);
                    if (errorPos <= 0) {
                        return null;
                    }

                    DBPErrorAssistant.ErrorPosition pos = new DBPErrorAssistant.ErrorPosition();
                    pos.position = errorPos;
                    return new ErrorPosition[]{pos};

                } catch (SQLException e) {
                    // Something went wrong
                    log.debug("Can't extract parse error info: " + e.getMessage());
                }
            }
        }
        return null;
    }

    ///////////////////////////////////////////////
    // Statistics

    @Override
    public boolean isStatisticsCollected() {
        return hasStatistics;
    }

    @Override
    public void collectObjectStatistics(DBRProgressMonitor monitor, boolean totalSizeOnly, boolean forceRefresh) throws DBException {
        if (hasStatistics && !forceRefresh) {
            return;
        }
        try (final JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load tablespace '" + getName() + "' statistics")) {
            // Tablespace stats
            try (JDBCStatement dbStat = session.createStatement()) {
                try (JDBCResultSet dbResult = dbStat.executeQuery(
                    "SELECT\n" +
                    "\tTS.TABLESPACE_NAME, F.AVAILABLE_SPACE, S.USED_SPACE\n" +
                    "FROM\n" +
                    "\tSYS.DBA_TABLESPACES TS,\n" +
                    "\t(SELECT TABLESPACE_NAME, SUM(BYTES) AVAILABLE_SPACE FROM DBA_DATA_FILES GROUP BY TABLESPACE_NAME) F,\n" +
                    "\t(SELECT TABLESPACE_NAME, SUM(BYTES) USED_SPACE FROM DBA_SEGMENTS GROUP BY TABLESPACE_NAME) S\n" +
                    "WHERE\n" +
                    "\tF.TABLESPACE_NAME(+) = TS.TABLESPACE_NAME AND S.TABLESPACE_NAME(+) = TS.TABLESPACE_NAME")) {
                    while (dbResult.next()) {
                        String tsName = dbResult.getString(1);
                        AltibaseTablespace tablespace = tablespaceCache.getObject(monitor, getDataSource(), tsName);
                        if (tablespace != null) {
                            tablespace.fetchSizes(dbResult);
                        }
                    }
                }
            }
        } catch (SQLException e) {
            throw new DBException("Can't read tablespace statistics", e, getDataSource());
        } finally {
            hasStatistics = true;
        }
    }

    private class AltibaseOutputReader implements DBCServerOutputReader {
        @Override
        public boolean isServerOutputEnabled() {
            return false;
        }

        @Override
        public boolean isAsyncOutputReadSupported() {
            return false;
        }

        public void enableServerOutput(DBRProgressMonitor monitor, DBCExecutionContext context, boolean enable) throws DBCException {
        	/* TODO: Modify
            String sql = enable ?
                "BEGIN DBMS_OUTPUT.ENABLE(" + AltibaseConstants.MAXIMUM_DBMS_OUTPUT_SIZE + "); END;" :
                "BEGIN DBMS_OUTPUT.DISABLE; END;";
            try (DBCSession session = context.openSession(monitor, DBCExecutionPurpose.UTIL, (enable ? "Enable" : "Disable ") + "DBMS output")) {
                JDBCUtils.executeSQL((JDBCSession) session, sql);
            } catch (SQLException e) {
                throw new DBCException(e, context);
            }
            */
        }

        @Override
        public void readServerOutput(@NotNull DBRProgressMonitor monitor, @NotNull DBCExecutionContext context, @Nullable DBCExecutionResult executionResult, @Nullable DBCStatement statement, @NotNull PrintWriter output) throws DBCException {
            try (JDBCSession session = (JDBCSession) context.openSession(monitor, DBCExecutionPurpose.UTIL, "Read DBMS output")) {
                try (CallableStatement getLineProc = session.getOriginal().prepareCall("{CALL DBMS_OUTPUT.GET_LINE(?, ?)}")) {
                    getLineProc.registerOutParameter(1, java.sql.Types.VARCHAR);
                    getLineProc.registerOutParameter(2, java.sql.Types.INTEGER);
                    int status = 0;
                    while (status == 0) {
                        getLineProc.execute();
                        status = getLineProc.getInt(2);
                        if (status == 0) {
                            String str = getLineProc.getString(1);
                            if (str != null) {
                                output.write(str);
                            }
                            output.write('\n');
                        }
                    }
                } catch (SQLException e) {
                    throw new DBCException(e, context);
                }
            }
        }
    }

    static class SchemaCache extends JDBCObjectCache<AltibaseDataSource, AltibaseSchema> {
        SchemaCache() {
            setListOrderComparator(DBUtils.<AltibaseSchema>nameComparator());
        }

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner) throws SQLException {
            String schemasQuery = "SELECT * FROM SYSTEM_.SYS_USERS_ WHERE USER_TYPE = 'U' ORDER BY USER_NAME";
            JDBCPreparedStatement dbStat = session.prepareStatement(schemasQuery);
            DBSObjectFilter schemaFilters = owner.getContainer().getObjectFilter(AltibaseSchema.class, null, false);
            JDBCUtils.setFilterParameters(dbStat, 1, schemaFilters);
            
            return dbStat;
        }

        @Override
        protected AltibaseSchema fetchObject(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException {
            return new AltibaseSchema(owner, resultSet);
        }

        @Override
        protected void invalidateObjects(DBRProgressMonitor monitor, AltibaseDataSource owner, Iterator<AltibaseSchema> objectIter) {
            setListOrderComparator(DBUtils.<AltibaseSchema>nameComparator());
        }
    }

    static class DataTypeCache extends JDBCObjectCache<AltibaseDataSource, AltibaseDataType> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner) throws SQLException {
            return session.prepareStatement(
                "SELECT * FROM V$DATATYPE ORDER BY TYPE_NAME");
        }

        @Override
        protected AltibaseDataType fetchObject(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException {
            return new AltibaseDataType(owner, resultSet);
        }
    }

    static class TablespaceCache extends JDBCObjectCache<AltibaseDataSource, AltibaseTablespace> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner) throws SQLException {
            return session.prepareStatement(
                "SELECT * FROM V$TABLESPACES ORDER BY NAME");
        }

        @Override
        protected AltibaseTablespace fetchObject(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException {
            return new AltibaseTablespace(owner, resultSet);
        }
    }

    static class UserCache extends JDBCObjectCache<AltibaseDataSource, AltibaseUser> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner) throws SQLException {
            return session.prepareStatement("SELECT * FROM SYSTEM_.SYS_USERS_ WHERE USER_TYPE = 'U' ORDER BY USER_NAME");
        }

        @Override
        protected AltibaseUser fetchObject(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException {
            return new AltibaseUser(owner, resultSet);
        }
    }

    static class RoleCache extends JDBCObjectCache<AltibaseDataSource, AltibaseRole> {
        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner) throws SQLException {
            return session.prepareStatement(
            		"SELECT * FROM SYSTEM_.SYS_USERS_ WHERE USER_TYPE = 'R' AND USER_NAME != 'PUBLIC' ORDER BY USER_NAME");
        }

        @Override
        protected AltibaseRole fetchObject(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException {
            return new AltibaseRole(owner, resultSet);
        }
    }

    static class ProfileCache extends JDBCStructCache<AltibaseDataSource, AltibaseUserProfile, AltibaseUserProfile.ProfileResource> {
        protected ProfileCache() {
            super("PROFILE");
        }

        @NotNull
        @Override
        protected JDBCStatement prepareObjectsStatement(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner) throws SQLException {
            return session.prepareStatement(
                "SELECT DISTINCT PROFILE FROM DBA_PROFILES ORDER BY PROFILE");
        }

        @Override
        protected AltibaseUserProfile fetchObject(@NotNull JDBCSession session, @NotNull AltibaseDataSource owner, @NotNull JDBCResultSet resultSet) throws SQLException, DBException {
            return new AltibaseUserProfile(owner, resultSet);
        }

        @Override
        protected JDBCStatement prepareChildrenStatement(@NotNull JDBCSession session, @NotNull AltibaseDataSource dataSource, @Nullable AltibaseUserProfile forObject) throws SQLException {
            final JDBCPreparedStatement dbStat = session.prepareStatement(
                "SELECT RESOURCE_NAME,RESOURCE_TYPE,LIMIT FROM DBA_PROFILES " +
                    (forObject == null ? "" : "WHERE PROFILE=? ") +
                    "ORDER BY RESOURCE_NAME");
            if (forObject != null) {
                dbStat.setString(1, forObject.getName());
            }
            return dbStat;
        }

        @Override
        protected AltibaseUserProfile.ProfileResource fetchChild(@NotNull JDBCSession session, @NotNull AltibaseDataSource dataSource, @NotNull AltibaseUserProfile parent, @NotNull JDBCResultSet dbResult) throws SQLException, DBException {
            return new AltibaseUserProfile.ProfileResource(parent, dbResult);
        }
    }

    /*
    @NotNull
    @Override
    protected String getStandardSQLDataTypeName(@NotNull DBPDataKind dataKind) {
        switch (dataKind) {
            case BOOLEAN: return AltibaseConstants.TYPE_NAME_BOOLEAN;
            case NUMERIC: return AltibaseConstants.TYPE_NAME_NUMERIC;
            case DATETIME: return AltibaseConstants.TYPE_NAME_TIMESTAMP;
            case BINARY:
            case CONTENT:
                return AltibaseConstants.TYPE_NAME_BLOB;
            case ROWID: return AltibaseConstants.TYPE_NAME_ROWID;
            default: return AltibaseConstants.TYPE_NAME_VARCHAR;
        }
    }
    */
}
