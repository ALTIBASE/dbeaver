package org.jkiss.dbeaver.ext.altibase.model;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.generic.model.GenericDataTypeCache;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCConstants;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCBasicDataTypeCache;
import org.jkiss.dbeaver.model.messages.ModelMessages;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

/**
 * AltibaseDataTypeCache
 */
public class AltibaseDataTypeCache extends JDBCBasicDataTypeCache<GenericStructContainer, AltibaseDataType>
{
    private static final Log log = Log.getLog(GenericDataTypeCache.class);

    public AltibaseDataTypeCache(GenericStructContainer owner) {
        super(owner);
    }

    @Override
    protected synchronized void loadObjects(DBRProgressMonitor monitor, GenericStructContainer container) throws DBException {
        AltibaseDataSource dataSource = (AltibaseDataSource) container.getDataSource();

        if (dataSource == null) {
            throw new DBException(ModelMessages.error_not_connected_to_database);
        }
        
        // Load domain types
        List<AltibaseDataType> tmpObjectList = new ArrayList<>();

        /*
        for (AltibaseFieldType fieldType : AltibaseFieldType.values()) {
            AltibaseDataType dataType = new AltibaseDataType(dataSource, fieldType);
            tmpObjectList.add(dataType);
        }
        */

        try {
            try (JDBCSession session = DBUtils.openMetaSession(monitor, dataSource, "Load Altibase data types")) {
                // Use CAST to improve performance, binaries are too slow
                try (JDBCPreparedStatement dbStat = session.prepareStatement(
                    "SELECT * FROM V$DATATYPE ORDER BY TYPE_NAME"))
                {
                    monitor.subTask("Load Altibase domain types");
                    try (JDBCResultSet dbResult = dbStat.executeQuery()) {
                        while (dbResult.next()) {
                            if (monitor.isCanceled()) {
                                break;
                            }
                            // super(owner, valueType, name, remarks, unsigned, searchable, precision, minScale, maxScale);
                            String typeName = JDBCUtils.safeGetString(dbResult, "TYPE_NAME");
                            if (typeName == null) {
                                continue;
                            }
                            int dataTypeId = JDBCUtils.safeGetInt(dbResult, "DATA_TYPE");
                            boolean searchabel = (JDBCUtils.safeGetInt(dbResult, "SEARCHABLE") > 0);
                            int precision = JDBCUtils.safeGetInt(dbResult, "COLUMN_SIZE");
                            int nullable = JDBCUtils.safeGetInt(dbResult, "NULLABLE");
                            boolean unsinged = (JDBCUtils.safeGetInt(dbResult, "UNSIGNED_ATTRIBUTE") == 1);
                            String remarks = JDBCUtils.safeGetString(dbResult, "LOCAL_TYPE_NAME");
                            int minScale = JDBCUtils.safeGetInt(dbResult, "MINIMUM_SCALE");
                            int maxScale = JDBCUtils.safeGetInt(dbResult, "MAXIMUM_SCALE");

                            AltibaseDataTypeDomain fieldType = AltibaseDataTypeDomain.getById(dataTypeId);
                            if (fieldType == null) {
                                log.error("Field type '" + fieldType + "' not found");
                                continue;
                            }

                            // GenericStructContainer owner, int valueType, String name, String remarks, boolean unsigned,
                			//boolean searchable, int precision, int minScale, int maxScale) {
                            AltibaseDataType dataType = new AltibaseDataType(
                                dataSource, fieldType, typeName, remarks, unsinged, searchabel, precision, minScale, maxScale);
                            tmpObjectList.add(dataType);
                        }
                    }
                }
            } catch (SQLException ex) {
                throw new DBException(ex, dataSource);
            }
        } catch (DBException e) {
            if (!handleCacheReadError(e)) {
                throw e;
            }
        }

        mergeCache(tmpObjectList);
    }

}
