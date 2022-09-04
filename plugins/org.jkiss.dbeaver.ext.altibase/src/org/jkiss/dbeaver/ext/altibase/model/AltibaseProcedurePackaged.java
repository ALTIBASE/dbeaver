package org.jkiss.dbeaver.ext.altibase.model;

import java.sql.SQLException;
import java.util.Map;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericFunctionResultType;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.DBConstants;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureParameterKind;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;

public class AltibaseProcedurePackaged extends AltibaseProcedureBase {

	private String pkgSchema;
	
	public AltibaseProcedurePackaged(GenericStructContainer container, String procedureName, String specificName,
			String description, DBSProcedureType procedureType, GenericFunctionResultType functionResultType, String pkgSchema) {
		super(container, procedureName, specificName, description, procedureType, functionResultType);
		this.pkgSchema = pkgSchema;
	}

    @Override
    public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBException {
        return "-- Unable to get pacakge depedent object source";
    }
    
    public void loadProcedureColumns(DBRProgressMonitor monitor) throws DBException
    {
        try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Load procedure columns")) {
            JDBCPreparedStatement dbStat = ((AltibaseMetaModel)getDataSource().getMetaModel()).prepareProcedurePackagedColumnLoadStatement(session, pkgSchema, container.getName(), this.getName());
    		dbStat.setFetchSize(DBConstants.METADATA_FETCH_SIZE);
            dbStat.executeStatement();
            JDBCResultSet dbResult = dbStat.getResultSet();
            try {
                while (dbResult.next()) {
                	boolean isFunction  = (JDBCUtils.safeGetInt(dbResult, "SUB_TYPE") == 1);
                    String columnName 	= JDBCUtils.safeGetString(dbResult, "PARA_NAME");
                    int position 		= JDBCUtils.safeGetInt(dbResult, "PARA_ORDER");
                    int columnSize 		= JDBCUtils.safeGetInt(dbResult, "SIZE");
                    int precision 		= JDBCUtils.safeGetInt(dbResult, "PRECISION");
                    int scale 			= JDBCUtils.safeGetInt(dbResult, "SCALE");
                    
                    int columnTypeNum 	= JDBCUtils.safeGetInt(dbResult, "INOUT_TYPE"); // 0: IN, 1: OUT, 2: IN OUT
                    int valueType 		= JDBCUtils.safeGetInt(dbResult, "DATA_TYPE");;
                    String typeName 	= JDBCUtils.safeGetString(dbResult, "TYPE_NAME");
                    String defaultValue = JDBCUtils.safeGetString(dbResult, "DEFAULT_VAL");
                    boolean notNull 	= (defaultValue == null);
                    String remarks 		= "";
                    
                    DBSProcedureParameterKind parameterType;

                    switch (columnTypeNum) {
                        case AltibaseProcedureParameter.PARAM_IN:
                            parameterType = DBSProcedureParameterKind.IN;
                            break;
                        case AltibaseProcedureParameter.PARAM_INOUT:
                            parameterType = DBSProcedureParameterKind.INOUT;
                            break;
                        case AltibaseProcedureParameter.PARAM_OUT:
                            parameterType = DBSProcedureParameterKind.OUT;
                            break;
                        default:
                            parameterType = DBSProcedureParameterKind.UNKNOWN;
                            break;
                    }
                    
                    // procedure with no argument case
                    if (isFunction == false && columnName == null && position == 0) {
                    	return; 
                    }
                    
                    // function return type
                    if (isFunction == true && columnName == null && position == 1) {
                    	continue; 
                    }
                    
                    if (isFunction == true) {
                    	--position;
                    }
                    
                    AltibaseProcedureParameter column = new AltibaseProcedureParameter(
                        this,
                        columnName,
                        typeName,
                        valueType,
                        position,
                        columnSize,
                        scale, precision, notNull,
                        remarks,
                        parameterType);

                    this.addColumn(column);
                }
            } finally {
                dbResult.close();
            }
        } catch (SQLException e) {
            throw new DBException(e, getDataSource());
        }

    }
}
