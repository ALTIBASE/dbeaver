
package org.jkiss.dbeaver.ext.altibase.model;

import java.util.Map;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericFunctionResultType;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;

public class AltibaseProcedureStandAlone extends AltibaseProcedureBase {
	
	public AltibaseProcedureStandAlone(GenericStructContainer container, String procedureName, String specificName,
			String description, DBSProcedureType procedureType, GenericFunctionResultType functionResultType) {
		super(container, procedureName, specificName, description, procedureType, functionResultType);
	}

    @Override
    public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBException {
        if (source == null) {
            source = getDataSource().getMetaModel().getProcedureDDL(monitor, this);
        }
        return source;
    }
    
    public String getProcedureTypeName()
    {
    	DBSProcedureType procedureType = getProcedureType();
    	if (procedureType == DBSProcedureType.UNKNOWN) {
    		return "TYPESET";
    	} else {
    		return procedureType.name();
    	}
    }
}
