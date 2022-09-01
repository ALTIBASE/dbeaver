package org.jkiss.dbeaver.ext.altibase.model;

import org.jkiss.dbeaver.ext.generic.model.GenericFunctionResultType;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;

public class AltibaseTypeset extends AltibaseProcedure {

	//private boolean isValid;
	
	public AltibaseTypeset(GenericStructContainer container, String procedureName) {
		super(container, procedureName, procedureName, "", DBSProcedureType.UNKNOWN, GenericFunctionResultType.NO_TABLE);
		//this.isValid = isValid;
	}
}
