package org.jkiss.dbeaver.ext.altibase.model;

import org.jkiss.dbeaver.ext.generic.model.GenericPackage;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;

public class AltibasePackage extends GenericPackage {

	private boolean status; // 0: Valid, 1: Invalid
	private int  authId; 	// 0: DEFINER, 1: CURRENT_USER
	private boolean hasBody;
	
	public AltibasePackage(GenericStructContainer container, String packageName, JDBCResultSet dbResult) {
		super(container, packageName, true);
		
		status = (JDBCUtils.safeGetInt(dbResult, "STATUS") == 0);
		authId = JDBCUtils.safeGetInt(dbResult, "AUTHID");
	}
	
	public void setBody(boolean hasBody) {
		this.hasBody = hasBody;
	}

}
