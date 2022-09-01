package org.jkiss.dbeaver.ext.altibase.model;

import java.util.Map;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.altibase.AltibaseConstants;
import org.jkiss.dbeaver.ext.altibase.AltibaseUtils;
import org.jkiss.dbeaver.ext.generic.model.GenericCatalog;
import org.jkiss.dbeaver.ext.generic.model.GenericPackage;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;

public class AltibasePackage extends GenericPackage {

	private String source;
	
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

    @Override
    //@Property(hidden = true, editable = true, updatable = true, order = -1)
    public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBException {
        if (source == null) {
        	source = "-- Package specification " 
        			+ AltibaseUtils.NEW_LINE 
        			+ ((AltibaseMetaModel)getDataSource().getMetaModel()).getPackageDDL(monitor, this, AltibaseConstants.PACKAGE_SPEC)
        			+ AltibaseUtils.NEW_LINE 
        			+ "-- Package body " 
        			+ AltibaseUtils.NEW_LINE;
        	
        	if (hasBody) {
        		source += ((AltibaseMetaModel)getDataSource().getMetaModel()).getPackageDDL(monitor, this, AltibaseConstants.PACKAGE_BODY);
        	} else {
        		source += "-- No body definition";
        	}
        }

        return source;
    }
    
    @Property(viewable = true, order = 5)
    public boolean isValid()
    {
        return status;
    }
}
