package org.jkiss.dbeaver.ext.altibase.model;

import java.util.Map;

import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.altibase.AltibaseConstants;
import org.jkiss.dbeaver.ext.altibase.AltibaseUtils;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericSynonym;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBPScriptObject;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.utils.CommonUtils;

public class AltibaseSynonym extends GenericSynonym implements DBPScriptObject {

	protected boolean isPublic;
	protected String ref_object_schema;
	protected String ref_object_name;
	
	protected String ddl;
	
	protected AltibaseSynonym(GenericStructContainer container, int owner_id, String name, String description, String ref_object_schema, String ref_object_name) {
		super(container, name, description);
		
		isPublic = (owner_id < 1);
		this.ref_object_schema = ref_object_schema;
		this.ref_object_name = ref_object_name;
	}

    @Nullable
    @Property(id="Reference", viewable = true, order = 3)
    public String getReferencedObjectName()
    {
    	return AltibaseUtils.getQuotedName(ref_object_schema, ref_object_name);
    }
    
	@Override
	public DBSObject getTargetObject(DBRProgressMonitor monitor) throws DBException {
		//getDataSource().
		return null;
	}

	public String getSchemaName() {
		return getParentObject().getName();
	}
	
	public String getDdlLocal()
	{
		String ddl;
		
		if (isPublic) {
			ddl = String.format("CREATE PUBLIC SYNONYM %s FOR %s;", 
			        AltibaseUtils.getQuotedName(null, getName()), 
					getReferencedObjectName());
		} else {
			ddl = String.format("CREATE SYNONYM %s FOR %s;", 
			        getFullyQualifiedName(DBPEvaluationContext.DDL), 
					getReferencedObjectName());
		}
		
		return ddl;
	}

	@Override
	public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBException {
        if (CommonUtils.getOption(options, DBPScriptObject.OPTION_REFRESH)) {
            ddl = null;
        }

        if (ddl == null) {
        	if (isPublic) {
        		ddl = getDdlLocal();
        	} else {
        		ddl = ((AltibaseMetaModel)getDataSource().getMetaModel()).getSynonymDDL(monitor, this, options);
        	}
        }
        return ddl;
	}
}
