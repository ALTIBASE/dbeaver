package org.jkiss.dbeaver.ext.altibase.model;

import org.jkiss.dbeaver.ext.generic.model.GenericDataType;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.model.struct.DBSTypedObject;

public class AltibaseDataType extends GenericDataType {

    public AltibaseDataType(GenericStructContainer owner, DBSTypedObject typed) {
        super(owner, typed);
    }
    
	public AltibaseDataType(GenericStructContainer owner, int valueType, String name, String remarks, boolean unsigned,
			boolean searchable, int precision, int minScale, int maxScale) {
		super(owner, valueType, name, remarks, unsigned, searchable, precision, minScale, maxScale);
	}

}
