package org.jkiss.dbeaver.ext.altibase.model;

import java.util.List;

import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableConstraintColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericUniqueKey;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraintType;

public class AltibaseConstraint extends GenericUniqueKey {

	protected List<GenericTableConstraintColumn> columns;
	protected String condition;
	protected boolean validated;
	
	public AltibaseConstraint(GenericTableBase table, String name, String remarks,
			DBSEntityConstraintType constraintType, boolean persisted, String condition, boolean validated) {
		super(table, name, remarks, constraintType, persisted);
		this.condition = condition;
		this.validated = validated;
	}

    @Property(viewable = true, order = 10)
    public String getCondition()
    {
        return condition;
    }
    
    @Property(viewable = true, order = 10)
    public boolean isValidated()
    {
        return validated;
    }
}
