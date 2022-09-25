package org.jkiss.dbeaver.ext.altibase.model;

import java.util.List;

import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableConstraintColumn;
import org.jkiss.dbeaver.ext.generic.model.GenericUniqueKey;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraintType;

/*
 * AltibaseConstraint types except "0: FOREIGN KEY".
 * 1: NOT NULL, 2: UNIQUE, 3: PRIMARY KEY, 5: TIMESTAMP, 6: LOCAL UNIQUE, 7: CHECK
 * 
 * Refer to SQL: AltibaseMetaModel.prepareUniqueConstraintsLoadStatement
 */
public class AltibaseConstraint extends GenericUniqueKey {
	
	// public DBSEntityConstraintType(String id, String name, String localizedName, boolean association, boolean unique, boolean custom, boolean logical)
	public static final DBSEntityConstraintType LOCAL_UNIQUE_KEY = new DBSEntityConstraintType(
			"localunique", "LOCAL UNIQUE", "LOCAL UNIQUE", false, true, true, false);
	public static final DBSEntityConstraintType TIMESTAMP = new DBSEntityConstraintType(
			"timestamp", "TIMESTAMP", "TIMESTAMP", false, false, true, false);

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
