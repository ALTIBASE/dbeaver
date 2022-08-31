package org.jkiss.dbeaver.ext.altibase.model;

import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObjectWithScript;

public interface AltibaseSourceObject extends DBSObjectWithScript {
	
    void setName(String name);

    AltibaseSourceType getSourceType();

    DBEPersistAction[] getCompileActions(DBRProgressMonitor monitor) throws DBCException;
    
}
