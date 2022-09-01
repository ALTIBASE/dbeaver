package org.jkiss.dbeaver.ext.altibase.model;

import java.util.ArrayList;
import java.util.List;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.generic.model.GenericCatalog;
import org.jkiss.dbeaver.ext.generic.model.GenericDataSource;
import org.jkiss.dbeaver.ext.generic.model.GenericProcedure;
import org.jkiss.dbeaver.ext.generic.model.GenericSchema;
import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureType;
import org.jkiss.utils.CommonUtils;

public class AltibaseSchema extends GenericSchema {

	public AltibaseSchema(GenericDataSource dataSource, GenericCatalog catalog, String schemaName) {
		super(dataSource, catalog, schemaName);
	}

    @Override
    public List<AltibaseTable> getPhysicalTables(DBRProgressMonitor monitor) throws DBException {
        List<? extends GenericTableBase> tables = getTables(monitor);
        if (tables != null) {
            List<AltibaseTable> filtered = new ArrayList<>();
            for (GenericTableBase table : tables) {
                if (table instanceof AltibaseTable) {
                    filtered.add((AltibaseTable) table);
                }
            }
            return filtered;
        }
        return null;
    }
    
    public List<AltibaseQueue> getQueueTables(DBRProgressMonitor monitor) throws DBException {
        List<? extends GenericTableBase> tables = getTables(monitor);
        if (tables != null) {
            List<AltibaseQueue> filtered = new ArrayList<>();
            for (GenericTableBase table : tables) {
                if (table instanceof AltibaseQueue) {
                    filtered.add((AltibaseQueue) table);
                }
            }
            return filtered;
        }
        return null;
    }
    
    @Override
    public List<AltibaseView> getViews(DBRProgressMonitor monitor) throws DBException {
        List<? extends GenericTableBase> tables = getTables(monitor);
        if (tables != null) {
            List<AltibaseView> filtered = new ArrayList<>();
            for (GenericTableBase table : tables) {
                if (table instanceof AltibaseView) {
                    filtered.add((AltibaseView) table);
                }
            }
            return filtered;
        }
        return null;
    }
    
    public List<AltibaseMaterializedView> getMaterializedViews(DBRProgressMonitor monitor) throws DBException {
        List<? extends GenericTableBase> tables = getTables(monitor);
        if (tables != null) {
            List<AltibaseMaterializedView> filtered = new ArrayList<>();
            for (GenericTableBase table : tables) {
                if (table instanceof AltibaseMaterializedView) {
                    filtered.add((AltibaseMaterializedView) table);
                }
            }
            return filtered;
        }
        return null;
    }
    
    public List<AltibaseTypeset> getTypesetsOnly(DBRProgressMonitor monitor) throws DBException {
        List<AltibaseTypeset> filteredProcedures = new ArrayList<>();
        for (GenericProcedure proc : CommonUtils.safeList(getProcedures(monitor))) {
            if (proc instanceof AltibaseTypeset) {
                filteredProcedures.add((AltibaseTypeset) proc);
            }
        }
        return filteredProcedures;
    }
}
