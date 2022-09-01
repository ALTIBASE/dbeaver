package org.jkiss.dbeaver.ext.altibase.model;

import org.jkiss.dbeaver.ext.generic.model.GenericTableBase;
import org.jkiss.dbeaver.ext.generic.model.GenericTableIndex;
import org.jkiss.dbeaver.model.struct.rdb.DBSIndexType;

public class AltibaseTableIndex extends GenericTableIndex {

	public AltibaseTableIndex(GenericTableBase table, boolean nonUnique, String qualifier, long cardinality,
			String indexName, DBSIndexType indexType, boolean persisted) {
		super(table, nonUnique, qualifier, cardinality, indexName, indexType, persisted);
	}
}
