/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2022 DBeaver Corp and others
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.jkiss.dbeaver.ext.altibase.model;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.altibase.AltibaseConstants;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTable;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.model.DBPNamedObject2;
import org.jkiss.dbeaver.model.data.DBDPseudoAttribute;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.utils.CommonUtils;

public class AltibaseTable extends GenericTable implements AltibaseTableBase, DBPNamedObject2 {
	
	private static final Log log = Log.getLog(AltibaseTable.class);
	
    public AltibaseTable(GenericStructContainer container, String tableName, String tableType, JDBCResultSet dbResult) {
		super(container, tableName, tableType, dbResult);
	}
    
    @Override
    protected boolean isTruncateSupported() {
        return true;
    }
    
    /*
     * In order to use a conversion function for geometry type.
     */
    @Override
    protected void appendSelectSource(DBRProgressMonitor monitor, StringBuilder query, String tableAlias, DBDPseudoAttribute rowIdAttribute) {
    	try {

    		int i = 0;
    		for (GenericTableColumn col : CommonUtils.safeCollection(getAttributes(monitor))) {
    			if (i++ > 0) {
    				query.append(",");
    			}

    			if (col.getTypeName().equalsIgnoreCase(AltibaseConstants.TYPE_NAME_GEOMETRY)) {
    				//query.append("ASBINARY(").append((tableAlias != null?tableAlias + ".":"")).append(geoColumn.getName()).append(") as ").append(geoColumn.getName());
    				query.append("ASEWKT(").append((tableAlias != null?tableAlias + ".":"")).append(col.getName()).append(", 32000) as ").append(col.getName());
    			} else{
    				query.append((tableAlias != null?tableAlias + ".":"")).append(col.getName()).append(" as ").append(col.getName());
    			}
    		}
    		return;

    	} catch (DBException e) {
    		log.warn(e);
    	}
    }
}
