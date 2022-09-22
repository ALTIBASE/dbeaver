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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.altibase.AltibaseConstants;
import org.jkiss.dbeaver.ext.generic.model.GenericStructContainer;
import org.jkiss.dbeaver.ext.generic.model.GenericTable;
import org.jkiss.dbeaver.ext.generic.model.GenericTableColumn;
import org.jkiss.dbeaver.model.DBPAttributeReferencePurpose;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBPNamedObject2;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.data.DBDAttributeBindingMeta;
import org.jkiss.dbeaver.model.data.DBDDataReceiver;
import org.jkiss.dbeaver.model.data.DBDPseudoAttribute;
import org.jkiss.dbeaver.model.data.DBDValueBinder;
import org.jkiss.dbeaver.model.data.DBDValueHandler;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCExecutionSource;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.DBCStatement;
import org.jkiss.dbeaver.model.exec.DBCStatementType;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.data.ExecuteBatchImpl;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCSQLDialect;
import org.jkiss.dbeaver.model.impl.jdbc.struct.JDBCTable;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLDialect;
import org.jkiss.dbeaver.model.struct.DBSAttributeBase;
import org.jkiss.dbeaver.model.struct.DBSDataManipulator.ExecuteBatch;
import org.jkiss.utils.ArrayUtils;
import org.jkiss.utils.CommonUtils;
import org.locationtech.jts.geom.Geometry;

public class AltibaseTable extends GenericTable implements AltibaseTableBase, DBPNamedObject2 {
	
	private static final Log log = Log.getLog(AltibaseTable.class);
	
    public AltibaseTable(GenericStructContainer container, String tableName, String tableType, JDBCResultSet dbResult) {
		super(container, tableName, tableType, dbResult);
	}
    
    @Override
    protected boolean isTruncateSupported() {
        return true;
    }
    
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
    
    /*
    ////////////////////////////////////////////////////////////////////
    // Update: Work-around for Geometry
    // Unable to use 
    private static final String DEFAULT_TABLE_ALIAS = "x";
    
    private boolean useUpsert(@NotNull DBCSession session) {
        SQLDialect dialect = session.getDataSource().getSQLDialect();
        return dialect instanceof JDBCSQLDialect && ((JDBCSQLDialect) dialect).supportsUpsertStatement();
    }
    
    private void readRequiredMeta(DBRProgressMonitor monitor)
            throws DBCException
        {
            try {
                getAttributes(monitor);
            }
            catch (DBException e) {
                throw new DBCException("Can't cache table columns", e);
            }
        }
    
    @NotNull
    @Override
    public ExecuteBatch updateData(
        @NotNull DBCSession session,
        @NotNull final DBSAttributeBase[] updateAttributes,
        @NotNull final DBSAttributeBase[] keyAttributes,
        @Nullable DBDDataReceiver keysReceiver, @NotNull final DBCExecutionSource source)
        throws DBCException
    {
        if (useUpsert(session)) {
            return insertData(
                session,
                ArrayUtils.concatArrays(updateAttributes, keyAttributes),
                keysReceiver,
                source,
                Collections.emptyMap());
        }
        readRequiredMeta(session.getProgressMonitor());

        DBSAttributeBase[] attributes = ArrayUtils.concatArrays(updateAttributes, keyAttributes);

        return new ExecuteBatchImpl(attributes, keysReceiver, false) {
            @NotNull
            @Override
            protected DBCStatement prepareStatement(@NotNull DBCSession session, DBDValueHandler[] handlers, Object[] attributeValues, Map<String, Object> options) throws DBCException {
                String tableAlias = null;
                SQLDialect dialect = session.getDataSource().getSQLDialect();
                if (dialect.supportsAliasInUpdate()) {
                    tableAlias = DEFAULT_TABLE_ALIAS;
                }
                // Make query
                StringBuilder query = new StringBuilder();
                String tableName = DBUtils.getEntityScriptName(AltibaseTable.this, options);
                query.append(generateTableUpdateBegin(tableName));
                if (tableAlias != null) {
                    query.append(' ').append(tableAlias);
                }
                String updateSet = generateTableUpdateSet();
                if (!CommonUtils.isEmpty(updateSet)) {
                    query.append("\n\t").append(updateSet); //$NON-NLS-1$ //$NON-NLS-2$
                }

                boolean hasKey = false;
                for (int i = 0; i < updateAttributes.length; i++) {
                    DBSAttributeBase attribute = updateAttributes[i];
                    if (hasKey) query.append(","); //$NON-NLS-1$
                    hasKey = true;
                    if (tableAlias != null) {
                        query.append(tableAlias).append(dialect.getStructSeparator());
                    }
                    query.append(getAttributeName(attribute, DBPAttributeReferencePurpose.UPDATE_TARGET)).append("="); //$NON-NLS-1$
                    DBDValueHandler valueHandler = handlers[i];
                    if (valueHandler instanceof DBDValueBinder) {
                    	//query.append("GEOMFROMTEXT(?), " + ((Geometry) attribute).getSRID() + ")");
                    	query.append(((DBDValueBinder) valueHandler).makeQueryBind(attribute, attributeValues[i]));
                    } else {
                        query.append("?"); //$NON-NLS-1$
                    }
                }
                if (keyAttributes.length > 0) {
                    query.append("\n\tWHERE "); //$NON-NLS-1$
                    hasKey = false;
                    for (int i = 0; i < keyAttributes.length; i++) {
                        DBSAttributeBase attribute = keyAttributes[i];
                        if (hasKey) query.append(" AND "); //$NON-NLS-1$
                        hasKey = true;
                        appendAttributeCriteria(tableAlias, dialect, query, attribute, attributeValues[updateAttributes.length + i]);
                    }
                }

                // Execute
                DBCStatement dbStat = session.prepareStatement(DBCStatementType.QUERY, query.toString(), false, false, keysReceiver != null);

                dbStat.setStatementSource(source);
                return dbStat;
            }

            @Override
            protected void bindStatement(@NotNull DBDValueHandler[] handlers, @NotNull DBCStatement statement, Object[] attributeValues) throws DBCException {
                int paramIndex = 0;
                for (int k = 0; k < handlers.length; k++) {
                    DBSAttributeBase attribute = attributes[k];
                    if (k >= updateAttributes.length && DBUtils.isNullValue(attributeValues[k])) {
                        // Skip NULL criteria binding
                        continue;
                    }
                    handlers[k].bindValueObject(statement.getSession(), statement, attribute, paramIndex++, attributeValues[k]);
                }
            }
        };
    }
    
    private void appendAttributeCriteria(@Nullable String tableAlias, SQLDialect dialect, StringBuilder query, DBSAttributeBase attribute, Object value) {
        DBDPseudoAttribute pseudoAttribute = null;
        if (DBUtils.isPseudoAttribute(attribute)) {
            if (attribute instanceof DBDAttributeBindingMeta) {
                pseudoAttribute = ((DBDAttributeBindingMeta) attribute).getPseudoAttribute();
            } else {
                log.error("Unsupported attribute argument: " + attribute);
            }
        }
        if (pseudoAttribute != null) {
            if (tableAlias == null) {
                tableAlias = this.getFullyQualifiedName(DBPEvaluationContext.DML);
            }
            String criteria = pseudoAttribute.translateExpression(tableAlias);
            query.append(criteria);
        } else {
            if (tableAlias != null) {
                query.append(tableAlias).append(dialect.getStructSeparator());
            }
            query.append(dialect.getCastedAttributeName(attribute, getAttributeName(attribute)));
        }
        if (DBUtils.isNullValue(value)) {
            query.append(" IS NULL"); //$NON-NLS-1$
        } else {
            query.append("=").append(dialect.getTypeCastClause(attribute, "?", true)); //$NON-NLS-1$
        }
    }
    */
}
