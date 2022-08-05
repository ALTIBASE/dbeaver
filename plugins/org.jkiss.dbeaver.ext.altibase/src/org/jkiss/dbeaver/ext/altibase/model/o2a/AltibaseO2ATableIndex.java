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
package org.jkiss.dbeaver.ext.altibase.model.o2a;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBPScriptObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.struct.JDBCTableIndex;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.meta.LazyProperty;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObjectLazy;
import org.jkiss.dbeaver.model.struct.rdb.DBSIndexType;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * AltibaseTableIndex
 */
public class AltibaseO2ATableIndex extends JDBCTableIndex<AltibaseO2ASchema, AltibaseO2ATablePhysical> implements DBSObjectLazy, DBPScriptObject
{

    private Object tablespace;
    private boolean nonUnique;
    private List<AltibaseO2ATableIndexColumn> columns;
    private String indexDDL;

    public AltibaseO2ATableIndex(
        AltibaseO2ASchema schema,
        AltibaseO2ATablePhysical table,
        String indexName,
        ResultSet dbResult)
    {
        super(schema, table, indexName, null, true);
        String indexTypeName = JDBCUtils.safeGetString(dbResult, "INDEX_TYPE");
        this.nonUnique = !"UNIQUE".equals(JDBCUtils.safeGetString(dbResult, "UNIQUENESS"));
        if (AltibaseO2AConstants.INDEX_TYPE_NORMAL.getId().equals(indexTypeName)) {
            indexType = AltibaseO2AConstants.INDEX_TYPE_NORMAL;
        } else if (AltibaseO2AConstants.INDEX_TYPE_BITMAP.getId().equals(indexTypeName)) {
            indexType = AltibaseO2AConstants.INDEX_TYPE_BITMAP;
        } else if (AltibaseO2AConstants.INDEX_TYPE_FUNCTION_BASED_NORMAL.getId().equals(indexTypeName)) {
            indexType = AltibaseO2AConstants.INDEX_TYPE_FUNCTION_BASED_NORMAL;
        } else {
            indexType = DBSIndexType.OTHER;
        }
        this.tablespace = JDBCUtils.safeGetString(dbResult, "TABLESPACE_NAME");
    }

    public AltibaseO2ATableIndex(AltibaseO2ASchema schema, AltibaseO2ATablePhysical parent, String name, boolean unique, DBSIndexType indexType)
    {
        super(schema, parent, name, indexType, false);
        this.nonUnique = !unique;

    }

    @NotNull
    @Override
    public AltibaseO2ADataSource getDataSource()
    {
        return getTable().getDataSource();
    }

    @Override
    @Property(viewable = true, order = 5)
    public boolean isUnique()
    {
        return !nonUnique;
    }

    public void setUnique(boolean unique) {
        this.nonUnique = !unique;
    }

    @Override
    public Object getLazyReference(Object propertyId)
    {
        return tablespace;
    }

    @Property(viewable = true, order = 10)
    @LazyProperty(cacheValidator = AltibaseO2ATablespace.TablespaceReferenceValidator.class)
    public Object getTablespace(DBRProgressMonitor monitor) throws DBException
    {
        return AltibaseO2ATablespace.resolveTablespaceReference(monitor, this, null);
    }

    @Nullable
    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public List<AltibaseO2ATableIndexColumn> getAttributeReferences(DBRProgressMonitor monitor)
    {
        return columns;
    }

    @Nullable
    @Association
    public AltibaseO2ATableIndexColumn getColumn(String columnName)
    {
        return DBUtils.findObject(columns, columnName);
    }

    void setColumns(List<AltibaseO2ATableIndexColumn> columns)
    {
        this.columns = columns;
    }

    public void addColumn(AltibaseO2ATableIndexColumn column)
    {
        if (columns == null) {
            columns = new ArrayList<>();
        }
        columns.add(column);
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context)
    {
        return DBUtils.getFullQualifiedName(getDataSource(),
            getTable().getContainer(),
            this);
    }

    @Override
    public String toString()
    {
        return getFullyQualifiedName(DBPEvaluationContext.UI);
    }

    @Override
    @Property(hidden = true, editable = true, updatable = true, order = -1)
    public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBException {
        if (indexDDL == null && isPersisted()) {
            try (JDBCSession session = DBUtils.openMetaSession(monitor, this, "Read index definition")) {
                indexDDL = JDBCUtils.queryString(session,"SELECT DBMS_METADATA.GET_DDL('INDEX', ?, ?) TXT FROM DUAL",
                        getName(),
                        getTable().getSchema().getName());
            } catch (SQLException e) {
                throw new DBException(e, getDataSource());
            }
        }
        return indexDDL;
    }
}
