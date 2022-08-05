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
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.impl.DBObjectNameCaseTransformer;
import org.jkiss.dbeaver.model.impl.jdbc.struct.JDBCTableConstraint;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraintType;

import java.util.ArrayList;
import java.util.List;

/**
 * AltibaseTableConstraint
 */
public abstract class AltibaseO2ATableConstraintBase extends JDBCTableConstraint<AltibaseO2ATableBase> {

    private static final Log log = Log.getLog(AltibaseO2ATableConstraintBase.class);

    private AltibaseO2AObjectStatus status;
    private List<AltibaseO2ATableConstraintColumn> columns;

    public AltibaseO2ATableConstraintBase(AltibaseO2ATableBase oracleTable, String name, DBSEntityConstraintType constraintType, AltibaseO2AObjectStatus status, boolean persisted)
    {
        super(oracleTable, name, null, constraintType, persisted);
        this.status = status;
    }

    protected AltibaseO2ATableConstraintBase(AltibaseO2ATableBase oracleTableBase, String name, String description, DBSEntityConstraintType constraintType, boolean persisted)
    {
        super(oracleTableBase, name, description, constraintType, persisted);
    }

    @NotNull
    @Override
    public AltibaseO2ADataSource getDataSource()
    {
        return getTable().getDataSource();
    }

    @NotNull
    @Property(viewable = true, editable = false, valueTransformer = DBObjectNameCaseTransformer.class, order = 3)
    @Override
    public DBSEntityConstraintType getConstraintType()
    {
        return constraintType;
    }

    @Property(viewable = true, editable = false, order = 9)
    public AltibaseO2AObjectStatus getStatus()
    {
        return status;
    }

    @Override
    public List<AltibaseO2ATableConstraintColumn> getAttributeReferences(DBRProgressMonitor monitor)
    {
        return columns;
    }

    public void addColumn(AltibaseO2ATableConstraintColumn column)
    {
        if (columns == null) {
            columns = new ArrayList<>();
        }
        this.columns.add(column);
    }

    void setColumns(List<AltibaseO2ATableConstraintColumn> columns)
    {
        this.columns = columns;
    }

}
