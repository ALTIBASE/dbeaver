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
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.meta.Property;

import java.sql.ResultSet;
import java.util.Collection;
import java.util.List;

/**
 * AltibaseTableTrigger
 */
public class AltibaseO2ATableTrigger extends AltibaseO2ATrigger<AltibaseO2ATableBase>
{
    private static final Log log = Log.getLog(AltibaseO2ATableTrigger.class);

    private AltibaseO2ASchema ownerSchema;
    private List<AltibaseO2ATriggerColumn> columns;

    public AltibaseO2ATableTrigger(AltibaseO2ATableBase table, String name)
    {
        super(table, name);
        ownerSchema = table.getSchema();
    }

    public AltibaseO2ATableTrigger(
        AltibaseO2ATableBase table,
        ResultSet dbResult)
    {
        super(table, dbResult);
        String ownerName = JDBCUtils.safeGetStringTrimmed(dbResult, "OWNER");
        if (ownerName != null) {
            this.ownerSchema = table.getDataSource().schemaCache.getCachedObject(ownerName);
            if (this.ownerSchema == null) {
                log.warn("Trigger owner schema '" + ownerName + "' not found");
            }
        }
        if (this.ownerSchema == null) {
            this.ownerSchema = table.getSchema();
        }
    }

    @Override
    @Property(viewable = true, order = 4)
    public AltibaseO2ATableBase getTable()
    {
        return parent;
    }

    @Override
    public AltibaseO2ASchema getSchema() {
        return this.ownerSchema;
    }

    @Association
    @Nullable
    public Collection<AltibaseO2ATriggerColumn> getColumns() {
        return columns;
    }

    public void setColumns(@NotNull List<AltibaseO2ATriggerColumn> columns) {
        this.columns = columns;
    }
}
