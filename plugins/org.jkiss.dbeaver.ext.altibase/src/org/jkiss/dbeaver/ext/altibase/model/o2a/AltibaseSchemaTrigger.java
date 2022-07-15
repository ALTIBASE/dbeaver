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

import org.jkiss.dbeaver.model.struct.rdb.DBSTable;

import java.sql.ResultSet;

/**
 * AltibaseSchemaTrigger
 */
public class AltibaseSchemaTrigger extends AltibaseTrigger<AltibaseSchema>
{
    public AltibaseSchemaTrigger(AltibaseSchema schema, String name)
    {
        super(schema, name);
    }

    public AltibaseSchemaTrigger(
        AltibaseSchema schema,
        ResultSet dbResult)
    {
        super(schema, dbResult);
    }


    @Override
    public DBSTable getTable() {
        return null;
    }

    @Override
    public AltibaseSchema getSchema() {
        return parent;
    }

}
