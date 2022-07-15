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
import org.jkiss.dbeaver.model.impl.DBObjectNameCaseTransformer;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.struct.DBSEntityElement;

import java.sql.ResultSet;

/**
 * Altibase data type member
 */
public abstract class AltibaseDataTypeMember implements DBSEntityElement
{

    private static final Log log = Log.getLog(AltibaseDataTypeMember.class);

    private AltibaseDataType ownerType;
    protected String name;
    protected int number;
    private boolean inherited;
    private boolean persisted;

    protected AltibaseDataTypeMember(AltibaseDataType ownerType)
    {
        this.ownerType = ownerType;
        this.persisted = false;
    }

    protected AltibaseDataTypeMember(AltibaseDataType ownerType, ResultSet dbResult)
    {
        this.ownerType = ownerType;
        this.inherited = JDBCUtils.safeGetBoolean(dbResult, "INHERITED", AltibaseConstants.YES);
        this.persisted = true;
    }

    @NotNull
    public AltibaseDataType getOwnerType()
    {
        return ownerType;
    }

    @Nullable
    @Override
    public String getDescription()
    {
        return null;
    }

    @NotNull
    @Override
    public AltibaseDataType getParentObject()
    {
        return ownerType;
    }

    @NotNull
    @Override
    public AltibaseDataSource getDataSource()
    {
        return ownerType.getDataSource();
    }

    @Override
    public boolean isPersisted()
    {
        return persisted;
    }

    @NotNull
    @Override
    @Property(viewable = true, editable = true, valueTransformer = DBObjectNameCaseTransformer.class, order = 1)
    public String getName()
    {
        return name;
    }

    public int getNumber()
    {
        return number;
    }

    @Property(viewable = true, order = 20)
    public boolean isInherited()
    {
        return inherited;
    }
}
