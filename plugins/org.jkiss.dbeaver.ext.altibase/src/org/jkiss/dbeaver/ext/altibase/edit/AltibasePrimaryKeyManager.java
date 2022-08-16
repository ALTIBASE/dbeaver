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
package org.jkiss.dbeaver.ext.altibase.edit;

import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.ext.altibase.GenericConstants;
import org.jkiss.dbeaver.ext.altibase.model.*;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.impl.sql.edit.struct.SQLConstraintManager;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraintType;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.cache.DBSObjectCache;

import java.util.Map;

/**
 * Generic constraint manager
 */
public class AltibasePrimaryKeyManager extends SQLConstraintManager<AltibaseUniqueKey, AltibaseTableBase> {

    @Nullable
    @Override
    public DBSObjectCache<? extends DBSObject, AltibaseUniqueKey> getObjectsCache(AltibaseUniqueKey object)
    {
        return object.getParentObject().getContainer().getConstraintKeysCache();
    }

    @Override
    public boolean canCreateObject(Object container) {
        return (container instanceof AltibaseTable)
            && (!(((AltibaseTable) container).getDataSource().getInfo() instanceof AltibaseDataSourceInfo) || ((AltibaseDataSourceInfo) ((AltibaseTable) container).getDataSource().getInfo()).supportsTableConstraints())
            && AltibaseUtils.canAlterTable((AltibaseTable) container);
    }

    @Override
    public boolean canEditObject(AltibaseUniqueKey object) {
        return AltibaseUtils.canAlterTable(object);
    }

    @Override
    public boolean canDeleteObject(AltibaseUniqueKey object) {
        return AltibaseUtils.canAlterTable(object);
    }

    @Override
    protected AltibaseUniqueKey createDatabaseObject(
        DBRProgressMonitor monitor, DBECommandContext context, final Object container,
        Object from, Map<String, Object> options)
    {
        AltibaseTableBase tableBase = (AltibaseTableBase)container;
        return tableBase.getDataSource().getMetaModel().createConstraintImpl(
            tableBase,
            GenericConstants.BASE_CONSTRAINT_NAME,
            DBSEntityConstraintType.PRIMARY_KEY,
            null,
            false);
    }

    @Override
    protected boolean isLegacyConstraintsSyntax(AltibaseTableBase owner) {
        return AltibaseUtils.isLegacySQLDialect(owner);
    }
}
