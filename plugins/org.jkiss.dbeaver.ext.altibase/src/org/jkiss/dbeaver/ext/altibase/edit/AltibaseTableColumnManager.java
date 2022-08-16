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
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.altibase.GenericConstants;
import org.jkiss.dbeaver.ext.altibase.model.AltibaseTable;
import org.jkiss.dbeaver.ext.altibase.model.AltibaseTableBase;
import org.jkiss.dbeaver.ext.altibase.model.AltibaseTableColumn;
import org.jkiss.dbeaver.ext.altibase.model.AltibaseUtils;
import org.jkiss.dbeaver.ext.altibase.model.meta.AltibaseMetaModel;
import org.jkiss.dbeaver.model.DBConstants;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.impl.edit.DBECommandAbstract;
import org.jkiss.dbeaver.model.impl.sql.edit.struct.SQLTableColumnManager;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSDataType;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.cache.DBSObjectCache;
import org.jkiss.utils.CommonUtils;

import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * Generic table column manager
 */
public class AltibaseTableColumnManager extends SQLTableColumnManager<AltibaseTableColumn, AltibaseTableBase> {

    @Nullable
    @Override
    public DBSObjectCache<? extends DBSObject, AltibaseTableColumn> getObjectsCache(AltibaseTableColumn object) {
        return object.getParentObject().getContainer().getTableCache().getChildrenCache(object.getParentObject());
    }

    @Override
    public boolean canCreateObject(Object container) {
        return container instanceof AltibaseTable && AltibaseUtils.canAlterTable((AltibaseTable) container);
    }

    @Override
    public boolean canEditObject(AltibaseTableColumn object) {
        return AltibaseUtils.canAlterTable(object);
    }

    @Override
    public boolean canDeleteObject(AltibaseTableColumn object) {
        return AltibaseUtils.canAlterTable(object);
    }

    @Override
    protected AltibaseTableColumn createDatabaseObject(DBRProgressMonitor monitor, DBECommandContext context, Object container, Object copyFrom, Map<String, Object> options) throws DBException {
        AltibaseTableBase tableBase = (AltibaseTableBase) container;
        DBSDataType columnType = findBestDataType(tableBase, DBConstants.DEFAULT_DATATYPE_NAMES);

        int columnSize = columnType != null && columnType.getDataKind() == DBPDataKind.STRING ? 100 : 0;
        AltibaseTableColumn column = tableBase.getDataSource().getMetaModel().createTableColumnImpl(
            monitor,
            null,
            tableBase,
            getNewColumnName(monitor, context, tableBase),
            columnType == null ? "INTEGER" : columnType.getName(),
            columnType == null ? Types.INTEGER : columnType.getTypeID(),
            columnType == null ? Types.INTEGER : columnType.getTypeID(),
            -1,
            columnSize,
            columnSize,
            null,
            null,
            10,
            false,
            null,
            null,
            false,
            false
        );
        column.setPersisted(false);
        return column;
    }

    @Override
    public StringBuilder getNestedDeclaration(DBRProgressMonitor monitor, AltibaseTableBase owner, DBECommandAbstract<AltibaseTableColumn> command, Map<String, Object> options) {
        StringBuilder decl = super.getNestedDeclaration(monitor, owner, command, options);
        addIncrementClauseToNestedDeclaration(command, decl);
        return decl;
    }

    public void addIncrementClauseToNestedDeclaration(DBECommandAbstract<AltibaseTableColumn> command, StringBuilder decl) {
        final AltibaseTableColumn column = command.getObject();
        if (column.isAutoIncrement()) {
            final String autoIncrementClause = column.getDataSource().getMetaModel().getAutoIncrementClause(column);
            if (autoIncrementClause != null && !autoIncrementClause.isEmpty()) {
                decl.append(" ").append(autoIncrementClause); //$NON-NLS-1$
            }
        }
    }

    @Override
    protected ColumnModifier[] getSupportedModifiers(AltibaseTableColumn column, Map<String, Object> options) {
        // According to SQL92 DEFAULT comes before constraints
        AltibaseMetaModel metaModel = column.getDataSource().getMetaModel();
        if (!metaModel.supportsNotNullColumnModifiers(column)) {
            return new ColumnModifier[]{
                DataTypeModifier,
                DefaultModifier
            };
        } else {
            return new ColumnModifier[]{
                DataTypeModifier,
                DefaultModifier,
                metaModel.isColumnNotNullByDefault() ? NullNotNullModifier : NotNullModifier
            };
        }
    }

    @Override
    protected long getDDLFeatures(AltibaseTableColumn object) {
        long features = 0;
        if (CommonUtils.toBoolean(object.getDataSource().getContainer().getDriver().getDriverParameter(GenericConstants.PARAM_DDL_DROP_COLUMN_SHORT))) {
            features |= DDL_FEATURE_OMIT_COLUMN_CLAUSE_IN_DROP;
        }
        if (CommonUtils.toBoolean(object.getDataSource().getContainer().getDriver().getDriverParameter(GenericConstants.PARAM_DDL_DROP_COLUMN_BRACKETS))) {
            features |= DDL_FEATURE_USER_BRACKETS_IN_DROP;
        }
        if (CommonUtils.toBoolean(object.getDataSource().getContainer().getDriver().getDriverParameter(GenericConstants.PARAM_ALTER_TABLE_ADD_COLUMN))) {
            features |= FEATURE_ALTER_TABLE_ADD_COLUMN;
        }
        return features;
    }

    @Override
    protected void addObjectModifyActions(DBRProgressMonitor monitor, DBCExecutionContext executionContext, List<DBEPersistAction> actionList, ObjectChangeCommand command, Map<String, Object> options) throws DBException {
        AltibaseTableColumn column = command.getObject();
        // Add more or less standard COMMENT ON if comment was actually edited (i.e. it is editable at least).
        if (command.hasProperty(DBConstants.PROP_ID_DESCRIPTION)) {
            addColumnCommentAction(actionList, column, column.getTable());
        }
    }

}
