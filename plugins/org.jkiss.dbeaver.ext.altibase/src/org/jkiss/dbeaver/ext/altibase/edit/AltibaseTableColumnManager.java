/*
 * DBeaver - Universal Database Manager
 * Copyright (C) 2010-2022 DBeaver Corp and others
 * Copyright (C) 2011-2012 Eugene Fradkin (eugene.fradkin@gmail.com)
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

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.altibase.model.o2a.AltibaseConstants;
import org.jkiss.dbeaver.ext.altibase.model.o2a.AltibaseDataType;
import org.jkiss.dbeaver.ext.altibase.model.o2a.AltibaseTableBase;
import org.jkiss.dbeaver.ext.altibase.model.o2a.AltibaseTableColumn;
import org.jkiss.dbeaver.model.DBPDataKind;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEObjectRenamer;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.impl.edit.SQLDatabasePersistAction;
import org.jkiss.dbeaver.model.impl.sql.edit.struct.SQLTableColumnManager;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.runtime.VoidProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.dbeaver.model.struct.DBSDataType;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.cache.DBSObjectCache;

import java.sql.Types;
import java.util.List;
import java.util.Map;

/**
 * Altibase table column manager
 */
public class AltibaseTableColumnManager extends SQLTableColumnManager<AltibaseTableColumn, AltibaseTableBase> implements DBEObjectRenamer<AltibaseTableColumn> {

    protected final ColumnModifier<AltibaseTableColumn> AltibaseDataTypeModifier = (monitor, column, sql, command) -> {
        AltibaseDataType dataType = column.getDataType();
        if (dataType != null) {
            String typeName = dataType.getTypeName();
            if (dataType.getDataKind() == DBPDataKind.STRING && column.isPersisted() &&
                (AltibaseConstants.TYPE_INTERVAL_DAY_SECOND.equals(typeName) || AltibaseConstants.TYPE_INTERVAL_YEAR_MONTH.equals(typeName))) {
                // These types have precision inside type name
                Integer precision = column.getPrecision();
                if (AltibaseConstants.TYPE_INTERVAL_YEAR_MONTH.equals(typeName) && precision != null) {
                    if (precision != AltibaseConstants.INTERVAL_DEFAULT_YEAR_DAY_PRECISION) {
                       String patchedName = " INTERVAL YEAR(" + precision + ") TO MONTH";
                       sql.append(patchedName);
                       return;
                    }
                } else {
                    Integer scale = column.getScale(); // fractional seconds precision
                    if (scale != null) {
                        String patchedName = " INTERVAL DAY(" + precision + ") TO SECOND(" + scale + ")";
                        sql.append(patchedName);
                        return;
                    }
                }
            }
        }
        DataTypeModifier.appendModifier(monitor, column, sql, command);
    };

    @Nullable
    @Override
    public DBSObjectCache<? extends DBSObject, AltibaseTableColumn> getObjectsCache(AltibaseTableColumn object)
    {
        return object.getParentObject().getContainer().tableCache.getChildrenCache(object.getParentObject());
    }

    protected ColumnModifier[] getSupportedModifiers(AltibaseTableColumn column, Map<String, Object> options)
    {
        return new ColumnModifier[] {AltibaseDataTypeModifier, DefaultModifier, NullNotNullModifierConditional};
    }

    @Override
    public boolean canEditObject(AltibaseTableColumn object) {
        return true;
    }

    @Override
    protected AltibaseTableColumn createDatabaseObject(DBRProgressMonitor monitor, DBECommandContext context, Object container, Object copyFrom, Map<String, Object> options) throws DBException
    {
        AltibaseTableBase table = (AltibaseTableBase) container;

        DBSDataType columnType = findBestDataType(table, "varchar2"); //$NON-NLS-1$

        final AltibaseTableColumn column = new AltibaseTableColumn(table);
        column.setName(getNewColumnName(monitor, context, table));
        column.setDataType((AltibaseDataType) columnType);
        column.setTypeName(columnType == null ? "INTEGER" : columnType.getName()); //$NON-NLS-1$
        column.setMaxLength(columnType != null && columnType.getDataKind() == DBPDataKind.STRING ? 100 : 0);
        column.setValueType(columnType == null ? Types.INTEGER : columnType.getTypeID());
        column.setOrdinalPosition(-1);
        return column;
    }

    @Override
    protected void addObjectCreateActions(DBRProgressMonitor monitor, DBCExecutionContext executionContext, List<DBEPersistAction> actions, ObjectCreateCommand command, Map<String, Object> options) {
        super.addObjectCreateActions(monitor, executionContext, actions, command, options);
        if (command.getProperty("comment") != null) {
            addColumnCommentAction(actions, command.getObject(), command.getObject().getParentObject());
        }
    }

    @Override
    protected void addObjectModifyActions(DBRProgressMonitor monitor, DBCExecutionContext executionContext, List<DBEPersistAction> actionList, ObjectChangeCommand command, Map<String, Object> options)
    {
        final AltibaseTableColumn column = command.getObject();
        boolean hasComment = command.getProperty("comment") != null;
        if (!hasComment || command.getProperties().size() > 1) {
            actionList.add(new SQLDatabasePersistAction(
                "Modify column",
                "ALTER TABLE " + column.getTable().getFullyQualifiedName(DBPEvaluationContext.DDL) + //$NON-NLS-1$
                " MODIFY " + getNestedDeclaration(monitor, column.getTable(), command, options))); //$NON-NLS-1$
        }
        if (hasComment) {
            addColumnCommentAction(actionList, column, column.getTable());
        }
    }

    @Override
    public void renameObject(@NotNull DBECommandContext commandContext, @NotNull AltibaseTableColumn object, @NotNull Map<String, Object> options, @NotNull String newName) throws DBException {
        processObjectRename(commandContext, object, options, newName);
    }

    @Override
    protected void addObjectRenameActions(DBRProgressMonitor monitor, DBCExecutionContext executionContext, List<DBEPersistAction> actions, ObjectRenameCommand command, Map<String, Object> options)
    {
        final AltibaseTableColumn column = command.getObject();

        actions.add(
            new SQLDatabasePersistAction(
                "Rename column",
                "ALTER TABLE " + column.getTable().getFullyQualifiedName(DBPEvaluationContext.DDL) + " RENAME COLUMN " +
                    DBUtils.getQuotedIdentifier(column.getDataSource(), command.getOldName()) + " TO " +
                    DBUtils.getQuotedIdentifier(column.getDataSource(), command.getNewName()))
        );
    }

}
