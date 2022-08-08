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
import org.jkiss.dbeaver.ext.altibase.model.o2a.AltibaseO2AConstants;
import org.jkiss.dbeaver.ext.altibase.model.o2a.AltibaseO2ADataType;
import org.jkiss.dbeaver.ext.altibase.model.o2a.AltibaseO2ATableBase;
import org.jkiss.dbeaver.ext.altibase.model.o2a.AltibaseO2ATableColumn;
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
public class AltibaseTableColumnManager extends SQLTableColumnManager<AltibaseO2ATableColumn, AltibaseO2ATableBase> implements DBEObjectRenamer<AltibaseO2ATableColumn> {

    protected final ColumnModifier<AltibaseO2ATableColumn> AltibaseDataTypeModifier = (monitor, column, sql, command) -> {
        AltibaseO2ADataType dataType = column.getDataType();
        if (dataType != null) {
            String typeName = dataType.getTypeName();
            if (dataType.getDataKind() == DBPDataKind.STRING && column.isPersisted() &&
                (AltibaseO2AConstants.TYPE_INTERVAL_DAY_SECOND.equals(typeName) || AltibaseO2AConstants.TYPE_INTERVAL_YEAR_MONTH.equals(typeName))) {
                // These types have precision inside type name
                Integer precision = column.getPrecision();
                if (AltibaseO2AConstants.TYPE_INTERVAL_YEAR_MONTH.equals(typeName) && precision != null) {
                    if (precision != AltibaseO2AConstants.INTERVAL_DEFAULT_YEAR_DAY_PRECISION) {
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
    public DBSObjectCache<? extends DBSObject, AltibaseO2ATableColumn> getObjectsCache(AltibaseO2ATableColumn object)
    {
        return object.getParentObject().getContainer().tableCache.getChildrenCache(object.getParentObject());
    }

    protected ColumnModifier[] getSupportedModifiers(AltibaseO2ATableColumn column, Map<String, Object> options)
    {
        return new ColumnModifier[] {AltibaseDataTypeModifier, DefaultModifier, NullNotNullModifierConditional};
    }

    @Override
    public boolean canEditObject(AltibaseO2ATableColumn object) {
        return true;
    }

    @Override
    protected AltibaseO2ATableColumn createDatabaseObject(DBRProgressMonitor monitor, DBECommandContext context, Object container, Object copyFrom, Map<String, Object> options) throws DBException
    {
        AltibaseO2ATableBase table = (AltibaseO2ATableBase) container;

        DBSDataType columnType = findBestDataType(table, "varchar2"); //$NON-NLS-1$

        final AltibaseO2ATableColumn column = new AltibaseO2ATableColumn(table);
        column.setName(getNewColumnName(monitor, context, table));
        column.setDataType((AltibaseO2ADataType) columnType);
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
        final AltibaseO2ATableColumn column = command.getObject();
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
    public void renameObject(@NotNull DBECommandContext commandContext, @NotNull AltibaseO2ATableColumn object, @NotNull Map<String, Object> options, @NotNull String newName) throws DBException {
        processObjectRename(commandContext, object, options, newName);
    }

    @Override
    protected void addObjectRenameActions(DBRProgressMonitor monitor, DBCExecutionContext executionContext, List<DBEPersistAction> actions, ObjectRenameCommand command, Map<String, Object> options)
    {
        final AltibaseO2ATableColumn column = command.getObject();

        actions.add(
            new SQLDatabasePersistAction(
                "Rename column",
                "ALTER TABLE " + column.getTable().getFullyQualifiedName(DBPEvaluationContext.DDL) + " RENAME COLUMN " +
                    DBUtils.getQuotedIdentifier(column.getDataSource(), command.getOldName()) + " TO " +
                    DBUtils.getQuotedIdentifier(column.getDataSource(), command.getNewName()))
        );
    }

}
