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
import org.jkiss.dbeaver.ext.altibase.model.AltibaseDataSource;
import org.jkiss.dbeaver.ext.altibase.model.AltibaseStructContainer;
import org.jkiss.dbeaver.ext.altibase.model.AltibaseTableBase;
import org.jkiss.dbeaver.ext.altibase.model.AltibaseView;
import org.jkiss.dbeaver.model.DBConstants;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.edit.DBECommandContext;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.edit.prop.DBECommandComposite;
import org.jkiss.dbeaver.model.exec.DBCExecutionContext;
import org.jkiss.dbeaver.model.impl.edit.SQLDatabasePersistAction;
import org.jkiss.dbeaver.model.impl.sql.edit.SQLObjectEditor;
import org.jkiss.dbeaver.model.impl.sql.edit.struct.SQLTableManager;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.cache.DBSObjectCache;
import org.jkiss.utils.CommonUtils;

import java.util.List;
import java.util.Map;

/**
 * Generic view manager
 */
public class AltibaseViewManager extends SQLObjectEditor<AltibaseTableBase, AltibaseStructContainer> {

    @Override
    public long getMakerOptions(DBPDataSource dataSource)
    {
        return FEATURE_EDITOR_ON_CREATE;
    }

    @Override
    protected void validateObjectProperties(DBRProgressMonitor monitor, ObjectChangeCommand command, Map<String, Object> options)
            throws DBException
    {
        if (CommonUtils.isEmpty(command.getObject().getName())) {
            throw new DBException("View name cannot be empty");
        }
        if (CommonUtils.isEmpty(command.getObject().getObjectDefinitionText(monitor, options))) {
            throw new DBException("View definition cannot be empty");
        }
    }

    @Nullable
    @Override
    public DBSObjectCache<? extends DBSObject, AltibaseTableBase> getObjectsCache(AltibaseTableBase object)
    {
        return object.getContainer().getTableCache();
    }

    @Override
    public boolean canCreateObject(Object container) {
        if (container instanceof DBSObject) {
            DBPDataSource dataSource = ((DBSObject) container).getDataSource();
            if (dataSource instanceof AltibaseDataSource) {
                return ((AltibaseDataSource) dataSource).getMetaModel().supportsViews((AltibaseDataSource) dataSource);
            }
        }
        return super.canCreateObject(container);
    }

    @Override
    protected AltibaseTableBase createDatabaseObject(DBRProgressMonitor monitor, DBECommandContext context, Object container, Object copyFrom, Map<String, Object> options)
    {
        AltibaseStructContainer structContainer = (AltibaseStructContainer) container;
        String tableName = getNewChildName(monitor, structContainer, SQLTableManager.BASE_VIEW_NAME);
        AltibaseTableBase viewImpl = structContainer.getDataSource().getMetaModel().createTableImpl(structContainer, tableName,
                GenericConstants.TABLE_TYPE_VIEW,
                null);
        if (viewImpl instanceof AltibaseView) {
            ((AltibaseView) viewImpl).setObjectDefinitionText("CREATE VIEW " + viewImpl.getFullyQualifiedName(DBPEvaluationContext.DDL) + " AS SELECT 1 as A\n");
        }
        return viewImpl;
    }

    @Override
    protected void addObjectCreateActions(DBRProgressMonitor monitor, DBCExecutionContext executionContext, List<DBEPersistAction> actions, ObjectCreateCommand command, Map<String, Object> options)
    {
        createOrReplaceViewQuery(actions, command);
    }

    @Override
    protected void addObjectDeleteActions(DBRProgressMonitor monitor, DBCExecutionContext executionContext, List<DBEPersistAction> actions, ObjectDeleteCommand command, Map<String, Object> options)
    {
        actions.add(
            new SQLDatabasePersistAction(
                "Drop view",
                "DROP " + getDropViewType(command.getObject()) + " " +
                command.getObject().getFullyQualifiedName(DBPEvaluationContext.DDL)) //$NON-NLS-2$
        );
    }

    protected String getDropViewType(AltibaseTableBase object) {
        return "VIEW";
    }

    private void createOrReplaceViewQuery(List<DBEPersistAction> actions, DBECommandComposite<AltibaseTableBase, PropertyHandler> command)
    {
        final AltibaseView view = (AltibaseView)command.getObject();
        actions.add(new SQLDatabasePersistAction("Create view", view.getDDL()));
    }

    @Override
    protected void addObjectModifyActions(DBRProgressMonitor monitor, DBCExecutionContext executionContext, List<DBEPersistAction> actionList, ObjectChangeCommand command, Map<String, Object> options)
    {
        final AltibaseView view = (AltibaseView)command.getObject();
        boolean hasComment = command.hasProperty(DBConstants.PROP_ID_DESCRIPTION);
        if (!hasComment || command.getProperties().size() > 1) {
            actionList.add(new SQLDatabasePersistAction("Create view", view.getDDL()));
        }
        if (hasComment) {
            actionList.add(new SQLDatabasePersistAction(
                    "Comment view",
                    "COMMENT ON VIEW " + command.getObject().getFullyQualifiedName(DBPEvaluationContext.DDL) +
                            " IS " + SQLUtils.quoteString(command.getObject(), CommonUtils.notEmpty(command.getObject().getDescription()))));
        }
    }

}
