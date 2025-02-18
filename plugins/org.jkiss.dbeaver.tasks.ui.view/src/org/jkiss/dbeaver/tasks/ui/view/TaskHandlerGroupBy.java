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
package org.jkiss.dbeaver.tasks.ui.view;

import org.eclipse.core.commands.AbstractHandler;
import org.eclipse.core.commands.ExecutionEvent;
import org.eclipse.core.commands.ExecutionException;
import org.eclipse.core.runtime.Platform;
import org.eclipse.ui.commands.IElementUpdater;
import org.eclipse.ui.handlers.HandlerUtil;
import org.eclipse.ui.menus.UIElement;
import org.jkiss.dbeaver.tasks.ui.internal.TaskUIViewBundle;
import org.jkiss.dbeaver.ui.UIUtils;
import org.jkiss.utils.CommonUtils;

import java.util.Map;

public class TaskHandlerGroupBy extends AbstractHandler implements IElementUpdater {
    public enum GroupBy {
        project,
        category,
        type
    }

    @Override
    public Object execute(ExecutionEvent event) throws ExecutionException {
        GroupBy groupBy = CommonUtils.valueOf(GroupBy.class, event.getParameter("group"), GroupBy.project);
        DatabaseTasksView view = (DatabaseTasksView) HandlerUtil.getActivePart(event);
        DatabaseTasksTree tasksTree = view.getTasksTree();
        switch (groupBy) {
            case project:
                tasksTree.setGroupByProject(!tasksTree.isGroupByProject());
                break;
            case category:
                tasksTree.setGroupByCategory(!tasksTree.isGroupByCategory());
                break;
            case type:
                tasksTree.setGroupByType(!tasksTree.isGroupByType());
                break;
        }
        tasksTree.regroupTasks(DatabaseTasksTree.ExpansionOptions.EXPAND_ALL);
        return null;
    }

    @Override
    public void updateElement(UIElement element, Map parameters) {
        DatabaseTasksView taskView = (DatabaseTasksView) UIUtils.findView(UIUtils.getActiveWorkbenchWindow(), DatabaseTasksView.VIEW_ID);
        if (taskView != null) {
            DatabaseTasksTree tasksTree = taskView.getTasksTree();
            GroupBy groupBy = CommonUtils.valueOf(GroupBy.class, (String)parameters.get("group"), GroupBy.project);

            switch (groupBy) {
                case project:
                    element.setChecked(tasksTree.isGroupByProject());
                    //element.setIcon(DBeaverIcons.getImageDescriptor(DBIcon.PROJECT));
                    break;
                case category:
                    element.setChecked(tasksTree.isGroupByCategory());
                    //element.setIcon(DBeaverIcons.getImageDescriptor(DBIcon.TREE_DATABASE_CATEGORY));
                    break;
                case type:
                    element.setChecked(tasksTree.isGroupByType());
                    //element.setIcon(DBeaverIcons.getImageDescriptor(DBIcon.TREE_TASK));
                    break;
            }
            String commandName = Platform.getResourceBundle(Platform.getBundle(TaskUIViewBundle.BUDLE_ID)).getString("command.org.jkiss.dbeaver.task.group." + groupBy.name() + ".name");
            element.setText(commandName);
            element.setTooltip(commandName);
        }
    }
}
