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
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.altibase.model.o2a.AltibaseO2AConstants;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

public class AltibaseO2ADependencyGroup implements DBSObject {
    private final DBSObject owner;
    private final boolean dependents;

    public AltibaseO2ADependencyGroup(DBSObject owner, boolean dependents) {
        this.owner = owner;
        this.dependents = dependents;
    }

    @NotNull
    public static Collection<AltibaseO2ADependencyGroup> of(@NotNull DBSObject owner) {
        return Collections.unmodifiableCollection(Arrays.asList(
            new AltibaseO2ADependencyGroup(owner, false),
            new AltibaseO2ADependencyGroup(owner, true)
        ));
    }

    @Association
    public Collection<AltibaseO2ADependency> getEntries(DBRProgressMonitor monitor) throws DBException {
        return AltibaseO2ADependency.readDependencies(monitor, owner, dependents);
    }

    @NotNull
    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return dependents
            ? AltibaseO2AConstants.EDIT_ALTIBASE_DEPENDENCIES_DEPENDENT_NAME
            : AltibaseO2AConstants.EDIT_ALTIBASE_DEPENDENCIES_DEPENDENCY_NAME;
    }

    @Nullable
    @Override
    public String getDescription() {
        return dependents
            ? AltibaseO2AConstants.EDIT_ALTIBASE_DEPENDENCIES_DEPENDENT_DESCRIPTION
            : AltibaseO2AConstants.EDIT_ALTIBASE_DEPENDENCIES_DEPENDENCY_DESCRIPTION;
    }

    @Override
    public boolean isPersisted() {
        return owner.isPersisted();
    }

    @Nullable
    @Override
    public DBSObject getParentObject() {
        return owner;
    }

    @NotNull
    @Override
    public DBPDataSource getDataSource() {
        return owner.getDataSource();
    }
}
