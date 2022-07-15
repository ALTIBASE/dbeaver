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
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.struct.DBSEntityConstraintType;
import org.jkiss.utils.CommonUtils;

import java.sql.ResultSet;

/**
 * AltibaseTableConstraint
 */
public class AltibaseTableConstraint extends AltibaseTableConstraintBase {

    private static final Log log = Log.getLog(AltibaseTableConstraint.class);

    private String searchCondition;

    public AltibaseTableConstraint(AltibaseTableBase oracleTable, String name, DBSEntityConstraintType constraintType, String searchCondition, AltibaseObjectStatus status)
    {
        super(oracleTable, name, constraintType, status, false);
        this.searchCondition = searchCondition;
    }

    public AltibaseTableConstraint(AltibaseTableBase table, ResultSet dbResult)
    {
        super(
            table,
            JDBCUtils.safeGetString(dbResult, "CONSTRAINT_NAME"),
            getConstraintType(JDBCUtils.safeGetString(dbResult, "CONSTRAINT_TYPE")),
            CommonUtils.notNull(
                CommonUtils.valueOf(AltibaseObjectStatus.class, JDBCUtils.safeGetStringTrimmed(dbResult, "STATUS")),
                AltibaseObjectStatus.ENABLED),
            true);
        this.searchCondition = JDBCUtils.safeGetString(dbResult, "SEARCH_CONDITION");
    }

    @Property(viewable = true, editable = true, order = 4)
    public String getSearchCondition()
    {
        return searchCondition;
    }

    public void setSearchCondition(String searchCondition) {
        this.searchCondition = searchCondition;
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context)
    {
        return DBUtils.getFullQualifiedName(getDataSource(),
            getTable().getContainer(),
            getTable(),
            this);
    }

    public static DBSEntityConstraintType getConstraintType(String code)
    {
        switch (code) {
            case "C":
                return DBSEntityConstraintType.CHECK;
            case "P":
                return DBSEntityConstraintType.PRIMARY_KEY;
            case "U":
                return DBSEntityConstraintType.UNIQUE_KEY;
            case "R":
                return DBSEntityConstraintType.FOREIGN_KEY;
            default:
                log.debug("Unsupported Altibase constraint type: " + code);
                return DBSEntityConstraintType.CHECK;
        }
    }
}
