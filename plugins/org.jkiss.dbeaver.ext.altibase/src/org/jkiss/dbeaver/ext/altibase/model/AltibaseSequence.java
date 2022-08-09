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
package org.jkiss.dbeaver.ext.altibase.model;

import java.math.BigDecimal;
import java.util.Map;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.model.DBPEvaluationContext;
import org.jkiss.dbeaver.model.DBPNamedObject2;
import org.jkiss.dbeaver.model.DBPQualifiedObject;
import org.jkiss.dbeaver.model.DBPScriptObject;
import org.jkiss.dbeaver.model.DBUtils;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.meta.PropertyLength;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.rdb.DBSSequence;

/**
 * AltibaseSequence
 */
public class AltibaseSequence implements DBSSequence, DBPScriptObject, DBPQualifiedObject, DBPNamedObject2
{
    private AltibaseStructContainer container;
    private String name;
    private String description;
    private BigDecimal lastValue;
    private BigDecimal minValue;
    private BigDecimal maxValue;
    private BigDecimal incrementBy;
    
    private BigDecimal cacheSize;
    private BigDecimal startWith;
    private boolean flagCycle;
    
    private String source;

    public AltibaseSequence(AltibaseStructContainer container, JDBCResultSet dbResult) {
        this.container = container;
        
        this.name = JDBCUtils.safeGetString(dbResult, "TABLE_NAME");
        this.description = "";
        this.lastValue 		= JDBCUtils.safeGetBigDecimal(dbResult, "CURRENT_SEQ");
        this.startWith 		= JDBCUtils.safeGetBigDecimal(dbResult, "START_SEQ");
        this.minValue 		= JDBCUtils.safeGetBigDecimal(dbResult, "MIN_SEQ");
        this.maxValue 		= JDBCUtils.safeGetBigDecimal(dbResult, "MAX_SEQ");
        this.incrementBy 	= JDBCUtils.safeGetBigDecimal(dbResult, "INCREMENT_SEQ");
        this.cacheSize 		= JDBCUtils.safeGetBigDecimal(dbResult, "CACHE_SIZE");
        this.flagCycle 		= JDBCUtils.safeGetBoolean(dbResult, "IS_CYCLE", "YES");
    }

    @NotNull
    @Override
    @Property(viewable = true, order = 1)
    public String getName() {
        return name;
    }

    @Override
    public void setName(String name) {
        this.name = name;
    }

    @Override
    public boolean isPersisted() {
        return true;
    }

    @Nullable
    @Override
    @Property(viewable = true, length = PropertyLength.MULTILINE, order = 10)
    public String getDescription() {
        return description;
    }

    @Nullable
    @Override
    public DBSObject getParentObject() {
        return container;
    }

    @NotNull
    @Override
    public AltibaseDataSource getDataSource() {
        return container.getDataSource();
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context) {
        return DBUtils.getFullQualifiedName(getDataSource(),
            container.getCatalog(),
            container.getSchema(),
            this);
    }

    @Override
    @Property(viewable = true, order = 2)
    public BigDecimal getLastValue() {
        return lastValue;
    }

    public void setLastValue(BigDecimal lastValue) {
        this.lastValue = lastValue;
    }
    
    @Property(viewable = true, order = 3)
    public BigDecimal getStartWith() {
        return startWith;
    }
    
    @Override
    @Property(viewable = true, order = 4)
    public BigDecimal getIncrementBy() {
        return incrementBy;
    }

    public void setIncrementBy(BigDecimal incrementBy) {
        this.incrementBy = incrementBy;
    }
    
    @Override
    @Property(viewable = true, order = 5)
    public BigDecimal getMinValue() {
        return minValue;
    }

    public void setMinValue(BigDecimal minValue) {
        this.minValue = minValue;
    }

    @Override
    @Property(viewable = true, order = 6)
    public BigDecimal getMaxValue() {
        return maxValue;
    }

    public void setMaxValue(BigDecimal maxValue) {
        this.maxValue = maxValue;
    }
    
    @Property(viewable = true, order = 7)
    public BigDecimal getCacheSize() {
        return cacheSize;
    }
    
    @Property(viewable = true, order = 8)
    public boolean getCycle() {
        return flagCycle;
    }
    
    public boolean isCycle() { return flagCycle; }

    public AltibaseSchema getSchema() { return container.getSchema(); }
    
    public String buildStatement(boolean forUpdate) {
        StringBuilder sb = new StringBuilder();
        if (forUpdate) {
            sb.append("ALTER SEQUENCE ");
        } else {
            sb.append("CREATE SEQUENCE ");
        }
        sb.append(getFullyQualifiedName(DBPEvaluationContext.DDL)).append(" ");

        if (getStartWith() != null) {
            sb.append("START WITH ").append(getStartWith()).append(" ");
        }
        
        if (getIncrementBy() != null) {
            sb.append("INCREMENT BY ").append(getIncrementBy()).append(" ");
        }
        if (getMinValue() != null) {
            sb.append("MINVALUE ").append(getMinValue()).append(" ");
        }
        if (getMaxValue() != null) {
            sb.append("MAXVALUE ").append(getMaxValue()).append(" ");
        }

        if (getCacheSize().compareTo(BigDecimal.ZERO) > 0) {
            sb.append("CACHE ").append(getCacheSize()).append(" ");
        }

        if (isCycle()) {
            sb.append("CYCLE ");
        }

        return sb.toString();
    }
    
    /*
     * Unable to use DBMS_METADATA for sequence because it returns 'START_WITH' value as CURRENT_SEQ+INCREMENT_SEQ for schema migration
     */
	@Override
	public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBException {
        if (source == null) {
        	source = buildStatement(false);
        }
        return source;
	}
}
