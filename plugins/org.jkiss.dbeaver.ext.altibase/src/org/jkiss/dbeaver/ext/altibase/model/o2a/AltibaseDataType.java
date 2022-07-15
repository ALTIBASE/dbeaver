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
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.altibase.model.source.AltibaseSourceObject;
import org.jkiss.dbeaver.model.*;
import org.jkiss.dbeaver.model.edit.DBEPersistAction;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCLogicalOperator;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCStatement;
import org.jkiss.dbeaver.model.impl.DBObjectNameCaseTransformer;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.impl.jdbc.cache.JDBCObjectCache;
import org.jkiss.dbeaver.model.meta.Association;
import org.jkiss.dbeaver.model.meta.Property;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.*;
import org.jkiss.utils.CommonUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Altibase data type
 */
public class AltibaseDataType extends AltibaseObject<DBSObject>
    implements DBSDataType, DBSEntity, DBPQualifiedObject, AltibaseSourceObject, DBPScriptObjectExt {

    private static final Log log = Log.getLog(AltibaseDataType.class);

    static class TypeDesc {
        final DBPDataKind dataKind;
        final int valueType;
        final int precision;
        final int minScale;
        final int maxScale;
        private TypeDesc(DBPDataKind dataKind, int valueType, int precision, int minScale, int maxScale)
        {
            this.dataKind = dataKind;
            this.valueType = valueType;
            this.precision = precision;
            this.minScale = minScale;
            this.maxScale = maxScale;
        }
    }

    static final Map<String, TypeDesc> PREDEFINED_TYPES = new HashMap<>();
    static final Map<Integer, TypeDesc> PREDEFINED_TYPE_IDS = new HashMap<>();
    static  {
        
    	// Char
        PREDEFINED_TYPES.put("CHAR", 	new TypeDesc(DBPDataKind.STRING, Types.CHAR, 0, 0, 0));
        PREDEFINED_TYPES.put("NCHAR", 	new TypeDesc(DBPDataKind.STRING, Types.NCHAR, 0, 0, 0));
        PREDEFINED_TYPES.put("VARCHAR", new TypeDesc(DBPDataKind.STRING, Types.VARCHAR, 0, 0, 0));
        PREDEFINED_TYPES.put("NVARCHAR",new TypeDesc(DBPDataKind.STRING, Types.NVARCHAR, 0, 0, 0));
        
        // Integer
        PREDEFINED_TYPES.put("SMALLINT",new TypeDesc(DBPDataKind.NUMERIC, Types.SMALLINT, 38, 127, -84));
        PREDEFINED_TYPES.put("INTEGER", new TypeDesc(DBPDataKind.NUMERIC, Types.INTEGER, 38, 127, -84));
        PREDEFINED_TYPES.put("BIGINT", 	new TypeDesc(DBPDataKind.NUMERIC, Types.BIGINT, 38, 127, -84));

        // Number
        // 4 byte floating point number
        PREDEFINED_TYPES.put("REAL", 	new TypeDesc(DBPDataKind.NUMERIC, Types.REAL, 7, 127, -84));
        // 8 byte floating point number
        PREDEFINED_TYPES.put("DOUBLE", 	new TypeDesc(DBPDataKind.NUMERIC, Types.DOUBLE, 15, 127, -84)); 
        
        // Decimal is identical to Numeric
        // PREDEFINED_TYPES.put("DECIMAL", new TypeDesc(DBPDataKind.NUMERIC, Types.DOUBLE, 38, 127, -84));
        PREDEFINED_TYPES.put("NUMERIC",	new TypeDesc(DBPDataKind.NUMERIC, Types.NUMERIC, 38, 127, -84));
        PREDEFINED_TYPES.put("FLOAT", 	new TypeDesc(DBPDataKind.NUMERIC, Types.FLOAT, 38, 0, 0));
        PREDEFINED_TYPES.put("NUMBER", 	new TypeDesc(DBPDataKind.NUMERIC, Types.NUMERIC, 38, 128, -84));

        // Date & time
        PREDEFINED_TYPES.put("DATE", 	new TypeDesc(DBPDataKind.DATETIME, Types.TIMESTAMP, 0, 0, 0));

        // LOB
        PREDEFINED_TYPES.put("BLOB", 	new TypeDesc(DBPDataKind.CONTENT, Types.BLOB, 0, 0, 0));
        PREDEFINED_TYPES.put("CLOB", 	new TypeDesc(DBPDataKind.CONTENT, Types.CLOB, 0, 0, 0));
        
        // Geometry
        PREDEFINED_TYPES.put("GEOMETRY", new TypeDesc(DBPDataKind.BINARY, Types.BINARY, 0, 0, 0));

        // Binary types
        PREDEFINED_TYPES.put("BIT", 	new TypeDesc(DBPDataKind.BINARY, Types.BINARY, 0, 0, 0));
        PREDEFINED_TYPES.put("VARBIT", 	new TypeDesc(DBPDataKind.BINARY, Types.VARBINARY, 0, 0, 0));
        PREDEFINED_TYPES.put("BYTE", 	new TypeDesc(DBPDataKind.BINARY, Types.BINARY, 0, 0, 0));
        PREDEFINED_TYPES.put("VARBYTE", new TypeDesc(DBPDataKind.BINARY, Types.VARBINARY, 0, 0, 0));
        PREDEFINED_TYPES.put("NIBBLE", 	new TypeDesc(DBPDataKind.BINARY, Types.BINARY, 0, 0, 0));

        for (TypeDesc type : PREDEFINED_TYPES.values()) {
            PREDEFINED_TYPE_IDS.put(type.valueType, type);
        }
    }
    
    /*
    private String typeCode;
    private byte[] typeOID;
    private Object superType;
    //private final AttributeCache attributeCache;
    // private final MethodCache methodCache;
    
    private boolean flagIncomplete;
    private boolean flagFinal;
    private boolean flagInstantiable;
    
    
    
    
    */
    private TypeDesc typeDesc;
    private boolean flagPredefined;
    private int valueType = java.sql.Types.OTHER;
    private String sourceDeclaration;
    private String sourceDefinition;
    private AltibaseDataType componentType;

    public AltibaseDataType(DBSObject owner, String typeName, boolean persisted)
    {
        super(owner, typeName, persisted);
        //this.methodCache = new MethodCache();
        if (owner instanceof AltibaseDataSource) {
            flagPredefined = true;
            findTypeDesc(typeName);
        }
    }

    public AltibaseDataType(DBSObject owner, ResultSet dbResult)
    {
        super(owner, JDBCUtils.safeGetString(dbResult, "TYPE_NAME"), true);
        //this.typeCode = JDBCUtils.safeGetString(dbResult, "TYPECODE");
        //this.typeOID = JDBCUtils.safeGetBytes(dbResult, "TYPE_OID");
        //this.flagPredefined = JDBCUtils.safeGetBoolean(dbResult, "PREDEFINED", AltibaseConstants.YES);
        //this.flagIncomplete = JDBCUtils.safeGetBoolean(dbResult, "INCOMPLETE", AltibaseConstants.YES);
        //this.flagFinal = JDBCUtils.safeGetBoolean(dbResult, "FINAL", AltibaseConstants.YES);
        //this.flagInstantiable = JDBCUtils.safeGetBoolean(dbResult, "INSTANTIABLE", AltibaseConstants.YES);
        //String superTypeOwner = JDBCUtils.safeGetString(dbResult, "SUPERTYPE_OWNER");
        //boolean hasAttributes;
        //boolean hasMethods;

        findTypeDesc(name);
    }

    // Use by tree navigator thru reflection
    public boolean hasMethods()
    {
        return false;
    }
    
    // Use by tree navigator thru reflection
    public boolean hasAttributes()
    {
        return false;
    }

    private boolean findTypeDesc(String typeName)
    {
        typeName = normalizeTypeName(typeName);
        this.typeDesc = PREDEFINED_TYPES.get(typeName);
        if (this.typeDesc == null) {
            log.warn("Unknown predefined type: " + typeName);
            return false;
        } else {
            this.valueType = this.typeDesc.valueType;
            return true;
        }
    }

    @Nullable
    public static DBPDataKind getDataKind(String typeName)
    {
        TypeDesc desc = PREDEFINED_TYPES.get(typeName);
        return desc != null ? desc.dataKind : null;
    }

    @Nullable
    @Override
    public AltibaseSchema getSchema()
    {
        return parent instanceof AltibaseSchema ? (AltibaseSchema)parent : null;
    }

    @Override
    public AltibaseSourceType getSourceType()
    {
        return AltibaseSourceType.TYPE;
    }

    @Override
    @Property(hidden = true, editable = true, updatable = true, order = -1)
    public String getObjectDefinitionText(DBRProgressMonitor monitor, Map<String, Object> options) throws DBCException
    {
    	return "-- Source code not available";
    }

    public void setObjectDefinitionText(String sourceDeclaration)
    {
        this.sourceDeclaration = sourceDeclaration;
    }

    @Override
    @Property(hidden = true, editable = true, updatable = true, order = -1)
    public String getExtendedDefinitionText(DBRProgressMonitor monitor) throws DBException
    {
        if (sourceDefinition == null && monitor != null) {
            sourceDefinition = AltibaseUtils.getSource(monitor, this, true, false);
        }
        return sourceDefinition;
    }

    public void setExtendedDefinitionText(String source)
    {
        this.sourceDefinition = source;
    }

    @Override
    public String getTypeName()
    {
        return getFullyQualifiedName(DBPEvaluationContext.DDL);
    }

    @Override
    public String getFullTypeName() {
        return DBUtils.getFullTypeName(this);
    }

    @Override
    public int getTypeID()
    {
        return valueType;
    }

    @Override
    public DBPDataKind getDataKind()
    {
        return JDBCUtils.resolveDataKind(getDataSource(), getName(), valueType);
    }

    @Override
    public Integer getScale()
    {
        return typeDesc == null ? 0 : typeDesc.minScale;
    }

    @Override
    public Integer getPrecision()
    {
        return typeDesc == null ? 0 : typeDesc.precision;
    }

    @Override
    public long getMaxLength()
    {
        return CommonUtils.toInt(getPrecision());
    }

    @Override
    public long getTypeModifiers() {
        return 0;
    }

    @Override
    public int getMinScale()
    {
        return typeDesc == null ? 0 : typeDesc.minScale;
    }

    @Override
    public int getMaxScale()
    {
        return typeDesc == null ? 0 : typeDesc.maxScale;
    }

    @NotNull
    @Override
    public DBCLogicalOperator[] getSupportedOperators(DBSTypedObject attribute) {
        return DBUtils.getDefaultOperators(this);
    }

    @Override
    public DBSObject getParentObject()
    {
        return parent instanceof AltibaseSchema ?
            parent :
            parent instanceof AltibaseDataSource ? ((AltibaseDataSource) parent).getContainer() : null;
    }

    @NotNull
    @Override
    @Property(viewable = true, editable = true, valueTransformer = DBObjectNameCaseTransformer.class, order = 1)
    public String getName()
    {
        return name;
    }

    @Property(viewable = true, editable = true, order = 2)
    public String getTypeCode()
    {
        return name;
    }

    @Property(hidden = true, viewable = false, editable = false)
    public byte[] getTypeOID()
    {
        return null;
    }

    @Property(viewable = true, editable = true, order = 3)
    public AltibaseDataType getSuperType(DBRProgressMonitor monitor)
    {
    	return null;
    }

    @Property(viewable = true, order = 4)
    public boolean isPredefined()
    {
        return flagPredefined;
    }

    @Property(viewable = true, order = 5)
    public boolean isIncomplete()
    {
        return false;
    }

    @Property(viewable = true, order = 6)
    public boolean isFinal()
    {
        return true;
    }

    @Property(viewable = true, order = 7)
    public boolean isInstantiable()
    {
        return false;
    }

    @NotNull
    @Override
    public DBSEntityType getEntityType()
    {
        return DBSEntityType.TYPE;
    }

    @Override
    @Association
    public List<AltibaseDataTypeAttribute> getAttributes(@NotNull DBRProgressMonitor monitor)
        throws DBException
    {
        return null;
    }

    @Nullable
    @Override
    public Collection<? extends DBSEntityConstraint> getConstraints(@NotNull DBRProgressMonitor monitor) throws DBException
    {
        return null;
    }

    @Override
    public AltibaseDataTypeAttribute getAttribute(@NotNull DBRProgressMonitor monitor, @NotNull String attributeName) throws DBException
    {
        return null;
    }

    /* TODO: Remove
    @Nullable
    @Association
    public Collection<AltibaseDataTypeMethod> getMethods(DBRProgressMonitor monitor)
        throws DBException
    {
        return methodCache != null ? methodCache.getAllObjects(monitor, this) : null;
    }
    */

    @Override
    public Collection<? extends DBSEntityAssociation> getAssociations(@NotNull DBRProgressMonitor monitor) throws DBException
    {
        return null;
    }

    @Override
    public Collection<? extends DBSEntityAssociation> getReferences(@NotNull DBRProgressMonitor monitor) throws DBException
    {
        return null;
    }

    @Nullable
    @Override
    public Object geTypeExtension() {
        return null;
    }

    @Property(viewable = true, order = 8)
    public AltibaseDataType getComponentType(@NotNull DBRProgressMonitor monitor)
        throws DBException
    {
        return componentType;
    }

    @NotNull
    @Override
    public String getFullyQualifiedName(DBPEvaluationContext context)
    {
        return parent instanceof AltibaseSchema ?
            DBUtils.getFullQualifiedName(getDataSource(), parent, this) :
            name;
    }

    @Override
    public String toString()
    {
        return getFullyQualifiedName(DBPEvaluationContext.UI);
    }

    public static AltibaseDataType resolveDataType(DBRProgressMonitor monitor, AltibaseDataSource dataSource, String typeOwner, String typeName)
    {
        typeName = normalizeTypeName(typeName);
        AltibaseSchema typeSchema = null;
        AltibaseDataType type = null;
        if (typeOwner != null) {
            try {
                typeSchema = dataSource.getSchema(monitor, typeOwner);
                if (typeSchema == null) {
                    log.error("Type attr schema '" + typeOwner + "' not found");
                } else {
                    type = typeSchema.getDataType(monitor, typeName);
                }
            } catch (DBException e) {
                log.error(e);
            }
        } else {
            type = (AltibaseDataType)dataSource.getLocalDataType(typeName);
        }
        if (type == null) {
            log.debug("Data type '" + typeName + "' not found - declare new one");
            type = new AltibaseDataType(typeSchema == null ? dataSource : typeSchema, typeName, true);
            type.flagPredefined = true;
            if (typeSchema == null) {
                dataSource.dataTypeCache.cacheObject(type);
            } else {
                typeSchema.dataTypeCache.cacheObject(type);
            }
        }
        return type;
    }

    private static String normalizeTypeName(String typeName) {
        if (CommonUtils.isEmpty(typeName)) {
            return "";
        }
        for (;;) {
            int modIndex = typeName.indexOf('(');
            if (modIndex == -1) {
                break;
            }
            int modEnd = typeName.indexOf(')', modIndex);
            if (modEnd == -1) {
                break;
            }
            typeName = typeName.substring(0, modIndex) +
                (modEnd == typeName.length() - 1 ? "" : typeName.substring(modEnd + 1));
        }
        return typeName;
    }

    @NotNull
    @Override
    public DBSObjectState getObjectState()
    {
        return DBSObjectState.NORMAL;
    }

    @Override
    public void refreshObjectState(@NotNull DBRProgressMonitor monitor) throws DBCException
    {

    }

	@Override
	public DBEPersistAction[] getCompileActions(DBRProgressMonitor monitor) throws DBCException {
		// TODO Auto-generated method stub
		return null;
	}
}
