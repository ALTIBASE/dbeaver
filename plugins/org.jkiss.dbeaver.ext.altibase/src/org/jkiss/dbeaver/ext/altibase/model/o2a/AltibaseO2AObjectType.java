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

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.model.DBIcon;
import org.jkiss.dbeaver.model.DBPImage;
import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.model.struct.DBSObject;
import org.jkiss.dbeaver.model.struct.DBSObjectType;

import java.util.HashMap;
import java.util.Map;

/**
 * Object type
 */
public enum AltibaseO2AObjectType implements DBSObjectType {

    CONSTRAINT ("CONSTRAINT", DBIcon.TREE_CONSTRAINT, AltibaseO2ATableConstraint.class, null), // fake object
	DIRECTORY("DIRECTORY", null, DBSObject.class, null),
    FOREIGN_KEY ("FOREIGN KEY", DBIcon.TREE_FOREIGN_KEY, AltibaseO2ATableForeignKey.class, null), // fake object
	FUNCTION("FUNCTION", DBIcon.TREE_PROCEDURE, AltibasevO2AProcedureStandalone.class, new ObjectFinder() {
        @Override
        public AltibasevO2AProcedureStandalone findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.proceduresCache.getObject(monitor, schema, objectName);
        }
    }),
	INDEX("INDEX", DBIcon.TREE_INDEX, AltibaseO2ATableIndex.class, new ObjectFinder() {
        @Override
        public AltibaseO2ATableIndex findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.indexCache.getObject(monitor, schema, objectName);
        }
    }),
	INDEX_PARTITION("INDEX PARTITION", null, DBSObject.class, null),
	INDEXTYPE("INDEXTYPE", null, DBSObject.class, null),
	JAVA_DATA("JAVA DATA", null, DBSObject.class, null),
	JAVA_RESOURCE("JAVA RESOURCE", null, DBSObject.class, null),
	JOB("JOB", null, DBSObject.class, null),
	JOB_CLASS("JOB CLASS", null, DBSObject.class, null),
	LIBRARY("LIBRARY", null, DBSObject.class, null),
	LOB("CONTENT", null, DBSObject.class, null),
	MATERIALIZED_VIEW("MATERIALIZED VIEW", DBIcon.TREE_VIEW, DBSObject.class, null),
	OPERATOR("OPERATOR", null, DBSObject.class, null),
	PACKAGE("PACKAGE", DBIcon.TREE_PACKAGE, AltibaseO2APackage.class, new ObjectFinder() {
        @Override
        public AltibaseO2APackage findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.packageCache.getObject(monitor, schema, objectName);
        }
    }),
	PACKAGE_BODY("PACKAGE BODY", DBIcon.TREE_PACKAGE, AltibaseO2APackage.class, new ObjectFinder() {
        @Override
        public AltibaseO2APackage findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.packageCache.getObject(monitor, schema, objectName);
        }
    }),
	PROCEDURE("PROCEDURE", DBIcon.TREE_PROCEDURE, AltibasevO2AProcedureStandalone.class, new ObjectFinder() {
        @Override
        public AltibasevO2AProcedureStandalone findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.proceduresCache.getObject(monitor, schema, objectName);
        }
    }),
	PROGRAM("PROGRAM", null, DBSObject.class, null),
    QUEUE("QUEUE", null, AltibaseO2AQueue.class, new ObjectFinder() {
        @Override
        public AltibaseO2AQueue findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.queueCache.getObject(monitor, schema, objectName);
        }
    }),
	RULE("RULE", null, DBSObject.class, null),
	RULE_SET("RULE SET", null, DBSObject.class, null),
	SCHEDULE("SCHEDULE", null, DBSObject.class, null),
	SEQUENCE("SEQUENCE", DBIcon.TREE_SEQUENCE, AltibaseO2ASequence.class, new ObjectFinder() {
        @Override
        public AltibaseO2ASequence findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.sequenceCache.getObject(monitor, schema, objectName);
        }
    }),
	SYNONYM("SYNONYM", DBIcon.TREE_SYNONYM, AltibaseO2ASynonym.class, new ObjectFinder() {
        @Override
        public AltibaseO2ASynonym findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.synonymCache.getObject(monitor, schema, objectName);
        }
    }),
	TABLE("TABLE", DBIcon.TREE_TABLE, AltibaseO2ATable.class, new ObjectFinder() {
        @Override
        public AltibaseO2ATableBase findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.tableCache.getObject(monitor, schema, objectName);
        }
    }),
	TABLE_PARTITION("TABLE PARTITION", null, DBSObject.class, null),
	TRIGGER("TRIGGER", DBIcon.TREE_TRIGGER, AltibaseO2ATrigger.class, new ObjectFinder() {
        @Override
        public AltibaseO2ATrigger findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            // First we will try to find a trigger at the tables level
            AltibaseO2ATableTrigger trigger = schema.tableTriggerCache.getObject(monitor, schema, objectName);
            if (trigger != null) {
                return trigger;
            }
            // Nope. Now we will try to find a trigger at the schemas level
            return schema.triggerCache.getObject(monitor, schema, objectName);
        }
    }),
	TYPE("TYPE", DBIcon.TREE_DATA_TYPE, AltibaseO2ADataType.class, new ObjectFinder() {
        @Override
        public AltibaseO2ADataType findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.dataTypeCache.getObject(monitor, schema, objectName);
        }
    }),
	TYPE_BODY("TYPE BODY", DBIcon.TREE_DATA_TYPE, AltibaseO2ADataType.class, new ObjectFinder() {
        @Override
        public AltibaseO2ADataType findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.dataTypeCache.getObject(monitor, schema, objectName);
        }
    }),
	VIEW("VIEW", DBIcon.TREE_VIEW, AltibaseO2AView.class, new ObjectFinder() {
        @Override
        public AltibaseO2AView findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
        {
            return schema.tableCache.getObject(monitor, schema, objectName, AltibaseO2AView.class);
        }
    });
    
    private static final Log log = Log.getLog(AltibaseO2AObjectType.class);

    private static Map<String, AltibaseO2AObjectType> typeMap = new HashMap<>();

    static {
        for (AltibaseO2AObjectType type : values()) {
            typeMap.put(type.getTypeName(), type);
        }
    }
    
    public static AltibaseO2AObjectType getByType(String typeName)
    {
        return typeMap.get(typeName);
    }

    private static interface ObjectFinder {
        DBSObject findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException;
    }
    
    private final String objectType;
    private final DBPImage image;
    private final Class<? extends DBSObject> typeClass;
    private final ObjectFinder finder;

    <OBJECT_TYPE extends DBSObject> AltibaseO2AObjectType(String objectType, DBPImage image, Class<OBJECT_TYPE> typeClass, ObjectFinder finder)
    {
        this.objectType = objectType;
        this.image = image;
        this.typeClass = typeClass;
        this.finder = finder;
    }

    public boolean isBrowsable()
    {
        return finder != null;
    }

    @Override
    public String getTypeName()
    {
        return objectType;
    }

    @Override
    public String getDescription()
    {
        return null;
    }

    @Override
    public DBPImage getImage()
    {
        return image;
    }

    @Override
    public Class<? extends DBSObject> getTypeClass()
    {
        return typeClass;
    }

    public DBSObject findObject(DBRProgressMonitor monitor, AltibaseO2ASchema schema, String objectName) throws DBException
    {
        if (finder != null) {
            return finder.findObject(monitor, schema, objectName);
        } else {
            return null;
        }
    }

    public static Object resolveObject(
        DBRProgressMonitor monitor,
        AltibaseO2ADataSource dataSource,
        String dbLink,
        String objectTypeName,
        String objectOwner,
        String objectName) throws DBException
    {
        if (dbLink != null) {
            return objectName;
        }
        AltibaseO2AObjectType objectType = AltibaseO2AObjectType.getByType(objectTypeName);
        if (objectType == null) {
            log.debug("Unrecognized Altibase object type: " + objectTypeName);
            return objectName;
        }
        if (!objectType.isBrowsable()) {
            log.debug("Unsupported Altibase object type: " + objectTypeName);
            return objectName;
        }
        final AltibaseO2ASchema schema = dataSource.getSchema(monitor, objectOwner);
        if (schema == null) {
            log.debug("Schema '" + objectOwner + "' not found");
            return objectName;
        }
        final DBSObject object = objectType.findObject(monitor, schema, objectName);
        if (object == null) {
            log.debug(objectTypeName + " '" + objectName + "' not found in '" + schema.getName() + "'");
            return objectName;
        }
        return object;
    }

    @Override
    public String toString()
    {
        return objectType;
    }


}
