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
package org.jkiss.dbeaver.ext.alti.model;

import org.jkiss.dbeaver.model.DBPDataKind;

import java.sql.Types;

public enum AltibaseFieldType {
	SMALLINT(7, Types.SMALLINT, DBPDataKind.NUMERIC, "SMALLINT"),
	INTEGER(8, Types.INTEGER, DBPDataKind.NUMERIC, "INTEGER"),
	FLOAT(10, Types.FLOAT, DBPDataKind.NUMERIC, "FLOAT"),
	DATE(12, Types.DATE, DBPDataKind.DATETIME, "DATE"),
	TIME(13, Types.TIME, DBPDataKind.DATETIME, "TIME"),
	CHAR(14, Types.CHAR, DBPDataKind.STRING, "CHAR"),
	BIGINT(16, Types.BIGINT, DBPDataKind.NUMERIC, "BIGINT"),
    NUMERIC(16, Types.NUMERIC, DBPDataKind.NUMERIC, "NUMERIC"), // Equal id, but another subtype - 1
    DECIMAL(16, Types.DECIMAL, DBPDataKind.NUMERIC, "DECIMAL"), // Equal id, but another subtype - 2
    BOOLEAN(23, Types.BOOLEAN, DBPDataKind.BOOLEAN, "BOOLEAN"),
	DOUBLE_PRECISION(27, Types.DOUBLE, DBPDataKind.NUMERIC, "DOUBLE PRECISION"),
	TIMESTAMP(35, Types.TIMESTAMP, DBPDataKind.DATETIME, "TIMESTAMP"),
	VARCHAR(37, Types.VARCHAR, DBPDataKind.STRING, "VARCHAR"),
	BLOB(261, Types.BLOB, DBPDataKind.CONTENT, "BLOB");

    private final int typeID;
    private final int valueType;
    private final DBPDataKind dataKind;
    private final String name;

    AltibaseFieldType(int typeID, int valueType, DBPDataKind dataKind, String name) {
        this.typeID = typeID;
        this.valueType = valueType;
        this.dataKind = dataKind;
        this.name = name;
    }

    public int getTypeID() {
        return typeID;
    }

    public int getValueType() {
        return valueType;
    }

    public DBPDataKind getDataKind() {
        return dataKind;
    }

    public String getName() {
        return name;
    }

    public static AltibaseFieldType getById(int id, int subTypeId) {
        if (id == 16) {
            switch (subTypeId) {
                case 1:
                    return NUMERIC;
                case 2:
                    return DECIMAL;
                default:
                    return BIGINT;
            }
        }
        for (AltibaseFieldType ft : values()) {
            if (ft.getTypeID() == id) {
                return ft;
            }
        }
        return null;
    }
}