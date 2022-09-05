package org.jkiss.dbeaver.ext.altibase.model;

import org.jkiss.dbeaver.model.DBPDataKind;

import java.sql.Types;

public enum AltibaseFieldType {
	// char types
	CHAR 	("CHAR", 	1,  DBPDataKind.STRING, Types.CHAR),
	VARCHAR ("VARCHAR", 12, DBPDataKind.STRING, Types.VARCHAR),
	NCHAR  	("NCHAR", 	-8, DBPDataKind.STRING, Types.CHAR), // Types.NCHAR returns question mark.
	NVARCHAR("NVARCHAR",-9, DBPDataKind.STRING, Types.NVARCHAR),
	// encrypted column data type
	ECHAR	("ECHAR",	60, DBPDataKind.STRING, Types.BINARY),
	EVARCHAR("EVARCHAR",61, DBPDataKind.STRING, Types.BINARY),

	// number types
	INTEGER	("INTEGER",  4,  	DBPDataKind.NUMERIC, Types.INTEGER),
	SMALLINT("SMALLINT", 5,  	DBPDataKind.NUMERIC, Types.SMALLINT),
	BIGINT 	("BIGINT", 	 -5, 	DBPDataKind.NUMERIC, Types.BIGINT),
	REAL 	("REAL", 	 7,  	DBPDataKind.NUMERIC, Types.REAL),
	NUMBER	("NUMBER", 	 10002, DBPDataKind.NUMERIC, Types.NUMERIC),
	NUMERIC ("NUMERIC",  2, 	DBPDataKind.NUMERIC, Types.NUMERIC),
	DOUBLE 	("DOUBLE", 	 8, 	DBPDataKind.NUMERIC, Types.DOUBLE),
	FLOAT 	("FLOAT", 	 6, 	DBPDataKind.NUMERIC, Types.FLOAT),

	// date & time
	DATE 	("DATE", 	 9,  	DBPDataKind.DATETIME, Types.TIMESTAMP),

	// binary
	BIT 	("BIT", 	 -7, 	DBPDataKind.BINARY, Types.BINARY),
	VARBIT 	("VARBIT", 	 -100, 	DBPDataKind.BINARY, Types.BINARY),
	BYTE 	("BYTE", 	 20001, DBPDataKind.BINARY, Types.BINARY),
	VARBYTE ("VARBYTE",  20003, DBPDataKind.BINARY, Types.BINARY),
	NIBBLE 	("NIBBLE", 	 20002, DBPDataKind.BINARY, Types.BINARY),
	BINARY 	("BINARY", 	 -2, 	DBPDataKind.BINARY, Types.BINARY),

	CLOB 	("CLOB", 	 40, 	DBPDataKind.BINARY, Types.CLOB),
	BLOB 	("BLOB", 	 30, 	DBPDataKind.BINARY, Types.BLOB),
	GEOMETRY("GEOMETRY", 10003, DBPDataKind.BINARY, Types.BLOB);

    private final String name;
    private final int typeID;
    private final int valueType;
    private final DBPDataKind dataKind;
    

    AltibaseFieldType(String name, int typeID, DBPDataKind dataKind, int valueType) {
    	this.name = name;
    	this.typeID = typeID;
        this.valueType = valueType;
        this.dataKind = dataKind;
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

    public static AltibaseFieldType getById(int id) {

        for (AltibaseFieldType ft : values()) {
            if (ft.getTypeID() == id) {
                return ft;
            }
        }
        return null;
    }
}