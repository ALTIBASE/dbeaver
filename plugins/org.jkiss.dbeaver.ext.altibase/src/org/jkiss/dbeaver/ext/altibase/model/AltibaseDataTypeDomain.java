package org.jkiss.dbeaver.ext.altibase.model;

import org.jkiss.dbeaver.ext.altibase.AltibaseConstants;
import org.jkiss.dbeaver.model.DBPDataKind;

import java.sql.Types;

public enum AltibaseDataTypeDomain {
	// char types
	CHAR 	("CHAR", 	AltibaseConstants.TYPE_CHAR,  		DBPDataKind.STRING, Types.CHAR),
	VARCHAR ("VARCHAR", AltibaseConstants.TYPE_VARCHAR, 	DBPDataKind.STRING, Types.VARCHAR),
	NCHAR  	("NCHAR", 	AltibaseConstants.TYPE_NCHAR, 		DBPDataKind.STRING, Types.NVARCHAR), // Types.NCHAR returns question mark.
	NVARCHAR("NVARCHAR",AltibaseConstants.TYPE_NVARCHAR,	DBPDataKind.STRING, Types.NVARCHAR),
	// encrypted column data type
	ECHAR	("ECHAR",	AltibaseConstants.TYPE_ECHAR, 		DBPDataKind.STRING, Types.BINARY),
	EVARCHAR("EVARCHAR",AltibaseConstants.TYPE_EVARCHAR,	DBPDataKind.STRING, Types.BINARY),

	// number types
	INTEGER	("INTEGER",  AltibaseConstants.TYPE_INTEGER,	DBPDataKind.NUMERIC, Types.INTEGER),
	SMALLINT("SMALLINT", AltibaseConstants.TYPE_SMALLINT,	DBPDataKind.NUMERIC, Types.SMALLINT),
	BIGINT 	("BIGINT", 	 AltibaseConstants.TYPE_BIGINT, 	DBPDataKind.NUMERIC, Types.BIGINT),
	REAL 	("REAL", 	 AltibaseConstants.TYPE_REAL,	  	DBPDataKind.NUMERIC, Types.REAL),
	NUMBER	("NUMBER", 	 AltibaseConstants.TYPE_NUMBER, 	DBPDataKind.NUMERIC, Types.NUMERIC),
	NUMERIC ("NUMERIC",  AltibaseConstants.TYPE_NUMERIC, 	DBPDataKind.NUMERIC, Types.NUMERIC),
	DOUBLE 	("DOUBLE", 	 AltibaseConstants.TYPE_DOUBLE, 	DBPDataKind.NUMERIC, Types.DOUBLE),
	FLOAT 	("FLOAT", 	 AltibaseConstants.TYPE_FLOAT, 		DBPDataKind.NUMERIC, Types.FLOAT),

	// date & time
	DATE 	("DATE", 	 AltibaseConstants.TYPE_DATE,  		DBPDataKind.DATETIME, Types.TIMESTAMP),

	// binary
	BIT 	("BIT", 	 AltibaseConstants.TYPE_BIT, 		DBPDataKind.BINARY, Types.BINARY),
	VARBIT 	("VARBIT", 	 AltibaseConstants.TYPE_VARBIT,		DBPDataKind.BINARY, Types.BINARY),
	BYTE 	("BYTE", 	 AltibaseConstants.TYPE_BYTE, 		DBPDataKind.BINARY, Types.BINARY),
	VARBYTE ("VARBYTE",  AltibaseConstants.TYPE_VARBYTE,	DBPDataKind.BINARY, Types.BINARY),
	NIBBLE 	("NIBBLE", 	 AltibaseConstants.TYPE_NIBBLE, 	DBPDataKind.BINARY, Types.BINARY),
	BINARY 	("BINARY", 	 AltibaseConstants.TYPE_BINARY, 	DBPDataKind.BINARY, Types.BINARY),

	CLOB 	("CLOB", 	 AltibaseConstants.TYPE_CLOB, 		DBPDataKind.CONTENT, Types.CLOB),
	BLOB 	("BLOB", 	 AltibaseConstants.TYPE_BLOB, 		DBPDataKind.CONTENT, Types.BLOB),
	GEOMETRY("GEOMETRY", AltibaseConstants.TYPE_GEOMETRY,	DBPDataKind.CONTENT, Types.VARBINARY); 

    private final String name;
    private final int typeID;
    private final int valueType;
    private final DBPDataKind dataKind;
    

    AltibaseDataTypeDomain(String name, int typeID, DBPDataKind dataKind, int valueType) {
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

    public static AltibaseDataTypeDomain getById(int id) {

        for (AltibaseDataTypeDomain ft : values()) {
            if (ft.getTypeID() == id) {
                return ft;
            }
        }
        return null;
    }
}