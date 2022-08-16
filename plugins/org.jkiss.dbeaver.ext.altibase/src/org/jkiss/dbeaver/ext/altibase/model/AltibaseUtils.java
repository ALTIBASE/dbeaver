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

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.ext.altibase.model.meta.AltibaseMetaColumn;
import org.jkiss.dbeaver.ext.altibase.model.meta.AltibaseMetaObject;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCUtils;
import org.jkiss.dbeaver.model.sql.SQLDialect;
import org.jkiss.dbeaver.model.sql.SQLUtils;
import org.jkiss.dbeaver.model.struct.DBSObject;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Generic utils
 */
public class AltibaseUtils {

    public static Object getColumn(AltibaseDataSource dataSource, String objectType, String columnId)
    {
        AltibaseMetaObject object = dataSource.getMetaObject(objectType);
        if (object == null) {
            return columnId;
        }
        AltibaseMetaColumn column = object.getColumn(columnId);
        if (column == null || !column.isSupported()) {
            return columnId;
        }
        return column.getColumnIdentifier();
    }

    public static Object getColumn(AltibaseMetaObject object, String columnId)
    {
        AltibaseMetaColumn column = object == null ? null : object.getColumn(columnId);
        if (column == null || !column.isSupported()) {
            return columnId;
        }
        return column.getColumnIdentifier();
    }


    public static String safeGetString(AltibaseMetaObject object, ResultSet dbResult, String columnId)
    {
        Object column = getColumn(object, columnId);
        if (column instanceof Number) {
            return JDBCUtils.safeGetString(dbResult, ((Number) column).intValue());
        } else {
            return JDBCUtils.safeGetString(dbResult, column.toString());
        }
    }

    public static String safeGetStringTrimmed(AltibaseMetaObject object, ResultSet dbResult, String columnId)
    {
        Object column = getColumn(object, columnId);
        if (column instanceof Number) {
            return JDBCUtils.safeGetStringTrimmed(dbResult, ((Number) column).intValue());
        } else {
            return JDBCUtils.safeGetStringTrimmed(dbResult, column.toString());
        }
    }

    public static int safeGetInt(AltibaseMetaObject object, ResultSet dbResult, String columnId)
    {
        Object column = getColumn(object, columnId);
        if (column instanceof Number) {
            return JDBCUtils.safeGetInt(dbResult, ((Number) column).intValue());
        } else {
            return JDBCUtils.safeGetInt(dbResult, column.toString());
        }
    }

    public static Integer safeGetInteger(AltibaseMetaObject object, ResultSet dbResult, String columnId)
    {
        Object column = getColumn(object, columnId);
        if (column instanceof Number) {
            return JDBCUtils.safeGetInteger(dbResult, ((Number) column).intValue());
        } else {
            return JDBCUtils.safeGetInteger(dbResult, column.toString());
        }
    }

    public static long safeGetLong(AltibaseMetaObject object, ResultSet dbResult, String columnId)
    {
        Object column = getColumn(object, columnId);
        if (column instanceof Number) {
            return JDBCUtils.safeGetLong(dbResult, ((Number) column).intValue());
        } else {
            return JDBCUtils.safeGetLong(dbResult, column.toString());
        }
    }

    public static double safeGetDouble(AltibaseMetaObject object, ResultSet dbResult, String columnId)
    {
        Object column = getColumn(object, columnId);
        if (column instanceof Number) {
            return JDBCUtils.safeGetDouble(dbResult, ((Number)column).intValue());
        } else {
            return JDBCUtils.safeGetDouble(dbResult, column.toString());
        }
    }

    public static BigDecimal safeGetBigDecimal(AltibaseMetaObject object, ResultSet dbResult, String columnId)
    {
        Object column = getColumn(object, columnId);
        if (column instanceof Number) {
            return JDBCUtils.safeGetBigDecimal(dbResult, ((Number)column).intValue());
        } else {
            return JDBCUtils.safeGetBigDecimal(dbResult, column.toString());
        }
    }

    public static boolean safeGetBoolean(AltibaseMetaObject object, ResultSet dbResult, String columnId)
    {
        Object column = getColumn(object, columnId);
        if (column instanceof Number) {
            return JDBCUtils.safeGetBoolean(dbResult, ((Number)column).intValue());
        } else {
            return JDBCUtils.safeGetBoolean(dbResult, column.toString());
        }
    }

    public static Object safeGetObject(AltibaseMetaObject object, ResultSet dbResult, String columnId)
    {
        Object column = getColumn(object, columnId);
        if (column instanceof Number) {
            return JDBCUtils.safeGetObject(dbResult, ((Number) column).intValue());
        } else {
            return JDBCUtils.safeGetObject(dbResult, column.toString());
        }
    }

    public static boolean isLegacySQLDialect(DBSObject owner) {
    	return false;
    }

    public static String normalizeProcedureName(String procedureName) {
        int divPos = procedureName.lastIndexOf(';');
        if (divPos != -1) {
            // [JDBC: SQL Server native driver]
            procedureName = procedureName.substring(0, divPos);
        }
        return procedureName;

    }

    public static boolean canAlterTable(@NotNull DBSObject object) {
        // Either object is not yet persisted (so no alter is required) or database supports table altering
        return !object.isPersisted() || object.getDataSource().getSQLDialect().supportsAlterTableStatement();
    }
    
    public static String getExplainPlan(JDBCSession session, String query) {
    	
    	Statement stmt = null;
    	String plan = "";
    	String methodName = "setExplainPlan";
    	String argType = "byte";
    	
		try {
			Connection conn = session.getOriginal();
			Class<? extends Connection> clazz = conn.getClass();
			Method method = getMethod4setExplainByteArg(clazz, methodName, argType);

			if (method == null) {
				throw new InvocationTargetException(null, 
						String.format("Unable to find the target method: [class] %s, [method] %s, [argument type] %s", 
								clazz.toString(), methodName, argType));
			}
			
			// sConn.setExplainPlan(AltibaseConnection.EXPLAIN_PLAN_ONLY);
			method.invoke(conn, Byte.valueOf((byte) 2));
			
			// sStmt.getExplainPlan()
			stmt = conn.prepareStatement(query);
			plan = (String) stmt.getClass().getMethod("getExplainPlan").invoke(stmt);
		} catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
				| SecurityException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally
		{
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		
    	return plan;
    }
    
    // There are two setExplain in Connction class: the first one's argument is boolean, the second one's argument is byte.
    // Return the second one.
	private static Method getMethod4setExplainByteArg(Class class1, String methodName, String argName)
    {
		for(Method method:class1.getDeclaredMethods()) {
			if (method.getName().equals(methodName))
			{
				for(Class paramType:method.getParameterTypes())
					if (paramType.toString().equals(argName))
							return method;
			}
		}
		
    	return null;
    }
}
