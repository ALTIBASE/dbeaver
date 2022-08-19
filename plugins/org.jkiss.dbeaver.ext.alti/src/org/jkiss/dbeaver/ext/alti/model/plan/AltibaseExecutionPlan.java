package org.jkiss.dbeaver.ext.alti.model.plan;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.alti.AltibaseUtils;
import org.jkiss.dbeaver.ext.alti.model.AltibaseDataSource;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanNode;
import org.jkiss.dbeaver.model.impl.plan.AbstractExecutionPlan;

public class AltibaseExecutionPlan extends AbstractExecutionPlan {

	AltibaseDataSource dataSource;
	private JDBCSession session;
	private String query;
	private List<AltibasePlanNode> rootNodes;
	
	public AltibaseExecutionPlan(AltibaseDataSource dataSource, JDBCSession session, String query)
    {
        this.dataSource = dataSource;
        this.session = session;
        this.query = query;
    }
	
	
	public void explain()
	        throws DBException
	{
		try {
			JDBCPreparedStatement dbStat = session.prepareStatement(getQueryString());
			// Read explained plan
			try {
				String plan = getExplainPlan(session, query);
				rootNodes = AltibasePlanBuilder.build(plan);
			} finally {
				dbStat.close();
			}
		} catch (SQLException e) {
			throw new DBCException(e, session.getExecutionContext());
		}
	}
	
	@Override
	public String getQueryString() {
		return query;
	}

	@Override
	public String getPlanQueryString() throws DBException {
		return null;
	}

	@Override
	public List<? extends DBCPlanNode> getPlanNodes(Map<String, Object> options) {
		return rootNodes;
	}

    public String getExplainPlan(JDBCSession session, String query) {
    	
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
	private Method getMethod4setExplainByteArg(Class class1, String methodName, String argName)
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
