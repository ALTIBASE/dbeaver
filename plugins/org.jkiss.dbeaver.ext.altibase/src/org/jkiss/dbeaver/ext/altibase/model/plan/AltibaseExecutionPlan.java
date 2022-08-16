package org.jkiss.dbeaver.ext.altibase.model.plan;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.jkiss.dbeaver.DBException;
import org.jkiss.dbeaver.ext.altibase.model.AltibaseDataSource;
import org.jkiss.dbeaver.ext.altibase.model.AltibaseUtils;
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
				String plan = AltibaseUtils.getExplainPlan(session, query);
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

}
