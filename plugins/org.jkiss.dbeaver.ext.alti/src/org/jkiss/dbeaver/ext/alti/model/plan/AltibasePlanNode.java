package org.jkiss.dbeaver.ext.alti.model.plan;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.jkiss.dbeaver.ext.alti.AltibaseUtils;
import org.jkiss.dbeaver.model.exec.plan.DBCPlanNode;
import org.jkiss.dbeaver.model.impl.plan.AbstractExecutionPlanNode;

public class AltibasePlanNode extends AbstractExecutionPlanNode {

	private int depth;
	private String plan;
	AltibasePlanNode parent;
	private List<AltibasePlanNode> nested;
	
	public AltibasePlanNode(int depth, String plan, AltibasePlanNode parent)
	{
		this.depth = depth;
		this.plan = plan;
		this.parent = parent;
		if (this.parent != null) {
			this.parent.addChildNode(this);
		}
		
		nested = new ArrayList<AltibasePlanNode>();
	}
	
	public void addChildNode(AltibasePlanNode node) {
		nested.add(node);
	}
	
	@Override
	public String getNodeName() {
		return plan;
	}

	@Override
	public String getNodeType() {
		return "Plan";
	}

	@Override
	public DBCPlanNode getParent() {
		return parent;
	}

	@Override
	public Collection<? extends DBCPlanNode> getNested() {
		return nested;
	}
	
	public int getDepth() { 
		return depth; 
	}
	
	public String toString() {
		return plan;
	}
	
	public String toString4Debug() {
		StringBuilder sb = new StringBuilder();
		
		sb.append(String.format("[depth:%3d] ", depth));
		
		for(int i = 0; i < depth; i++) {
			sb.append("-");
		}
		
		sb.append(plan).append(AltibaseUtils.NEW_LINE);
		for(AltibasePlanNode node:nested) {
			sb.append(node.toString4Debug());
		}
		
		return sb.toString();
	}
	
	// in case of depth < this.depth
	public AltibasePlanNode getParentNodeAtDepth(int depth) {
		if (this.depth > depth) {
			return this.parent.getParentNodeAtDepth(depth);
		} else if (this.depth == depth) {
			return this.parent;
		} else {
			throw new IllegalArgumentException("Argument depth: " + depth + ", this.depth: " + this.depth);
		}
			
	}

}
