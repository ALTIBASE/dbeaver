package org.jkiss.dbeaver.ext.altibase.data;

import java.sql.SQLException;
import java.util.BitSet;

import org.jkiss.dbeaver.model.data.DBDContent;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.impl.jdbc.data.handlers.JDBCContentValueHandler;
import org.jkiss.dbeaver.model.impl.jdbc.data.handlers.JDBCObjectValueHandler;
import org.jkiss.dbeaver.model.struct.DBSTypedObject;

public class AltibaseBitSetValueHandler extends JDBCObjectValueHandler {
	
	public static final AltibaseBitSetValueHandler INSTANCE = new AltibaseBitSetValueHandler();
	
    @Override
    protected Object fetchColumnValue(
        DBCSession session,
        JDBCResultSet resultSet,
        DBSTypedObject type,
        int index)
        throws DBCException, SQLException
    {
    	String value = null;
        Object obj = resultSet.getObject(index);
        
        // binary representation of BitSet
        if (obj != null && !resultSet.wasNull()) {
        	BitSet bitSet = (BitSet) obj;
        	int size = bitSet.size();
        	
        	StringBuilder sb = new StringBuilder(size);
        	for(int i = 0; i < size; i++) {
        		sb.append(bitSet.get(i)?'1':'0');
        	}
        	
        	value = sb.toString();
        }

        return getValueFromObject(session, type, value, false, false);
    }
}
