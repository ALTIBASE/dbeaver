package org.jkiss.dbeaver.ext.altibase.data;

import java.sql.SQLException;
import java.sql.Types;

import org.jkiss.code.NotNull;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.data.gis.handlers.GISGeometryValueHandler;
import org.jkiss.dbeaver.ext.altibase.AltibaseConstants;
import org.jkiss.dbeaver.ext.altibase.model.AltibaseTable;
import org.jkiss.dbeaver.model.data.DBDAttributeBinding;
import org.jkiss.dbeaver.model.data.DBDDisplayFormat;
import org.jkiss.dbeaver.model.exec.DBCException;
import org.jkiss.dbeaver.model.exec.DBCSession;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCPreparedStatement;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCResultSet;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.gis.DBGeometry;
import org.jkiss.dbeaver.model.gis.GisAttribute;
import org.jkiss.dbeaver.model.struct.DBSTypedObject;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBWriter;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.io.WKTWriter;

/*
 * AltibaseGeometryValueHandler
 */
public class AltibaseGeometryValueHandler extends GISGeometryValueHandler {

	private static final Log log = Log.getLog(AltibaseGeometryValueHandler.class);
	
	public static final AltibaseGeometryValueHandler INSTANCE = new AltibaseGeometryValueHandler();
	
    @Override
    protected Object fetchColumnValue(DBCSession session, JDBCResultSet resultSet, DBSTypedObject type, int index) throws DBCException, SQLException {
    	Object object = resultSet.getObject(index);
    	return getValueFromObject(session, type, object,false, false);
    }

    @NotNull
    @Override
    public Class<?> getValueObjectType(@NotNull DBSTypedObject attribute) {
        return DBGeometry.class;
    }
    
    @Override
    public DBGeometry getValueFromObject(@NotNull DBCSession session, @NotNull DBSTypedObject type, Object object, boolean copy, boolean validateValue) throws DBCException {
    	DBGeometry dbGeometry = null;
    	byte[] bytes = null;
    	
    	try {
    		
    		/*
    		 * EWKT: 32,000 is maximum length of return value.
    		 */
    		if (object == null) {
                return new DBGeometry();
    		} else if (object instanceof DBGeometry) {
                if (copy) {
                    return ((DBGeometry) object).copy();
                } else {
                    return (DBGeometry) object;
                }
            }  else if (object instanceof Geometry) {
                return new DBGeometry((Geometry) object);
            } else {
    			Geometry geom = null;
    			// SRID=xxxx;MULTIPOLYGON (((199....
    			String[] sContent = ((String)object).split(";", 2);

    			// SRID
    			if (sContent.length == 2) {
    				
    				int srid = 0;
    				geom = new WKTReader().read(sContent[1]);
    				
    				sContent[0] = sContent[0].replaceAll("SRID=", "");
    				try {
    					srid = Integer.parseInt(sContent[0]);
    				} catch(NumberFormatException e) {
    					log.warn("Failed to parse SRID: " + e);
    				}
    				
    				geom.setSRID(srid);
    			} 
    			// No SRID, just in case.
    			else if (sContent.length == 1)
    			{
    				geom = new WKTReader().read(sContent[0]);
    			}

    			dbGeometry = new DBGeometry(geom);
        	}
    		
    	} catch (ParseException e) {
    		e.printStackTrace();
    	}

    	return dbGeometry;
    }

    
    @Override
    protected void bindParameter(JDBCSession session, JDBCPreparedStatement statement, DBSTypedObject paramType, int paramIndex, Object value) throws DBCException, SQLException {
    	
    	// Throw exception intentionally because unable to find update the given data with JDBC.
    	throw new DBCException("Not allowed update spatial data");
    	
    	/*
        int srid = 0;
        if (paramType instanceof DBDAttributeBinding) {
            paramType = ((DBDAttributeBinding) paramType).getAttribute();
        }
        if (value instanceof DBGeometry) {
            srid = ((DBGeometry) value).getSRID();
            value = ((DBGeometry) value).getRawValue();
        }
        if (srid == 0 && paramType instanceof GisAttribute) {
            srid = ((GisAttribute) paramType).getAttributeGeometrySRID(session.getProgressMonitor());
        }
        if (value == null) {
            statement.setNull(paramIndex, paramType.getTypeID());
        } else if (value instanceof Geometry) {
            if (((Geometry) value).getSRID() == 0) {
                ((Geometry) value).setSRID(srid);
            }
		*/
            /*
             * new WKBWriter(2, true):
             * 0xA101D ( 659485) stERR_ABORT_VALIDATE_INVALID_LENGTH Invalid data length. 
             * # *Cause: The length of the data exceeds the valid scope.
            
            byte[] wkbBytes = new WKBWriter(2, true).write((Geometry) value);
            statement.setObject(paramIndex, wkbBytes, AltibaseConstants.TYPE_GEOMETRY);
            */
            
            /*
             * new WKBWriter(2, true):
             * SQL Error [659458] [HY000]: Not applicable
            
            byte[] wkbBytes = new WKBWriter(2).write((Geometry) value);
            statement.setObject(paramIndex, wkbBytes, AltibaseConstants.TYPE_GEOMETRY);
            */
            
            /*
             * SQL Error [135180] [22018]: Conversion not applicable.
             
            String strGeom = "GEOMETRY'" +  getStringFromGeometry(session, (Geometry)value) + "'";
            statement.setString(paramIndex, strGeom);
            */
        /*
    	} else {
            String strValue = value.toString();
            if (srid != 0 && !strValue.startsWith("SRID=")) {
                strValue = "SRID=" + srid + ";" + strValue;
            }
            statement.setObject(paramIndex, strValue, AltibaseConstants.TYPE_GEOMETRY);
        }
        */
    }
    
    private String getStringFromGeometry(JDBCSession session, Geometry geometry) throws DBCException {
        // Use all possible dimensions (4 stands for XYZM) for the most verbose output (see DBGeometry#getString)
        final String strGeom = new WKTWriter(4).write(geometry);
        if (geometry.getSRID() > 0) {
            return "SRID=" + geometry.getSRID() + ";" + strGeom;
        } else {
            return strGeom;
        }
    }
    
    @NotNull
    @Override
    public String getValueDisplayString(@NotNull DBSTypedObject column, Object value, @NotNull DBDDisplayFormat format) {
        if (value instanceof DBGeometry && format == DBDDisplayFormat.NATIVE) {
            int valueSRID = ((DBGeometry) value).getSRID();
            String strValue = value.toString();
            if (valueSRID != 0 && !strValue.startsWith("SRID=")) {
                strValue = "SRID=" + valueSRID + ";" + strValue;
            }
            return strValue;
        }
        return super.getValueDisplayString(column, value, format);
    }
}