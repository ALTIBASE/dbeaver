package org.jkiss.dbeaver.ext.altibase.data;

import java.sql.Types;

import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.model.DBPDataSource;
import org.jkiss.dbeaver.model.data.DBDFormatSettings;
import org.jkiss.dbeaver.model.data.DBDValueHandler;
import org.jkiss.dbeaver.model.data.DBDValueHandlerProvider;
import org.jkiss.dbeaver.model.struct.DBSTypedObject;

public class AltibaseValueHandlerProvider implements DBDValueHandlerProvider {

    @Nullable
    @Override
	public DBDValueHandler getValueHandler(DBPDataSource dataSource, DBDFormatSettings preferences,
			DBSTypedObject typedObject) {

		switch (typedObject.getTypeID()) {
        case Types.BIT:
        	return AltibaseBitSetValueHandler.INSTANCE;
		}

		return null;
	}

}
