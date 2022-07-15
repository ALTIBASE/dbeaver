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

package org.jkiss.dbeaver.ext.altibase.model.o2a;

import org.jkiss.dbeaver.model.struct.rdb.DBSProcedureParameterKind;
import org.jkiss.utils.CommonUtils;

/**
* Parameter/argument mode
*/
public enum AltibaseParameterMode {
    IN(DBSProcedureParameterKind.IN),
    OUT(DBSProcedureParameterKind.OUT),
    INOUT(DBSProcedureParameterKind.INOUT),
    RETURN(DBSProcedureParameterKind.RETURN);
    private final DBSProcedureParameterKind parameterKind;

    AltibaseParameterMode(DBSProcedureParameterKind parameterKind)
    {
        this.parameterKind = parameterKind;
    }

    public static AltibaseParameterMode getMode(String modeName)
    {
        if (CommonUtils.isEmpty(modeName)) {
            return null;
        } else if ("IN".equals(modeName)) {
            return IN;
        } else if ("OUT".equals(modeName)) {
            return AltibaseParameterMode.OUT;
        } else {
            return AltibaseParameterMode.INOUT;
        }
    }

    public DBSProcedureParameterKind getParameterKind()
    {
        return parameterKind;
    }
}