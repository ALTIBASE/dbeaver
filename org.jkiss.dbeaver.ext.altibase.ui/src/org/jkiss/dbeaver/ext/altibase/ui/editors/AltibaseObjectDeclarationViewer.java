package org.jkiss.dbeaver.ext.altibase.ui.editors;

import org.jkiss.dbeaver.model.runtime.DBRProgressMonitor;
import org.jkiss.dbeaver.ui.editors.sql.SQLSourceViewer;

public class AltibaseObjectDeclarationViewer extends SQLSourceViewer {

    @Override
    protected boolean isReadOnly() {
        return true;
    }
    
    @Override
    protected void setSourceText(DBRProgressMonitor monitor, String sourceText) {
        getInputPropertySource().setPropertyValue(
            monitor,
            "Definition",
            sourceText);
    }
}
