package org.jkiss.dbeaver.ext.altibase.ui.editors;

import org.jkiss.dbeaver.ui.editors.sql.SQLSourceViewer;

public class AltibaseObjectDeclarationViewer extends SQLSourceViewer {
    public AltibaseObjectDeclarationViewer() {
    }

    @Override
    protected boolean isReadOnly() {
        return true;
    }
}
