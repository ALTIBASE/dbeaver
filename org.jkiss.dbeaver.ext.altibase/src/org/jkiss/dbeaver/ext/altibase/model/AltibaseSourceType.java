package org.jkiss.dbeaver.ext.altibase.model;

public enum AltibaseSourceType {
	
    TYPE(false),
    PROCEDURE(false),
    FUNCTION(false),
    PACKAGE(false),
    TRIGGER(false),
    VIEW(true),
    MATERIALIZED_VIEW(true),
    JOB(false),
    SEQUENCE(false);

    private final boolean isCustom;

    AltibaseSourceType(boolean custom)
    {
        isCustom = custom;
    }

    public boolean isCustom()
    {
        return isCustom;
    }
}
