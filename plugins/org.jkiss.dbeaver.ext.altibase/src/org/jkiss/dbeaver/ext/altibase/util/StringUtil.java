package org.jkiss.dbeaver.ext.altibase.util;

import org.jkiss.utils.StandardConstants;

public class StringUtil {
	public static final String NEW_LINE = System.getProperty(StandardConstants.ENV_LINE_SEPARATOR);
	
	public static boolean isEmpty(String aValue) {
		if (aValue != null)
			return (aValue.length() < 1);
		else
			return true;
	}
}
