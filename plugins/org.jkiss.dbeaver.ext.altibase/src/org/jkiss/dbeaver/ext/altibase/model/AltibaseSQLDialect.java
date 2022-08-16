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

package org.jkiss.dbeaver.ext.altibase.model;

import java.util.Arrays;

import org.jkiss.code.NotNull;
import org.jkiss.code.Nullable;
import org.jkiss.dbeaver.Log;
import org.jkiss.dbeaver.ext.altibase.data.AltibaseBinaryFormatter;
import org.jkiss.dbeaver.model.DBPIdentifierCase;
import org.jkiss.dbeaver.model.DBPKeywordType;
import org.jkiss.dbeaver.model.data.DBDBinaryFormatter;
import org.jkiss.dbeaver.model.exec.DBCLogicalOperator;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCDatabaseMetaData;
import org.jkiss.dbeaver.model.exec.jdbc.JDBCSession;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCDataSource;
import org.jkiss.dbeaver.model.impl.jdbc.JDBCSQLDialect;
import org.jkiss.dbeaver.model.impl.sql.BasicSQLDialect;
import org.jkiss.dbeaver.model.preferences.DBPPreferenceStore;
import org.jkiss.dbeaver.model.sql.SQLConstants;
import org.jkiss.dbeaver.model.sql.SQLExpressionFormatter;
import org.jkiss.dbeaver.model.sql.SQLSyntaxManager;
import org.jkiss.dbeaver.model.sql.parser.SQLParserActionKind;
import org.jkiss.dbeaver.model.sql.parser.SQLRuleManager;
import org.jkiss.dbeaver.model.sql.parser.SQLTokenPredicateSet;
import org.jkiss.dbeaver.model.sql.parser.tokens.SQLTokenType;
import org.jkiss.dbeaver.model.sql.parser.tokens.predicates.TokenPredicateFactory;
import org.jkiss.dbeaver.model.sql.parser.tokens.predicates.TokenPredicateSet;
import org.jkiss.dbeaver.model.sql.parser.tokens.predicates.TokenPredicatesCondition;
import org.jkiss.dbeaver.model.struct.DBSTypedObject;
import org.jkiss.dbeaver.model.struct.rdb.DBSProcedure;
import org.jkiss.utils.ArrayUtils;
import org.jkiss.utils.CommonUtils;

/**
 * Altibase SQL dialect: Referenced OracleSQLDialect
 */
public class AltibaseSQLDialect extends JDBCSQLDialect// implements SQLDataTypeConverter {
{

    private static final Log log = Log.getLog(AltibaseSQLDialect.class);
    
    public static final String ALTIBASE_DIALECT_ID = "altibase";

    private static final String[] EXEC_KEYWORDS = new String[]{ "call", "exec" };

    private static final String[] ALTIBASE_NON_TRANSACTIONAL_KEYWORDS = ArrayUtils.concatArrays(
        BasicSQLDialect.NON_TRANSACTIONAL_KEYWORDS,
        new String[]{
            "CREATE", "ALTER", "DROP",
            "ANALYZE", "VALIDATE",
        }
    );

    private static final String[][] ALTIBASE_BEGIN_END_BLOCK = new String[][]{
        {SQLConstants.BLOCK_BEGIN, SQLConstants.BLOCK_END},
        {"IF", SQLConstants.BLOCK_END},
        {"LOOP", SQLConstants.BLOCK_END + " LOOP"},
        {SQLConstants.KEYWORD_CASE, SQLConstants.BLOCK_END + " " + SQLConstants.KEYWORD_CASE},
    };

    private static final String[] ALTIBASE_BLOCK_HEADERS = new String[]{
        "DECLARE",
        "PACKAGE"
    };

    private static final String[] ALTIBASE_INNER_BLOCK_PREFIXES = new String[]{
        "AS",
        "IS",
    };

    public static final String[] OTHER_TYPES_FUNCTIONS = {
        //functions without parentheses #8710
        "CURRENT_DATE",
        "CURRENT_TIMESTAMP",
        "SYSDATE",
        "SYSTIMESTAMP"
    };

    public static final String[] ADVANCED_KEYWORDS = {
        "REPLACE",
        "PACKAGE",
        "FUNCTION",
        "TYPE",
        "BODY",
        "RECORD",
        "TRIGGER",
        "MATERIALIZED",
        "IF",
        "EACH",
        "RETURN",
        "WRAPPED",
        "AFTER",
        "BEFORE",
        "DATABASE",
        "ANALYZE",
        "VALIDATE",
        "STRUCTURE",
        "COMPUTE",
        "STATISTICS",
        "LOOP",
        "WHILE",
        "BULK",
        "ELSIF",
        "EXIT",
    };
    private boolean crlfBroken;
    private DBPPreferenceStore preferenceStore;

    private SQLTokenPredicateSet cachedDialectSkipTokenPredicates = null;

    public AltibaseSQLDialect() {
        super("Altibase", "altibase");
        setUnquotedIdentCase(DBPIdentifierCase.UPPER);
    }

    public void initDriverSettings(JDBCSession session, JDBCDataSource dataSource, JDBCDatabaseMetaData metaData) {
        super.initDriverSettings(session, dataSource, metaData);
        crlfBroken = !dataSource.isServerVersionAtLeast(6, 0);
        preferenceStore = dataSource.getContainer().getPreferenceStore();

        addFunctions(
            Arrays.asList(
            		// Aggregate functions
            		"AVG",
            		"CORR",
            		"COUNT",
            		"COVAR_POP",
            		"COVAR_SAMP",
            		"CUME_DIST",
            		"FIRST",
            		"GROUP_CONCAT",
            		"LAST",
            		"LISTAGG",
            		"MAX",
            		"MEDIAN",
            		"MIN",
            		"PERCENTILE_CONT",
            		"PERCENTILE_DISC",
            		"PERCENT_RANK",
            		"RANK",
            		"STATS_ONE_WAY_ANOVA",
            		"STDDEV",
            		"STDDEV_POP",
            		"STDDEV_SAMP",
            		"SUM",
            		"VARIANCE",
            		"VAR_POP",
            		"VAR_SAMP",

            		// Windows functions: aggregate
            		"RATIO_TO_REPORT",

            		// Windows functions: Ranking
            		"DENSE_RANK",
            		"LAG",
            		"LAG_IGNORE_NULLS",
            		"LEAD",
            		"LEAD_IGNORE_NULLS",
            		"NTILE",
            		"ROW_NUMBER",

            		// Windows functions: Ordering
            		"FIRST_VALUE",
            		"FIRST_VALUE_IGNORE_NULLS",
            		"LAST_VALUE",
            		"LAST_VALUE_IGNORE_NULLS",
            		"NTH_VALUE",
            		"NTH_VALUE_IGNORE_NULLS",

            		// Number functions
            		"ABS",
            		"ACOS",
            		"ASIN",
            		"ATAN",
            		"ATAN2",
            		"BITAND",
            		"BITNOT",
            		"BITOR",
            		"BITXOR",
            		"CEIL",
            		"COS",
            		"COSH",
            		"EXP",
            		"FLOOR",
            		"ISNUMERIC",
            		"LN",
            		"LOG",
            		"MOD",
            		"NUMAND",
            		"NUMOR",
            		"NUMSHIFT",
            		"NUMXOR",
            		"POWER",
            		"RAND",
            		"RANDOM",
            		"ROUND",
            		"SIGN",
            		"SIN",
            		"SINH",
            		"SQRT",
            		"TAN",
            		"TANH",
            		"TRUNC",

            		// Char functions: return char
            		"CHOSUNG",
            		"CHR",
            		"CONCAT",
            		"DIGITS",
            		"INITCAP",
            		"LOWER",
            		"LPAD",
            		"LTRIM",
            		"NCHR",
            		"PKCS7PAD16",
            		"PKCS7UNPAD16",
            		"RANDOM_STRING",
            		"REGEXP_COUNT",
            		"REGEXP_REPLACE",
            		"REPLACE2",
            		"REPLICATE",
            		"REVERSE_STR",
            		"RPAD",
            		"RTRIM",
            		"STUFF",
            		"SUBSTR",
            		"SUBSTRB",
            		"SUBSTRING",
            		"TRANSLATE",
            		"TRIM",
            		"UPPER",

            		// Number functions: return number
            		"ASCII",
            		"CHARACTER_LENGTH",
            		"CHAR_LENGTH",
            		"DIGEST",
            		"INSTR",
            		"INSTRB",
            		"LENGTH",
            		"LENGTHB",
            		"OCTET_LENGTH",
            		"POSITION",
            		"REGEXP_INSTR",
            		"REGEXP_SUBSTR",
            		"SIZEOF",

            		// Date functions
            		"ADD_MONTHS",
            		"CONV_TIMEZONE",
            		"DATEADD",
            		"DATEDIFF",
            		"DATENAME",
            		"DATEPART",
            		"DB_TIMEZONE",
            		"EXTRACT",
            		"LAST_DAY",
            		"MONTHS_BETWEEN",
            		"NEXT_DAY",
            		"SESSION_TIMEZONE",
            		"UNIX_DATE",
            		"UNIX_TIMESTAMP",

            		// Conversion functions
            		"ASCIISTR",
            		"BIN_TO_NUM",
            		"CONVERT",
            		"DATE_TO_UNIX",
            		"HEX_DECODE",
            		"HEX_ENCODE",
            		"HEX_TO_NUM",
            		"OCT_TO_NUM",
            		"RAW_TO_FLOAT",
            		"RAW_TO_INTEGER",
            		"RAW_TO_NUMERIC",
            		"RAW_TO_VARCHAR",
            		"TO_BIN",
            		"TO_CHAR",
            		"TO_DATE",
            		"TO_HEX",
            		"TO_INTERVAL",
            		"TO_NCHAR",
            		"TO_NUMBER",
            		"TO_OCT",
            		"TO_RAW",
            		"UNISTR",
            		"UNIX_TO_DATE",

            		// Encryption functions
            		"AESDECRYPT",
            		"AESENCRYPT",
            		"DESDECRYPT",
            		"DESENCRYPT",
            		"TDESDECRYPT",
            		"TDESENCRYPT",
            		"TRIPLE_DESDECRYPT",
            		"TRIPLE_DESENCRYPT",

            		// Etc. functions
            		"BASE64_DECODE",
            		"BASE64_DECODE_STR",
            		"BASE64_ENCODE",
            		"BASE64_ENCODE_STR",
            		"BINARY_LENGTH",
            		"CASE WHEN",
            		"CASE2",
            		"COALESCE",
            		"DECODE",
            		"DUMP",
            		"EMPTY_BLOB",
            		"EMPTY_CLOB",
            		"GREATEST",
            		"GROUPING",
            		"GROUPING_ID",
            		"HOST_NAME",
            		"LEAST",
            		"LNNVL",
            		"MSG_CREATE_QUEUE",
            		"MSG_DROP_QUEUE",
            		"MSG_RCV_QUEUE",
            		"MSG_SND_QUEUE",
            		"NULLIF",
            		"NVL",
            		"NVL2",
            		"QUOTE_PRINTABLE_DECODE",
            		"QUOTE_PRINTABLE_ENCODE",
            		"RAW_CONCAT",
            		"RAW_SIZEOF",
            		"ROWNUM",
            		"SENDMSG",
            		"SESSION_ID",
            		"SUBRAW",
            		"SYS_CONNECT_BY_PATH",
            		"SYS_CONTEXT",
            		"SYS_GUID_STR",
            		"USER_ID",
            		"USER_LOCK_RELEASE",
            		"USER_LOCK_REQUEST",
            		"USER_NAME"
            ));
        removeSQLKeyword("SYSTEM");

        for (String kw : ADVANCED_KEYWORDS) {
            addSQLKeyword(kw);
        }

        addKeywords(Arrays.asList(OTHER_TYPES_FUNCTIONS), DBPKeywordType.OTHER);
        turnFunctionIntoKeyword("TRUNCATE");

        cachedDialectSkipTokenPredicates = makeDialectSkipTokenPredicates(dataSource);
    }

    @Override
    protected void loadDataTypesFromDatabase(JDBCDataSource dataSource) {
        super.loadDataTypesFromDatabase(dataSource);
        //addDataTypes(OracleDataType.PREDEFINED_TYPES.keySet());
    }

    @Override
    public String[][] getBlockBoundStrings() {
        return ALTIBASE_BEGIN_END_BLOCK;
    }

    @Override
    public String[] getBlockHeaderStrings() {
        return ALTIBASE_BLOCK_HEADERS;
    }

    @Nullable
    @Override
    public String[] getInnerBlockPrefixes() {
        return ALTIBASE_INNER_BLOCK_PREFIXES;
    }

    @NotNull
    @Override
    public String[] getExecuteKeywords() {
        return EXEC_KEYWORDS;
    }

    @NotNull
    @Override
    public MultiValueInsertMode getDefaultMultiValueInsertMode() {
        return MultiValueInsertMode.INSERT_ALL;
    }

    @Override
    public String getLikeEscapeClause(@NotNull String escapeChar) {
        return " ESCAPE " + getQuotedString(escapeChar);
    }

    @NotNull
    @Override
    public String escapeScriptValue(DBSTypedObject attribute, @NotNull Object value, @NotNull String strValue) {
        if (CommonUtils.isNaN(value) || CommonUtils.isInfinite(value)) {
            // These special values should be quoted, as shown in the example below
            // https://docs.oracle.com/cd/B19306_01/server.102/b14200/functions090.htm
            return '\'' + String.valueOf(value) + '\'';
        }
        return super.escapeScriptValue(attribute, value, strValue);
    }

    @Override
    public boolean supportsAliasInSelect() {
        return true;
    }

    @Override
    public boolean supportsAliasInUpdate() {
        return true;
    }

    @Override
    public boolean supportsTableDropCascade() {
        return true;
    }

    @Nullable
    @Override
    public SQLExpressionFormatter getCaseInsensitiveExpressionFormatter(@NotNull DBCLogicalOperator operator) {
        if (operator == DBCLogicalOperator.LIKE) {
            return (left, right) -> "UPPER(" + left + ") LIKE UPPER(" + right + ")";
        }
        return super.getCaseInsensitiveExpressionFormatter(operator);
    }

    @Override
    public boolean isDelimiterAfterBlock() {
        return true;
    }

    @NotNull
    @Override
    public DBDBinaryFormatter getNativeBinaryFormatter() {
        return AltibaseBinaryFormatter.INSTANCE;
    }

    @Nullable
    @Override
    public String getDualTableName() {
        return "DUAL";
    }

    @NotNull
    @Override
    public String[] getNonTransactionKeywords() {
        return ALTIBASE_NON_TRANSACTIONAL_KEYWORDS;
    }

    @Override
    protected String getStoredProcedureCallInitialClause(DBSProcedure proc) {
        String schemaName = proc.getParentObject().getName();
        return "CALL " + schemaName + "." + proc.getName();
    }

    @Override
    public boolean isDisableScriptEscapeProcessing() {
        return preferenceStore == null;
    }

    @NotNull
    @Override
    public String[] getScriptDelimiters() {
        return super.getScriptDelimiters();
    }

    @Override
    public boolean isCRLFBroken() {
        return crlfBroken;
    }

    /*
    @Override
    public String getColumnTypeModifiers(@NotNull DBPDataSource dataSource, @NotNull DBSTypedObject column, @NotNull String typeName, @NotNull DBPDataKind dataKind) {
        Integer scale;
        switch (typeName) {
            case OracleConstants.TYPE_NUMBER:
            case OracleConstants.TYPE_DECIMAL:
                DBSDataType dataType = DBUtils.getDataType(column);
                scale = column.getScale();
                int precision = CommonUtils.toInt(column.getPrecision());
                if (precision == 0 && dataType != null && scale != null && scale == dataType.getMinScale()) {
                    return "";
                }
                if (precision == 0 || precision > OracleConstants.NUMERIC_MAX_PRECISION) {
                    precision = OracleConstants.NUMERIC_MAX_PRECISION;
                }
                if (scale != null && precision > 0) {
                    return "(" + precision + ',' + scale + ")";
                }
                break;
            case OracleConstants.TYPE_INTERVAL_DAY_SECOND:
                // This interval type has fractional seconds precision. In bounds from 0 to 9. We can show this parameter.
                // FIXME: This type has day precision inside type name. Like INTERVAL DAY(2) TO SECOND(6). So far we can't show it (But we do it in Column Manager)
                scale = column.getScale();
                if (scale == null) {
                    return "";
                }
                if (scale < 0 || scale > 9) {
                    scale = OracleConstants.INTERVAL_DEFAULT_SECONDS_PRECISION;
                }
                return "(" + scale + ")";
            case OracleConstants.TYPE_NAME_BFILE:
            case OracleConstants.TYPE_NAME_CFILE:
            case OracleConstants.TYPE_CONTENT_POINTER:
            case OracleConstants.TYPE_LONG:
            case OracleConstants.TYPE_LONG_RAW:
            case OracleConstants.TYPE_OCTET:
            case OracleConstants.TYPE_INTERVAL_YEAR_MONTH:
                // Don't add modifiers to these types
                return "";
        }
        return super.getColumnTypeModifiers(dataSource, column, typeName, dataKind);
    }

    @Override
    public String convertExternalDataType(@NotNull SQLDialect sourceDialect, @NotNull DBSTypedObject sourceTypedObject, @Nullable DBPDataTypeProvider targetTypeProvider) {
        String type = super.convertExternalDataType(sourceDialect, sourceTypedObject, targetTypeProvider);
        if (type != null) {
            return type;
        }
        String externalTypeName = sourceTypedObject.getTypeName().toUpperCase(Locale.ENGLISH);
        String localDataType = null, dataTypeModifies = null;

        switch (externalTypeName) {
            case "VARCHAR":
                //We don't want to use a VARCHAR it's not recommended
                //See https://docs.oracle.com/en/database/oracle/oracle-database/19/sqlrf/Data-Types.html#GUID-DF7E10FC-A461-4325-A295-3FD4D150809E
                localDataType = OracleConstants.TYPE_NAME_VARCHAR2;
                if (sourceTypedObject.getMaxLength() > 0
                    && sourceTypedObject.getMaxLength() != Integer.MAX_VALUE
                    && sourceTypedObject.getMaxLength() != Long.MAX_VALUE) {
                    dataTypeModifies = String.valueOf(sourceTypedObject.getMaxLength());
                }
                break;
            case "XML":
            case "XMLTYPE":
                localDataType = OracleConstants.TYPE_FQ_XML;
                break;
            case "JSON":
            case "JSONB":
                localDataType = "JSON";
                break;
            case "GEOMETRY":
            case "GEOGRAPHY":
            case "SDO_GEOMETRY":
                localDataType = OracleConstants.TYPE_FQ_GEOMETRY;
                break;
            case "NUMERIC":
                localDataType = OracleConstants.TYPE_NUMBER;
                if (sourceTypedObject.getPrecision() != null) {
                    dataTypeModifies = sourceTypedObject.getPrecision().toString();
                    if (sourceTypedObject.getScale() != null) {
                        dataTypeModifies += "," + sourceTypedObject.getScale();
                    }
                }
                break;
        }
        if (localDataType == null) {
            return null;
        }
        if (targetTypeProvider != null) {
            try {
                DBSDataType dataType = targetTypeProvider.resolveDataType(new VoidProgressMonitor(), localDataType);
                if (dataType == null) {
                    return null;

                }
                String targetTypeName = DBUtils.getObjectFullName(dataType, DBPEvaluationContext.DDL);
                if (dataTypeModifies != null) {
                    targetTypeName += "(" + dataTypeModifies + ")";
                }
                return targetTypeName;
            } catch (DBException e) {
                log.debug("Error resolving local data type", e);
                return null;
            }
        }
        return localDataType;
    }
     */
    
    @Override
    @NotNull
    public SQLTokenPredicateSet getSkipTokenPredicates() {
        return cachedDialectSkipTokenPredicates == null ? super.getSkipTokenPredicates() : cachedDialectSkipTokenPredicates;
    }

    @NotNull
    private SQLTokenPredicateSet makeDialectSkipTokenPredicates(JDBCDataSource dataSource) {
        SQLSyntaxManager syntaxManager = new SQLSyntaxManager();
        syntaxManager.init(this, dataSource.getContainer().getPreferenceStore());
        SQLRuleManager ruleManager = new SQLRuleManager(syntaxManager);
        ruleManager.loadRules(dataSource, false);
        TokenPredicateFactory tt = TokenPredicateFactory.makeDialectSpecificFactory(ruleManager);

        // Oracle SQL references could be found from https://docs.oracle.com/en/database/oracle/oracle-database/
        // by following through Get Started links till the SQL Language Reference link presented

        TokenPredicateSet conditions = TokenPredicateSet.of(
                // https://docs.oracle.com/en/database/oracle/oracle-database/12.2/lnpls/CREATE-PACKAGE-BODY-statement.html#GUID-68526FF2-96A1-4F14-A10B-4DD3E1CD80BE
                // also presented in the earliest found reference on 7.3, so considered as always supported https://docs.oracle.com/pdf/A32538_1.pdf
                new TokenPredicatesCondition(
                        SQLParserActionKind.BEGIN_BLOCK,
                        tt.sequence(
                                "CREATE",
                                tt.optional("OR", "REPLACE"),
                                "PACKAGE", "BODY"
                        ),
                        tt.sequence()
                ),
                // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/CREATE-FUNCTION.html#GUID-156AEDAC-ADD0-4E46-AA56-6D1F7CA63306
                // https://docs.oracle.com/en/database/oracle/oracle-database/21/sqlrf/CREATE-PROCEDURE.html#GUID-771879D8-BBFD-4D87-8A6C-290102142DA3
                // not fully described, only some cases partially discovered
                new TokenPredicatesCondition(
                        SQLParserActionKind.SKIP_SUFFIX_TERM,
                        tt.sequence(
                                "CREATE",
                                tt.optional("OR", "REPLACE"),
                                tt.alternative("FUNCTION", "PROCEDURE")
                        ),
                        tt.sequence(tt.alternative(
                                tt.sequence("RETURN", SQLTokenType.T_TYPE),
                                "deterministor", "pipelined", "parallel_enable", "result_cache",
                                ")",
                                tt.sequence("procedure", SQLTokenType.T_OTHER),
                                tt.sequence(SQLTokenType.T_OTHER, SQLTokenType.T_TYPE)
                        ), ";")
                )
        );

        return conditions;
    }

    @Override
    public boolean hasCaseSensitiveFiltration() {
        return true;
    }

    @Override
    public boolean supportsAliasInConditions() {
        return false;
    }
}