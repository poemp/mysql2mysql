package org.poem.utils;

import com.google.common.collect.Lists;
import org.poem.vo.EnumDataType;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

import static org.poem.utils.SqlUtils.DEFAULT_CREATE_TIME;

/**
 * @author poem
 */
class SqlHelperHandler {

    /**
     * 创建
     *
     * @param type
     * @return
     */
    private static String getPostgresColType(String type) {
        switch (type.trim().toLowerCase()) {
            case "string":
                return "varchar(500)";
            case "integer":
            case "int":
                return "int4";
            case "double":
            case "float":
                return "numeric(20,2 )";
            case "long":
                return "int8";
            case "timestamp":
            case "date":
                return "timestamp(6)";
            default:
                throw new RuntimeException("【Postgres】没有类型：" + type);
        }
    }

    /**
     * 创建Mysql的数据类型
     *
     * @param type
     * @return
     */
    private static String getMysqlColType(String type) {
        switch (type.trim().toLowerCase()) {
            case "string":
                return "varchar(200)";
            case "integer":
            case "int":
                return "int(20)";
            case "double":
            case "float":
                return "double(20,2 )";
            case "long":
                return "bigint(20)";
            case "timestamp":
            case "date":
                return "timestamp";
            default:
                throw new RuntimeException("【Mysql】没有类型：" + type);
        }
    }

    /**
     * 把java类型转换为 数据库类型
     *
     * @param type         数据的类型
     * @param enumDataType 数据库的类型
     * @return 数据类型
     */
    private static String geColType(String type, EnumDataType enumDataType) {
        if (EnumDataType.POSTGRES.getType().equalsIgnoreCase(enumDataType.getType())) {
            return SqlHelperHandler.getPostgresColType(type);
        }
        return SqlHelperHandler.getMysqlColType(type);
    }


    /**
     * 创建新建表的sql
     *
     * @param tableName    表的名字
     * @param colsDataType 表的列类型关系
     * @return
     */
    static String getCreateMysqlTableSql(String tableName, Map<String, String> colsDataType, Map<String, String> colsIndexKey) {
        StringBuilder sqlStr = new StringBuilder("-- ----------------------------\n");
        sqlStr.append("-- Table structure for ").append(tableName).append("\n");
        sqlStr.append("-- ----------------------------\n");
        sqlStr.append("CREATE TABLE IF NOT EXISTS  `").append(tableName).append("` (").append("\n\t\t");
        List<String> colunmsList = Lists.newArrayList();
        List<String> keyList = Lists.newArrayList();
        colsDataType.forEach(
                (k, v) -> {
                    if ("timestamp".equalsIgnoreCase(String.valueOf(v))) {
                        colunmsList.add("`" + k + "`  timestamp  NULL DEFAULT NULL ");
                    } else {
                        colunmsList.add("`" + k + "`  " + SqlHelperHandler.getMysqlColType(v) + " DEFAULT NULL ");
                    }
                    if (colsIndexKey != null && !CollectionUtils.isEmpty(colsIndexKey)) {
                        if (null != colsIndexKey.get(String.valueOf(v)) && "PRI".equals(String.valueOf(String.valueOf(v)))) {
                            keyList.add(String.valueOf(v));
                        }
                    }
                }
        );

        colunmsList.add("`" + DEFAULT_CREATE_TIME + "`  TIMESTAMP NULL  DEFAULT NULL ");
        if (!CollectionUtils.isEmpty(keyList)) {
            for (String s : keyList) {
                colunmsList.add("KEY `" + s + "` (`" + s + "`)");
            }
        }
        sqlStr.append(String.join(",\n\t\t", colunmsList));
        sqlStr.append(" \n)ENGINE=MyISAM ").append(";").append("\n");
        return sqlStr.toString();
    }


    /**
     * 长安postgres的sql
     *
     * @param tableName    表
     * @param colsDataType 类型
     * @return 创建表的sql
     */
    static String getPostgresTableSql(String tableName, Map<String, String> colsDataType, Map<String, String> colsIndexKey) {
        StringBuilder sqlStr = new StringBuilder("-- ----------------------------\n");
        sqlStr.append("-- Table structure for ").append(tableName).append("\n");
        sqlStr.append("-- ----------------------------\n");
        sqlStr.append("CREATE TABLE IF NOT EXISTS  \"public\".\"").append(tableName).append("\" (").append("\n\t\t");
        List<String> colunmsList = Lists.newArrayList();
        List<String> keyList = Lists.newArrayList();
        colsDataType.forEach(
                (k, v) -> {
                    if ("timestamp".equalsIgnoreCase(String.valueOf(v))) {
                        colunmsList.add("\"" + k + "\"  timestamp(6)");
                    } else {
                        colunmsList.add("\"" + k + "\"  " + geColType(v, EnumDataType.POSTGRES) + " DEFAULT NULL ");
                    }
                    if (colsIndexKey != null && !CollectionUtils.isEmpty(colsIndexKey)) {
                        if (null != colsIndexKey.get(String.valueOf(v)) && "PRI".equals(String.valueOf(String.valueOf(v)))) {
                            keyList.add(String.valueOf(v));
                        }
                    }
                }
        );
        colunmsList.add("\"" + DEFAULT_CREATE_TIME + "\"  timestamp(6) ");
        sqlStr.append(String.join(",\n\t\t", colunmsList));
        sqlStr.append(" \n)").append(";").append("\n");
        List<String> indexStr = Lists.newArrayList();
        if (!CollectionUtils.isEmpty(keyList)) {
            for (String s : keyList) {
                indexStr.add(" CREATE INDEX " + tableName + "_" + s + " ON " + tableName + " (" + s + ");");
            }
        }
        sqlStr.append(String.join("\n", indexStr));
        return sqlStr.toString();
    }

}
