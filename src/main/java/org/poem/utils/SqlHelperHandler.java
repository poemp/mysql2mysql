package org.poem.utils;

import com.google.common.collect.Lists;
import org.poem.vo.EnumDataType;
import org.springframework.util.CollectionUtils;

import java.util.List;
import java.util.Map;

import static com.alibaba.druid.util.FnvHash.Constants.MAX_SIZE;
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
            case "varchar":
            case "longtext":
                return "varchar(500)";
            case "integer":
            case "int":
                return "int4";
            case "double":
            case "float":
            case "decimal":
                return "numeric(20,2 )";
            case "long":
            case "bigint":
            case "serial":
                return "int8";
            case "timestamp":
            case "date":
                return "timestamp(6)";
            case "bit":
                return "bit";
            case "text":
                return "text";
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
            case "varchar":
                return "varchar(200)";
            case "integer":
            case "int":
                return "int(20)";
            case "double":
            case "float":
                return "double(20,2 )";
            case "long":
            case "bigint":
                return "bigint(20)";
            case "timestamp":
            case "date":
                return "timestamp";
            case "bit":
                return "bit";
            case "text":
                return "text";
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
        if (!colsDataType.containsKey(DEFAULT_CREATE_TIME)) {
            colunmsList.add("`" + DEFAULT_CREATE_TIME + "`  TIMESTAMP NULL  DEFAULT NULL ");
        }

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
        if (!colsDataType.containsKey(DEFAULT_CREATE_TIME)) {
            colunmsList.add("\"" + DEFAULT_CREATE_TIME + "\"  timestamp(6) ");
        }
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


    /**
     * 创建mysql的插入sql
     *
     * @param table     表
     * @param metadata  数据列
     * @param columns   列
     * @param typeZip   类型
     * @param currentNo 当前的版本
     * @return
     */
    static List<String> createMysqlInsertSql(String table,
                                             List<Map<String, Object>> metadata,
                                             List<String> columns,
                                             Map<String, String> typeZip,
                                             String currentNo) {
        List<String> insertLists = Lists.newArrayList();
        int sequence = 0;
        Map<String, Object> metadatum;
        StringBuilder sql = new StringBuilder();
        if (!columns.contains(SqlUtils.DEFAULT_CREATE_TIME)) {
            columns.add(SqlUtils.DEFAULT_CREATE_TIME);
        }
        for (int index = 0; index < metadata.size(); index++) {
            metadatum = metadata.get(index);
            if (metadatum == null) {
                continue;
            }
            if (columns.contains(DEFAULT_CREATE_TIME)) {
                metadatum.put(SqlUtils.DEFAULT_CREATE_TIME, currentNo);
            }

            if (index % (MAX_SIZE / 2) == 0) {
                if (index != 0) {
                    sql.replace(sql.lastIndexOf(","), sql.lastIndexOf(",") + 1, "");
                    insertLists.add(sql.toString());
                }
                sql = new StringBuilder().append(" INSERT  INTO `").append(table).append("` (`")
                        .append(String.join("`,`", columns)).append("`").append(")")
                        .append(" VALUES  ");
                sequence = 0;
            }
            sql.append("(");
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                String type = typeZip.get(column);
                Object data = metadatum.get(column);
                if ("Long".equalsIgnoreCase(type)
                        || "Integer".equalsIgnoreCase(type)
                        || "Float".equalsIgnoreCase(type)
                        || "Double".equalsIgnoreCase(type)
                        || "BigDecimal".equalsIgnoreCase(type)

                ) {
                    if ("".equals(data) || data == null) {
                        data = 0;
                    }
                    sql.append(data);
                } else if ("tinyint".equalsIgnoreCase(type)) {
                    //bool类型
                    sql.append(data);
                } else {
                    //去掉分号
                    if (data instanceof String) {
                        if ("".equals(data) || "null".equals(data)) {
                            sql.append("null");
                        } else {
                            String item = ((String) data).replaceAll(";", "，").replaceAll("'", " ");
                            if (item.endsWith("\\")) {
                                item = item.replaceAll("\\\\", "\\\\\\\\");
                            }
                            sql.append("'").append(item).append("'");
                        }
                    } else {
                        if (data == null || "".equals(data) || "null".equals(data)) {
                            sql.append("null");
                        } else {
                            sql.append("'").append(data).append("'");
                        }
                    }
                }
                if (i != columns.size() - 1) {
                    sql.append(",");
                }
            }
            sql.append(")");
            if (sequence == 0 || index % (MAX_SIZE / 2) != 0) {
                sql.append(",");
            }
            sequence++;
        }
        if (sql.lastIndexOf(",") != -1) {
            sql.replace(sql.lastIndexOf(","), sql.lastIndexOf(",") + 1, "");
        }
        insertLists.add(sql.toString());
        return insertLists;
    }


    /**
     * 创建mysql的插入sql
     *
     * @param table     表
     * @param metadata  数据列
     * @param columns   列
     * @param typeZip   类型
     * @param currentNo 当前的版本
     * @return
     */
    static List<String> createPostInsertSql(String table,
                                            List<Map<String, Object>> metadata,
                                            List<String> columns,
                                            Map<String, String> typeZip,
                                            String currentNo) {
        List<String> insertLists = Lists.newArrayList();
        int sequence = 0;
        Map<String, Object> metadatum;
        StringBuilder sql = new StringBuilder();
        if (!columns.contains(SqlUtils.DEFAULT_CREATE_TIME)) {
            columns.add(SqlUtils.DEFAULT_CREATE_TIME);
        }
        for (int index = 0; index < metadata.size(); index++) {
            metadatum = metadata.get(index);
            if (metadatum == null) {
                continue;
            }
            metadatum.put(SqlUtils.DEFAULT_CREATE_TIME, currentNo);
            if (index % (MAX_SIZE / 2) == 0) {
                if (index != 0) {
                    sql.replace(sql.lastIndexOf(","), sql.lastIndexOf(",") + 1, "");
                    insertLists.add(sql.toString());
                }
                sql = new StringBuilder().append(" INSERT  INTO public.").append(table).append(" (\"")
                        .append(String.join("\",\"", columns)).append("\")")
                        .append(" VALUES  ");
                sequence = 0;
            }
            sql.append("(");
            for (int i = 0; i < columns.size(); i++) {
                String column = columns.get(i);
                String type = typeZip.get(column);
                Object data = metadatum.get(column);
                if ("Long".equalsIgnoreCase(type)
                        || "Integer".equalsIgnoreCase(type)
                        || "Float".equalsIgnoreCase(type)
                        || "Double".equalsIgnoreCase(type)
                        || "BigDecimal".equalsIgnoreCase(type)

                ) {
                    if ("".equals(data) || data == null) {
                        data = 0;
                    }
                    sql.append(data);
                } else if ("tinyint".equalsIgnoreCase(type)) {
                    //bool类型
                    sql.append(data);
                } else {
                    //去掉分号
                    if (data instanceof String) {
                        if ("".equals(data) || "null".equals(data)) {
                            sql.append("null");
                        } else {
                            String item = ((String) data).replaceAll(";", "，").replaceAll("'", " ");
                            if (item.endsWith("\\")) {
                                item = item.replaceAll("\\\\", "\\\\\\\\");
                            }
                            sql.append("'").append(item).append("'");
                        }
                    } else {
                        if (data == null || "".equals(data) || "null".equals(data)) {
                            sql.append("null");
                        } else {
                            sql.append("'").append(data).append("'");
                        }
                    }
                }
                if (i != columns.size() - 1) {
                    sql.append(",");
                }
            }
            sql.append(")");
            if (sequence == 0 || index % (MAX_SIZE / 2) != 0) {
                sql.append(",");
            }
            sequence++;
        }
        if (sql.lastIndexOf(",") != -1) {
            sql.replace(sql.lastIndexOf(","), sql.lastIndexOf(",") + 1, "");
        }
        insertLists.add(sql.toString());
        return insertLists;
    }
}
