package org.poem.utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.poem.vo.EnumDataType;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * @author sangfor
 */
public class SqlUtils {


    private static final Integer MAX_SIZE = 10000;


    /**
     * 公共的数据列
     */
    public static final String DEFAULT_CREATE_TIME = "_CREATE_TIME_";

    /**
     * 生成插入的sql
     *
     * @param table     需要插入的表
     * @param metadata  查询的数据
     * @param columns   列
     * @param typeZip   数据类型
     * @param currentNo 当前的时间戳
     * @return
     */
    public static List<String> createMysqlInsertSql(String table,
                                                    List<Map<String, Object>> metadata,
                                                    List<String> columns,
                                                    Map<String, String> typeZip,
                                                    String currentNo) {
        return createInsertSql(table, metadata, columns, typeZip, currentNo, EnumDataType.MYSQL);
    }

    /**
     * 生成插入的sql
     *
     * @param table        需要插入的表
     * @param metadata     查询的数据
     * @param columns      列
     * @param typeZip      数据类型
     * @param enumDataType 数据库的类型
     * @return
     */
    public static List<String> createInsertSql(String table,
                                               List<Map<String, Object>> metadata,
                                               List<String> columns,
                                               Map<String, String> typeZip,
                                               EnumDataType enumDataType) {
        return createInsertSql(table, metadata, columns, typeZip, DateUtils.formatDateTime(new Date()), enumDataType);
    }

    /**
     * 生成插入的sql
     *
     * @param table        需要插入的表
     * @param metadata     查询的数据
     * @param columns      列
     * @param typeZip      数据类型
     * @param currentNo    当前的时间戳
     * @param enumDataType 数据库的类型
     * @return
     */
    public static List<String> createInsertSql(String table,
                                               List<Map<String, Object>> metadata,
                                               List<String> columns,
                                               Map<String, String> typeZip,
                                               String currentNo,
                                               EnumDataType enumDataType) {
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
     * 压缩数据
     *
     * @param columns
     * @param columnsType
     * @return
     */
    public static Map<String, String> zipColumnTypes(List<String> columns, List<String> columnsType) {
        Map<String, String> map = Maps.newHashMap();
        if (columns.size() != columnsType.size()) {
            return map;
        }
        for (int i = 0; i < columns.size(); i++) {
            map.put(columns.get(i), columnsType.get(i));
        }
        return map;
    }


    /**
     * 创建表
     *
     * @param tableName    表面
     * @param colsDataType 数据库的列的类型映射关系
     * @return
     */
    public static String getCreateTableSql(String tableName, Map<String, String> colsDataType) {
        return SqlHelperHandler.getCreateMysqlTableSql(tableName, colsDataType, null);
    }


    /**
     * 创建数据表的sql
     *
     * @param tableName    表
     * @param colsDataType 列名字和列的类型的映射关系
     * @param enumDataType 创建表的数据库类型
     * @return
     */
    public static String getCreateTableSql(String tableName, Map<String, String> colsDataType, EnumDataType enumDataType) {
        return getCreateTableSql(tableName, colsDataType, null, enumDataType);
    }

    /**
     * 创建插入表格的sql
     *
     * @param tableName    表名字
     * @param colsDataType 列名字和列的类型的映射关系
     * @param colsIndexKey 列名字与列的索引的映射管理
     * @param enumDataType 创建表的数据库类型
     * @return 创建的sql
     */
    public static String getCreateTableSql(String tableName, Map<String, String> colsDataType, Map<String, String> colsIndexKey, EnumDataType enumDataType) {
        if (EnumDataType.MYSQL.getType().equalsIgnoreCase(enumDataType.getType())) {
            return SqlHelperHandler.getCreateMysqlTableSql(tableName, colsDataType, colsIndexKey);
        } else if (EnumDataType.POSTGRES.getType().equalsIgnoreCase(enumDataType.getType())) {
            return SqlHelperHandler.getPostgresTableSql(tableName, colsDataType, colsIndexKey);
        }
        return getCreateTableSql(tableName, colsDataType);
    }

    /**
     * 删除原来的数据
     *
     * @param table       表
     * @param holdTimeOut 删除之前的数据的时间
     * @return
     */
    public static String deleteOldSql(String table, Integer holdTimeOut) {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.HOUR_OF_DAY, -holdTimeOut);
        String yesterday = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(cal.getTime());
        return "DELETE FROM " + table + " WHERE " + SqlUtils.DEFAULT_CREATE_TIME + " <= '" + yesterday + "'";
    }
}
