package org.poem.mysql2mysql;

import com.google.common.collect.Lists;
import org.poem.config.ContextDatabase;
import org.poem.utils.DataSourceDriverHelper;
import org.poem.utils.SqlUtils;
import org.poem.vo.DataTransformTaskVO;
import org.poem.vo.EnumDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.util.CollectionUtils;

import java.sql.Date;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author poem
 */
public class MysqlTransformOutComponent {

    private static final Logger logger = LoggerFactory.getLogger(MysqlTransformOutComponent.class);

    private static final long MAX_SIZE = 10000;


    private static ExecutorService threadPoolExecutor;

    static {
        int core = Runtime.getRuntime().availableProcessors();
        AtomicInteger integer = new AtomicInteger(0);
        threadPoolExecutor = new ThreadPoolExecutor(core, core * 2, 1000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(core),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        //线程命名
                        return new Thread(r, "mysql-insert-" + integer.incrementAndGet());
                    }
                }, new ThreadPoolExecutor.CallerRunsPolicy());
    }

    /**
     * 开始导数据了
     *
     * @param dataTransformVO
     */
    public void importData(DataTransformTaskVO dataTransformVO, JdbcTemplate sourceJdbc, JdbcTemplate targetJdbc) throws SQLException {
        createTable(dataTransformVO, sourceJdbc, targetJdbc);
        exportData(dataTransformVO, sourceJdbc, targetJdbc);
    }



    /**
     * 创建表
     *
     * @param dataTransformVO
     */
    private void createTable(DataTransformTaskVO dataTransformVO, JdbcTemplate sourceJdbc, JdbcTemplate targetJdbc) {
        //来源表
        String schema = ContextDatabase.getSourceCatalog();
        List<Map<String, Object>> colums =
                sourceJdbc.queryForList("SELECT COLUMN_NAME,DATA_TYPE,IS_NULLABLE, COLUMN_TYPE,COLUMN_KEY FROM information_schema.COLUMNS WHERE TABLE_NAME='"
                        + dataTransformVO.getTable() + "' AND  TABLE_SCHEMA = '" + schema + "'");

        List<String> colunmsType = Lists.newArrayList();
        List<String> columnNames = Lists.newArrayList();
        List<String> keyList = Lists.newArrayList();
        for (Map<String, Object> colum : colums) {
            columnNames.add(colum.get("COLUMN_NAME")+"");
            colunmsType.add(colum.get("DATA_TYPE") + "");
            if (null != colum.get("COLUMN_KEY") && "PRI".equals(String.valueOf(colum.get("COLUMN_KEY")))) {
                keyList.add(String.valueOf(colum.get("COLUMN_NAME")));
            }
        }
        Map<String, String> zipColumnTypes = SqlUtils.zipColumnTypes(columnNames, colunmsType);
        Map<String, String> zipColumnKey = SqlUtils.zipColumnTypes(columnNames, keyList);
        String createTableSql =  SqlUtils.getCreateTableSql(dataTransformVO.getTable(),zipColumnTypes,zipColumnKey,ContextDatabase.getTargetSchema());

        //目标
        targetJdbc.execute("drop table if exists " + dataTransformVO.getTable());
        if (logger.isDebugEnabled()) {
            logger.debug("[" + dataTransformVO.getTable() + "] ======================================= ");
            logger.debug("[" + dataTransformVO.getTable() + "]\n" + createTableSql);
            logger.debug("[" + dataTransformVO.getTable() + "] ======================================= ");
        }
        targetJdbc.update(createTableSql);
    }




    /**
     * 导数据
     *
     * @param dataTransformVO
     */
    private void exportData(DataTransformTaskVO dataTransformVO, JdbcTemplate sourceJdbc, JdbcTemplate targetJdbc) throws SQLException {
        //来源表
        long dataSize = sourceJdbc.queryForObject("SELECT sum(1) as su FROM " + dataTransformVO.getTable(), Long.class);
        String schema = ContextDatabase.getSourceCatalog();
        List<Map<String, Object>> colums = sourceJdbc.queryForList("select COLUMN_NAME,DATA_TYPE,IS_NULLABLE from information_schema.COLUMNS where table_name='" + dataTransformVO.getTable() + "' and  TABLE_SCHEMA = '" + schema + "'");
        List<String> colnums = Lists.newArrayList();
        List<String> types = Lists.newArrayList();
        List<String> isNull = Lists.newArrayList();
        for (Map<String, Object> colum : colums) {
            colnums.add(String.valueOf(colum.get("COLUMN_NAME")));
            types.add(String.valueOf(colum.get("DATA_TYPE")));
            isNull.add(String.valueOf(colum.get("IS_NULLABLE")));
        }

        if (logger.isDebugEnabled()) {
            logger.debug("[" + dataTransformVO.getTable() + "]Import Data ：" + dataSize);
        }
        long index = (int) (dataSize / MAX_SIZE) + 1;
        for (long i = 0; i < index; i++) {
            long in = i;
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    long start = (in) * MAX_SIZE;
                    logger.info("[" + dataTransformVO.getTable() + "] SELECT * FROM " + dataTransformVO.getTable() + " LIMIT " + start + " , " + MAX_SIZE);
                    List<Map<String, Object>> rs = sourceJdbc.queryForList("SELECT * FROM " + dataTransformVO.getTable() + " LIMIT " + start + " , " + MAX_SIZE);
                    List<String> insertSql = createMysqlInsertSql(dataTransformVO.getTable(), rs, colnums, types, isNull);
                    batchInsertMysql(insertSql, targetJdbc, dataTransformVO);
                }
            });
        }
    }

    /**
     * 创建插入的sql
     *
     * @param table
     * @param metadata
     * @param colnums
     * @param types
     * @return
     */
    private List<String> createMysqlInsertSql(String table, List<Map<String, Object>> metadata, List<String> colnums, List<String> types, List<String> isNull) {
        List<String> insertLists = Lists.newArrayList();
        Map<String, String> typeZip = DataSourceDriverHelper.zipColunmTypes(colnums, types);
        Map<String, Object> metadatum;
        StringBuilder sql = new StringBuilder();
        int sequence = 0;
        for (int index = 0; index < metadata.size(); index++) {
            metadatum = metadata.get(index);
            if (index % (MAX_SIZE / 2) == 0) {
                if (index != 0) {
                    sql.replace(sql.lastIndexOf(","), sql.lastIndexOf(",") + 1, "");
                    insertLists.add(sql.toString());
                }
                sql = new StringBuilder().append(" INSERT  INTO `").append(table).append("` (`")
                        .append(String.join("`,`", colnums)).append("`").append(")")
                        .append(" VALUES  ");
                sequence = 0;
            }
            sql.append("(");
            for (int i = 0; i < colnums.size(); i++) {
                String colnum = colnums.get(i);
                String type = typeZip.get(colnum);
                Object data = metadatum.get(colnum);
                String nullEnable = isNull.get(i);
                if (data == null && "NO".equalsIgnoreCase(nullEnable)) {
                    if ("datetime".equalsIgnoreCase(type)) {
                        data = new Date(System.currentTimeMillis());
                    } else {
                        data = "";
                    }
                } else if ("null".equals(data) && "NO".equalsIgnoreCase(nullEnable)) {
                    data = "";
                }
                if ("Long".equalsIgnoreCase(type)
                        || "Integer".equalsIgnoreCase(type)
                        || "Float".equalsIgnoreCase(type)
                        || "Double".equalsIgnoreCase(type)
                        || "BigDecimal".equalsIgnoreCase(type)
                        || "int".equalsIgnoreCase(type)
                        || "bit".equalsIgnoreCase(type)
                ) {
                    if ("".equals(data) || data == null) {
                        data = 0;
                    } else if ("bit".equalsIgnoreCase(type)) {
                        data = ((byte[]) data)[0];
                    }
                    sql.append(data);
                } else if ("tinyint".equalsIgnoreCase(type)) {
                    //bool类型
                    sql.append(data);
                } else {
                    //去掉分号
                    if (data instanceof String) {
                        if ("null".equals(data)) {
                            sql.append("null");
                        } else {
                            String item = ((String) data).replaceAll(";", "，").replaceAll("'", " ");
                            if (item.endsWith("\\")) {
                                item = item.replaceAll("\\\\", "\\\\\\\\");
                            }
                            sql.append("'");
                            sql.append(item);
                            sql.append("'");
                        }
                    } else {
                        if (data == null || "".equals(data) || "null".equals(data)) {
                            sql.append("null");
                        } else {
                            sql.append("'");
                            sql.append(data);
                            sql.append("'");

                        }
                    }
                }
                if (i != colnums.size() - 1) {
                    sql.append(",");
                }
            }
            sql.append(")");
            if (sequence == 0 || index % (MAX_SIZE / 2) != 0) {
                sql.append(",");
            }
            sequence++;
        }
        sql.replace(sql.lastIndexOf(","), sql.lastIndexOf(",") + 1, "");
        insertLists.add(sql.toString());

        return insertLists;
    }

    /**
     * 批量插入的mysql中
     *
     * @param insertSqls
     * @param targetJdbc
     */
    private void batchInsertMysql(List<String> insertSqls, JdbcTemplate targetJdbc, DataTransformTaskVO dataTransformVO) {
        insertSqls.parallelStream().forEach(
                targetJdbc::update
        );
        logger.info("[{}] Success Sql Is {}", dataTransformVO.getTable(), insertSqls.size());
    }
}
