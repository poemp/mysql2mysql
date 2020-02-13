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
            columnNames.add(colum.get("COLUMN_NAME") + "");
            colunmsType.add(colum.get("DATA_TYPE") + "");
            if (null != colum.get("COLUMN_KEY") && "PRI".equals(String.valueOf(colum.get("COLUMN_KEY")))) {
                keyList.add(String.valueOf(colum.get("COLUMN_NAME")));
            }
        }
        Map<String, String> zipColumnTypes = SqlUtils.zipColumnTypes(columnNames, colunmsType);
        Map<String, String> zipColumnKey = SqlUtils.zipColumnTypes(columnNames, keyList);
        String createTableSql = SqlUtils.getCreateTableSql(dataTransformVO.getTable(),
                zipColumnTypes, zipColumnKey, ContextDatabase.getTargetSchema());

        //目标
        targetJdbc.execute("drop table if exists " + dataTransformVO.getTable());
        if (logger.isDebugEnabled()) {
            logger.info("[" + dataTransformVO.getTable() + "] ======================================= ");
            logger.info("[" + dataTransformVO.getTable() + "]\n" + createTableSql);
            logger.info("[" + dataTransformVO.getTable() + "] ======================================= ");
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
        Long sum = sourceJdbc.queryForObject("SELECT sum(1) as su FROM " + dataTransformVO.getTable(), Long.class);
        long dataSize = sum == null ? 0 : sum;
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
        Map<String, String> zipColumnTypes = SqlUtils.zipColumnTypes(colnums, types);
        long index = (int) (dataSize / MAX_SIZE) + 1;
        for (long i = 0; i < index; i++) {
            long in = i;
            threadPoolExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    long start = (in) * MAX_SIZE;
                    logger.info("[" + dataTransformVO.getTable() + "] SELECT * FROM " + dataTransformVO.getTable() + " LIMIT " + start + " , " + MAX_SIZE);
                    List<Map<String, Object>> rs = sourceJdbc.queryForList("SELECT * FROM " + dataTransformVO.getTable() + " LIMIT " + start + " , " + MAX_SIZE);
                    List<String> insertSql = createMysqlInsertSql(dataTransformVO.getTable(), rs, colnums, zipColumnTypes);
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
     * @param zipColumnTypes
     * @return
     */
    private List<String> createMysqlInsertSql(String table, List<Map<String, Object>> metadata, List<String> colnums,
                                              Map<String, String> zipColumnTypes) {
        return SqlUtils.createInsertSql(table, metadata, colnums, zipColumnTypes, ContextDatabase.getTargetSchema());
    }

    /**
     * 批量插入的mysql中
     *
     * @param insertSqls
     * @param targetJdbc
     */
    private void batchInsertMysql(List<String> insertSqls, JdbcTemplate targetJdbc, DataTransformTaskVO dataTransformVO) {
        try {
            for (String insertSql : insertSqls) {
                targetJdbc.update(insertSql);
            }
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e.getMessage());
        }
//        logger.info("[{}] Success Sql Is {}", dataTransformVO.getTable(), insertSqls.size());
    }
}
