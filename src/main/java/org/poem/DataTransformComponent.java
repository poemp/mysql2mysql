package org.poem;

import lombok.Data;
import org.poem.config.table.TransformTables;
import org.poem.mysql2mysql.MysqlToMysqlComponent;
import org.poem.vo.DataTransformTaskVO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author poem
 * 任务管理
 */
@Data
@Configuration
@Component
public class DataTransformComponent implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(DataTransformComponent.class);


    private final JdbcTemplate sourceJdbc;

    private final JdbcTemplate targetJdbc;

    private final TransformTables transformTables;

    /**
     * 缓存
     */
    private static final ConcurrentHashMap<String, String> LOCK_MAP = new ConcurrentHashMap<String, String>();

    public DataTransformComponent(@Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbc, @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbc, TransformTables transformTables) {
        this.sourceJdbc = sourceJdbc;
        this.targetJdbc = targetJdbc;
        this.transformTables = transformTables;
    }


    /**
     * 启动的时候执行该方法，或者是使用ApplicationListener，在启动的时候执行该方法
     * 具体使用见：http://blog.csdn.net/liuchuanhong1/article/details/77568187
     */
    @Scheduled(cron = "0 0/1 * * * ? ")
    public void schedule() throws SQLException {
        MysqlToMysqlComponent mysqlToMysqlComponent = new MysqlToMysqlComponent();
        DataTransformTaskVO dataTransformVO;
        String tables = transformTables.getTables();
        if (!StringUtils.isEmpty(tables)) {
            String[] ts = tables.split(",");
            for (String t : ts) {
                if (LOCK_MAP.get(t) == null) {
                    try {
                        LOCK_MAP.put(t, "1");
                        logger.info("Start Export Table {}, {}", t, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss ")));
                        dataTransformVO = new DataTransformTaskVO();
                        dataTransformVO.setTable(t);
                        mysqlToMysqlComponent.importData(dataTransformVO, sourceJdbc, targetJdbc);
                        logger.info("End Export Table {}, {}", t, LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss ")));
                    } catch (Exception e) {
                        logger.error(e.getMessage(), e);
                    } finally {
                        LOCK_MAP.remove(t);
                    }
                }

            }
        }

    }


    /**
     * @param args
     * @throws Exception
     */
    @Override
    public void run(String... args) throws Exception {
        schedule();
    }

}
