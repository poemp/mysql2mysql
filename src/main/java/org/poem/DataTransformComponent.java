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
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import java.sql.SQLException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author poem
 * 任务管理
 */
@Data
@Configuration
@Component
public class DataTransformComponent implements CommandLineRunner {

    private static final Logger logger = LoggerFactory.getLogger(DataTransformComponent.class);


    private static ExecutorService threadPoolExecutor;

    private final JdbcTemplate sourceJdbc;

    private final JdbcTemplate targetJdbc;

    private final TransformTables transformTables;


    public DataTransformComponent(@Qualifier("sourceJdbcTemplate") JdbcTemplate sourceJdbc, @Qualifier("targetJdbcTemplate") JdbcTemplate targetJdbc, TransformTables transformTables) {
        this.sourceJdbc = sourceJdbc;
        this.targetJdbc = targetJdbc;
        this.transformTables = transformTables;
    }
    /**
     * 多线程
     */
    static {
        int core = Runtime.getRuntime().availableProcessors();
        AtomicInteger integer = new AtomicInteger(0);
        threadPoolExecutor = new ThreadPoolExecutor(core, core * 4, 1000, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(core),
                new ThreadFactory() {
                    @Override
                    public Thread newThread(Runnable r) {
                        //线程命名
                        Thread th = new Thread(r, "mysql-to-mysql-threadPool-" + integer.incrementAndGet());
                        return th;
                    }
                }, new ThreadPoolExecutor.CallerRunsPolicy());
    }


    /**
     * 启动的时候执行该方法，或者是使用ApplicationListener，在启动的时候执行该方法
     * 具体使用见：http://blog.csdn.net/liuchuanhong1/article/details/77568187
     */
    public void schedule() throws SQLException {
        MysqlToMysqlComponent mysqlToMysqlComponent = new MysqlToMysqlComponent();
        DataTransformTaskVO dataTransformVO;
        String tables = transformTables.getTables();
        if (!StringUtils.isEmpty(tables)){
            String[] ts = tables.split(",");
            for (String t : ts) {
                dataTransformVO = new DataTransformTaskVO();
                dataTransformVO.setTable(t );
                mysqlToMysqlComponent.importData(dataTransformVO, sourceJdbc, targetJdbc);
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
