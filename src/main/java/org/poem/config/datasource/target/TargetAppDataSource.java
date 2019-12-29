package org.poem.config.datasource.target;

import com.zaxxer.hikari.HikariDataSource;
import lombok.Data;
import org.poem.config.ContextDatabase;
import org.poem.utils.DataSourceDriverHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import javax.sql.DataSource;

/**
 * 目标数据库
 * @author sangfor
 */
@Configuration
@Component
@Data
@ConfigurationProperties(prefix = "spring.target")
public class TargetAppDataSource {

    private static final Logger logger = LoggerFactory.getLogger(TargetAppDataSource.class);

    private String driverClassName;
    private String url;
    private String username;
    private String password;


    @Bean(name = "targetHikariData")
    public DataSource targetHikariDataSource() {
        logger.info("\nInit target  DataBase [" + DataSourceDriverHelper.getDatasourceType(url) + "] : " +
                "\n\t\t [driverClassName]:" + DataSourceDriverHelper.getJdbc(url) +
                "\n\t\t [url]:" + url +
                "\n\t\t [user]:" + username +
                "\n\t\t [password]:" + password
        );
        HikariDataSource dataSource = new HikariDataSource();
        dataSource.setDriverClassName(DataSourceDriverHelper.getJdbc(url));
        dataSource.setJdbcUrl(url);
        dataSource.setUsername(username);
        dataSource.setPassword(password);
        dataSource.setMaximumPoolSize(30);
        dataSource.setConnectionTestQuery("select 1");
        return dataSource;
    }

    @Bean(name = "targetJdbcTemplate")
    public JdbcTemplate targetJdbcExcuetor(@Qualifier("targetHikariData") DataSource targetHikariDataSource) {
        ContextDatabase.setTargetCatalog(DataSourceDriverHelper.getCatalog(url));
        return new JdbcTemplate(targetHikariDataSource);
    }

}
