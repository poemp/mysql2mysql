package org.poem.vo;


import lombok.Data;

/**
 * @author sangfor
 */
@Data
public class GlobalSqlVariablesVO {
    /**
     * 主键
     */
    private Long id;

    /**
     * 名字
     */
    private String name;

    /**
     * 说明
     */
    private String content;

    /**
     * 函数的定义名字
     */
    private String functionName;

    /**
     * mysql的实现
     */
    private String mysqlFunction;

    /**
     * postgres函数
     */
    private String postgresFunction;

    /**
     * hive的执行函数
     */
    private String hiveFunction;

    /**
     * hbase 的执行函数
     */
    private String hbaseFunction;

}
