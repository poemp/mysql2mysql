package org.poem.config;

import lombok.Data;
import org.poem.vo.EnumDataType;

/**
 * @author sangfor
 */
@Data
public class ContextDatabase {



    /**
     * 数据库
     */
    private static String sourceCatalog;

    /**
     * source 数据库的类型
     */
    private static EnumDataType sourceSchema;

    /**
     * 数据库
     */
    private static String targetCatalog;
    /**
     * source 数据库的类型
     */
    private static EnumDataType targetSchema;

    public static String getSourceCatalog() {
        return sourceCatalog;
    }

    public static void setSourceCatalog(String sourceCatalog) {
        ContextDatabase.sourceCatalog = sourceCatalog;
    }

    public static String getTargetCatalog() {
        return targetCatalog;
    }

    public static void setTargetCatalog(String targetCatalog) {
        ContextDatabase.targetCatalog = targetCatalog;
    }


    public static EnumDataType getSourceSchema() {
        return sourceSchema;
    }

    public static void setSourceSchema(EnumDataType sourceSchema) {
        ContextDatabase.sourceSchema = sourceSchema;
    }

    public static EnumDataType getTargetSchema() {
        return targetSchema;
    }

    public static void setTargetSchema(EnumDataType targetSchema) {
        ContextDatabase.targetSchema = targetSchema;
    }
}
