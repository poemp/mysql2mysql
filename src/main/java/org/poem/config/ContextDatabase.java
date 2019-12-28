package org.poem.config;

import lombok.Data;

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
     * 数据库
     */
    private static String targetCatalog;


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
}
