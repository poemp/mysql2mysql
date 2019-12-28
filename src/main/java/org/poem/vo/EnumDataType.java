package org.poem.vo;

/**
 * @author poem
 */
public enum EnumDataType {

    /**
     * null
     */
    NULL(""),
    /**
     * mysql
     */
    MYSQL("mysql"),
    /**
     * hive
     */
    HIVE("hive"),
    /**
     * oracle
     */
    ORACLE("oracle");

    private String type;

    EnumDataType(String type) {
        this.type = type;
    }

    public String getType() {
        return type;
    }

    @Override
    public String toString() {
        return type;
    }
}
