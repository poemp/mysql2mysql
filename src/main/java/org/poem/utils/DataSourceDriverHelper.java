package org.poem.utils;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.poem.vo.EnumDataType;

import java.util.List;
import java.util.Map;

/**
 * @author poem
 */
public class DataSourceDriverHelper {

    /**
     * 根据数据源的信息，返回数据库的驱动信息
     *
     * @param url
     * @return
     */
    public static String getJdbc(String url) {
        if (StringUtils.isEmpty(url)) {
            return "";
        } else if (url.startsWith("jdbc:mysql")) {
            return "com.mysql.cj.jdbc.Driver";
        } else if (url.startsWith("jdbc:hive2")) {
            return "org.apache.hive.jdbc.HiveDriver";
        } else if (url.startsWith("jdbc:oracle:thin")) {
            return "oracle.jdbc.driver.OracleDriver";
        }

        return "";
    }

    /**
     * 数据库
     * @param url
     * @return
     */
    public static String getCatalog(String url){
        int idex = url.lastIndexOf("?");
        char[] chars = url.toCharArray();
        char x = "/".toCharArray()[0];
        for(int i =  idex ; i >= 0; i--){
            if (chars[i] == x){
                return  url.substring(i + 1, idex);
            }
        }
        return "";
    }


    /**
     * 压缩数据
     *
     * @param colunms
     * @param colunmsType
     * @return
     */
    public static Map<String, String> zipColunmTypes(List<String> colunms, List<String> colunmsType) {
        Map<String, String> map = Maps.newHashMap();
        for (int i = 0; i < colunms.size(); i++) {
            map.put(colunms.get(i), colunmsType.get(i));
        }
        return map;
    }


    /**
     * 根据数据源的信息，返回数据库的驱动信息
     *
     * @param url
     * @return
     */
    public static EnumDataType getDatasourceType(String url) {
        if (StringUtils.isEmpty(url)) {
            return EnumDataType.NULL;
        } else if (url.startsWith("jdbc:mysql")) {
            return EnumDataType.MYSQL;
        } else if (url.startsWith("jdbc:hive2")) {
            return EnumDataType.HIVE;
        } else if (url.startsWith("jdbc:oracle:thin")) {
            return EnumDataType.ORACLE;
        }

        return EnumDataType.NULL;
    }

}
