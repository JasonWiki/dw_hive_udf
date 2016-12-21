package com.angejia.dw.hive.udf.parse;

import org.apache.commons.lang.StringUtils; 
import org.apache.hadoop.hive.ql.exec.UDF;
import java.util.List;
import java.util.ArrayList;

/**
 * 解析 actionId 为 Page Id, 用于兼容
 */
public class ParseActionIdToPageId extends UDF {

    public String evaluate(String actionId) throws Exception {
        return ParseActionIdToPageId.actionIdToPageId(actionId , "");
    }

    public String evaluate(String actionId, String extendString) throws Exception {
        return ParseActionIdToPageId.actionIdToPageId(actionId, extendString);
    }

    public static String actionIdToPageId(String actionId, String extendString) {
        String rsPageIdString = "";

        if (actionId == null || actionId.length() <= 0) { 
            return rsPageIdString; 
        }

        // 分解 action ID
        String[] actionIdArr = actionId.split("-");
        int actionIdArrLength = actionIdArr.length;
 
        // 处理 1-100038 格式的 action Id , 转换为 page Id
        if (actionIdArrLength == 2) {  
            // 拼接成为 1-100038 -> 1-100000
            rsPageIdString = actionId.substring(0, actionId.length() - 3) + "000";

        // 处理 1-100042-0111 格式的 action Id , 转换为 page Id
        } else if (actionIdArrLength == 3) { 

            List<String> rsList = new ArrayList<String>();
            int i = 0;
            for (String element : actionIdArr) {
                if (i == 2) break;
                rsList.add(element);
                i ++ ;
            }
            rsList.add("0");
            rsPageIdString = StringUtils.join(rsList, "-");
        }

        return rsPageIdString + extendString;
    }



    public static void main(String[] args) throws Exception{

        ParseActionIdToPageId obj = new ParseActionIdToPageId();
        System.out.println(obj.evaluate("1-100038"));
        System.out.println(obj.evaluate("1-100042-12312312"));
        System.out.println(obj.evaluate("1"));
        System.out.println(obj.evaluate(null));
        System.out.println(obj.evaluate(""));

    }
}
