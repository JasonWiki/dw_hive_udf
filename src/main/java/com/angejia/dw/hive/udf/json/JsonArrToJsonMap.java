package com.angejia.dw.hive.udf.json;

import com.angejia.dw.hive.udf.UdfBase;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import org.codehaus.jackson.map.ObjectMapper;

/**
 * @author Jason
 * Json Arr, 转换成 json Map
 * 案例: 
     [{"a":[{"b":"1"},{"c":"2"},{"d":"3"},{"e":"4"}]},{"f":"5"},"g"]

      To

     {"a":"1","pcsafe":[{"az500":"1"},{"az03":"1"},{"az04":"S"},{"az05":"100115"}]}
 */

public class JsonArrToJsonMap extends UdfBase {

    // Json 操作对象
    private ObjectMapper objectMapper = new ObjectMapper();

    public String evaluate(String str) {
        if (str == null) {
            return null;
        }

        Map<String, Object> jsonMap = this.jsonStrToMap(str);
        return this.mapToJsonStr(jsonMap);
    }


    /**
     * json 字符 -> map
     * @param jsonString 
     * @return
     */
    private Map<String,Object> jsonStrToMap(String jsonString) {

        // 保存处理好的 Json 格式
        Map<String,Object> resultMap = new HashMap<String, Object>();
        
        try {
            // 待处理的 Json 格式
            List<Object> listMap = objectMapper.readValue(jsonString, List.class);

            Map<String,Object> formatMap;
            for (int i = 0; i <= listMap.size() - 1; i ++) {
                Object valMap = listMap.get(i);
                try {
                    formatMap = (Map<String, Object>) valMap;
                    // 把 list map 中嵌套数据, 放到 map 中
                    for(Map.Entry<String, Object> entry :formatMap.entrySet()) {
                        resultMap.put(entry.getKey(), entry.getValue());
                    }
                } catch (ClassCastException e)  {
                    System.out.println(valMap + " : " + e.getMessage());
                }
                formatMap = null;
            }

        } catch(Exception e) {
            System.out.println(jsonString + " : " + e.toString());
        }

        return resultMap;
    }


    /**
     * map -> json 字符
     * @param map
     * @return
     */
    public String mapToJsonStr(Map<String,Object> map) {
        String jsonStr = "";

        try {
            jsonStr = objectMapper.writeValueAsString(map);
        } catch(Exception e) {
            System.out.println(map + " : " + e.toString());
        }

        return jsonStr;
    }

    
    public static void  main(String[] args) throws Exception{

        String jsonArr = "[{\"a\":\"1\"},{\"b\":\"2\"},\"3\",{\"c\":[{\"d\":\"4\"},{\"e\":\"5\"}]}]";
        // [{"pcsafe":[{"az500":"1"},{"az03":"1"},{"az04":"S"},{"az05":"100115"}]},{"a":"1"},"b"]
        jsonArr = "[{\"pcsafe\":[{\"az500\":\"1\"},{\"az03\":\"1\"},{\"az04\":\"S\"},{\"az05\":\"100115\"}]},{\"a\":\"1\"},\"b\"]";

        System.out.println(jsonArr);
        JsonArrToJsonMap obj = new JsonArrToJsonMap();
        System.out.println(obj.evaluate(jsonArr));
    }

}


