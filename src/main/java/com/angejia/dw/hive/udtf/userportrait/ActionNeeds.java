package com.angejia.dw.hive.udtf.userportrait;

import java.util.ArrayList;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import org.codehaus.jackson.map.ObjectMapper;


/**
 * @author Jason
 * 
 * 把用户画像的嵌套 Json 列, 转换为行
 * 
 * hive 中使用的案例
 *  1. 添加 jar 和 临时函数
 *      add jar /path/xxx.jar; 
 *      create temporary function userportrait_action_needs as 'com.angejia.dw.hive.udtf.userportrait.ActionNeeds';
 *  2. 创建测试数据
 *      drop table dw_db_temp.src;
        create table dw_db_temp.src AS select '{"1":{"bedrooms":"2","community":"12793","city":"1","price":"2","block":"129","district":"13","cnt":"1"},"2":{"bedrooms":"2","community":"16232","city":"1","price":"4","block":"183","district":"13","cnt":"1"}}'  AS properties
 *  3. 测试 
 *      测试列转行
 *      SELECT
          tag_key, city, district, block, community, bedrooms, price, cnt
        FROM
          dw_db_temp.src
        lateral view
          userportrait_action_needs(properties) now_action_needs_list AS tag_key, city, district, block, community, bedrooms, price, cnt
        ;
 */
public class ActionNeeds extends GenericUDTF {

    /**
     * 返回 UDTF 的返回行的信息（字段名，字段类型）
     */
    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
        }
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        }

        // 保存字段名
        ArrayList<String> fieldNames = new ArrayList<String>();

        // 保存字段家
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        // 增加字段
        fieldNames.add("tag_key"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        // 以下是重复的套路！！！
        fieldNames.add("city"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        fieldNames.add("district"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        fieldNames.add("block"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        fieldNames.add("community"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        fieldNames.add("bedrooms"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        fieldNames.add("price"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        fieldNames.add("cnt"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        // 最后返回 (字段名, 字段类型)
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }



    /**
     * 具体干活的小伙子！
     * process 方法, 真正的处理过程在 process 函数中
     *  在 process 中每一次 forward() 调用产生一行；如果产生多列可以将多个列的值放在一个数组中，然后将该数组传入到 forward() 函数
     */
    @Override
    public void process(Object[] args) throws HiveException {
        if (args == null || args.length < 1 ) {

        } else {
            // 读取输入的 json 格式数据 
            String inputJsonString = args[0].toString();

            // 转换 json string 为 Map 对象
            Map<String, Map<String, String>>  mapList;
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                mapList = objectMapper.readValue(inputJsonString, Map.class);

                //System.out.println(mapList);

                for(Map.Entry<String, Map<String, String>> entry:  mapList.entrySet()) {
                    Map<String, String> curMap = entry.getValue();

                    // 获取元素
                    String key = entry.getKey();
                    String city = curMap.get("city");
                    String district = curMap.get("district");
                    String block = curMap.get("block");
                    String community = curMap.get("community");
                    String bedrooms = curMap.get("bedrooms");
                    String price = curMap.get("price");
                    String cnt = curMap.get("cnt");

                    // 保存结果
                    ArrayList<String> result = new ArrayList<String>();
                    result.add(key);
                    result.add(city);
                    result.add(district);
                    result.add(block);
                    result.add(community);
                    result.add(bedrooms);
                    result.add(price);
                    result.add(cnt);

                    // System.out.println(result);

                    forward(result);
                }

            } catch(Exception e) {
                System.out.println(e.toString());
            }

        }

    }


    @Override
    public void close() throws HiveException {
        
    }

    public static void  main(String[] args) throws Exception{

        String json = "{\"45\":{\"bedrooms\":\"2\",\"community\":\"12793\",\"city\":\"1\",\"price\":\"2\",\"block\":\"129\",\"district\":\"13\",\"cnt\":\"1\"},\"34\":{\"bedrooms\":\"2\",\"community\":\"16232\",\"city\":\"1\",\"price\":\"4\",\"block\":\"183\",\"district\":\"13\",\"cnt\":\"1\"},\"67\":{\"bedrooms\":\"2\",\"community\":\"12652\",\"city\":\"1\",\"price\":\"2\",\"block\":\"127\",\"district\":\"13\",\"cnt\":\"1\"},\"12\":{\"price\":\"2\",\"city\":\"1\",\"cnt\":\"1\",\"block\":\"184\"},\"66\":{\"bedrooms\":\"2\",\"community\":\"16237\",\"city\":\"1\",\"price\":\"3\",\"block\":\"183\",\"district\":\"13\",\"cnt\":\"1\"},\"51\":{\"city\":\"1\",\"cnt\":\"5\",\"community\":\"15961\"},\"8\":{\"city\":\"1\",\"cnt\":\"0\",\"bedrooms\":\"2\",\"community\":\"20152\"},\"19\":{\"city\":\"1\",\"cnt\":\"21\",\"community\":\"16253\"},\"23\":{\"bedrooms\":\"2\",\"city\":\"1\",\"price\":\"2\",\"block\":\"126\",\"district\":\"13\",\"cnt\":\"1\"},\"62\":{\"bedrooms\":\"2\",\"community\":\"12819\",\"city\":\"1\",\"price\":\"3\",\"block\":\"130\",\"district\":\"13\",\"cnt\":\"1\"},\"4\":{\"city\":\"1\",\"cnt\":\"0\",\"community\":\"9375\"},\"40\":{\"bedrooms\":\"2\",\"city\":\"1\",\"price\":\"2\",\"district\":\"13\",\"cnt\":\"65\"},\"15\":{\"bedrooms\":\"1\",\"city\":\"1\",\"price\":\"1\",\"block\":\"184\",\"cnt\":\"6\"},\"11\":{\"city\":\"1\",\"cnt\":\"3\",\"block\":\"184\"},\"9\":{\"bedrooms\":\"2\",\"community\":\"20152\",\"city\":\"1\",\"price\":\"9\",\"block\":\"60\",\"district\":\"7\",\"cnt\":\"0\"},\"44\":{\"bedrooms\":\"2\",\"community\":\"12575\",\"city\":\"1\",\"price\":\"2\",\"block\":\"125\",\"district\":\"13\",\"cnt\":\"2\"}}";

        // 获取传入的值, key:value;key:value;
        Object[] udtfArgs =  {json};

        ActionNeeds obj = new ActionNeeds();
        obj.process(udtfArgs);
        
    }

}
