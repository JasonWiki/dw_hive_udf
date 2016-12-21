package com.angejia.dw.hive.udtf;

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
 * 把 json string 列, 转换为 k ,v 行
 * 
 * hive 中使用的案例
 *  1. 添加 jar 和 临时函数
 *      add jar /path/xxx.jar; 
 *      create temporary function json_str_to_kv as 'com.angejia.dw.hive.udtf.JsonStrToKv';
 *  2. 创建测试数据
 *      drop table dw_db_temp.src;
        create table dw_db_temp.src AS select '{"5":"0","8":"0","15":"4","18":"0","7":"41","11":"0","14":"2","19":"1","10":"8","13":"132"}' AS properties;
 *  3. 测试 
 *      测试列转行
 *      SELECT
          key, value
        FROM
          dw_db_temp.src
        lateral view
          json_str_to_kv(properties) kv_list AS key, value
        ;
 *
 */
public class JsonStrToKv extends GenericUDTF {

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
        fieldNames.add("key"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        // 以下是重复的套路！！！
        fieldNames.add("value"); // 字段名
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
            Map<String, String>  mapList;
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                mapList = objectMapper.readValue(inputJsonString, Map.class);

                // System.out.println(mapList);

                for(Map.Entry<String, String> entry:  mapList.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();

                    // 保存结果
                    ArrayList<String> result = new ArrayList<String>();
                    result.add(key);
                    result.add(value);

                    //System.out.println(result);

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

        String json = "{\"5\":\"0\",\"8\":\"0\",\"15\":\"4\",\"18\":\"0\",\"7\":\"41\",\"11\":\"0\",\"14\":\"2\",\"19\":\"1\",\"10\":\"8\",\"13\":\"132\"}";
        // 获取传入的值, key:value;key:value;
        Object[] udtfArgs =  {json};

        JsonStrToKv obj = new JsonStrToKv();
        obj.process(udtfArgs);
        
    }
}
