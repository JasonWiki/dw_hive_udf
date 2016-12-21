package com.angejia.dw.hive.udtf;

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


/**
 * @author Jason
 * 原理思路
 *   1. 继承 GenericUDTF 实现 initialize, process, close 三个方法
 *   2. UDTF 首先会调用 initialize 方法，此方法返回 UDTF 的返回行的信息（返回个数，类型）
 *   3. initialize 后, 会调用 process 方法, 真正的处理过程在 process 函数中
 *        在 process 中每一次 forward() 调用产生一行；如果产生多列可以将多个列的值放在一个数组中，然后将该数组传入到 forward() 函数
 *   4. 最后 close() 方法调用，对需要清理的方法进行清理。
 *   
 * hive 中使用的案例
 *  1. 添加 jar 和 临时函数
 *      add jar /path/xxx.jar; 
 *      create temporary function udtf_demo as 'com.angejia.dw.hive.udtf.UdtfDemo';
 *  2. 创建测试数据
 *      create table dw_db_temp.src AS select "a:1-b:2" AS properties;
 *  3. 测试 
 *      测试列转行
 *      select udtf_demo('a:1-b:2') as (key,value);
 *      
 *      测试表字段列转行
 *      select 
          key,value
        from dw_db_temp.src 
        lateral view 
          udtf_demo(properties) now_list AS key, value
        ;
 *  
 */

public class UdtfDemo extends GenericUDTF {

    /**
     * 返回 UDTF 的返回行的信息（字段名，字段类型）
     * 
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

        // 增加字段名为 col1 的字段
        fieldNames.add("col1"); // 字段名
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);    // 字段类型

        // 以下是重复的套路！！！
        fieldNames.add("col2");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);

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
        String input = args[0].toString();
        // 按照 ; 号分割
        String[] test = input.split("-");
        for(int i=0; i<test.length; i++) {
            try {
                String[] result = test[i].split(":");
                // System.out.println(result);
                // 生成一行
                forward(result);
                
            } catch (Exception e) {
                continue;
            }
        }
    }


    /**
     * 处理完成后的关闭流程
     */
    @Override
    public void close() throws HiveException {
        
    }

    public static void  main(String[] args) throws Exception{
        
        // 获取传入的值, key:value;key:value;
        Object[] argdss =  {"key:value-key:value"};

        UdtfDemo obj = new UdtfDemo();
        obj.process(argdss);

    }

}
