package com.angejia.dw.hive.udf;
 
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author Jason
 * 1.编写 udf 函数
 * 2.上传到 hdfs 
 * 3.add jar hdfs://Ucluster/user/jars/test.jar
 * 4.create temporary function my_test as 'com.angejia.dw.hive.udf.UdfDemo';
 * 5.select network_type,my_test(network_type) from dw_app_access_log limit 10;
 */

public final class UdfDemo extends UDF {
  public Text evaluate(final Text s) {
    if (s == null) { return null; }
        return new Text(s.toString().toLowerCase());
  }
}