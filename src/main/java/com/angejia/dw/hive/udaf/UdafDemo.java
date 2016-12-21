package com.angejia.dw.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDAF;
import org.apache.hadoop.hive.ql.exec.UDAFEvaluator;

/**
 * @author Jason
 * udaf 案例, 用于实现求平均值
 * 
 * 测试: 
 *  add jar /path/xxx.jar
 *  create temporary function udaf_avg_demo as 'com.angejia.dw.hive.udaf.UdafDemo';
 *  select udaf_avg_demo(field_1) from table_name;
 */
public class UdafDemo extends UDAF  {

    /**
     * 用于保存结果数据的对象
     */
    public static class ResultObject {
        private long mCount;
        private double mSum;
    }


    public static class UdafDemoEvaluator implements UDAFEvaluator {
        ResultObject resultObject;
        
        public UdafDemoEvaluator() {
            super();

            resultObject = new ResultObject();

            init();
        }


        /**
         * init 函数类似于构造函数, 用于 UDAF 的初始化
         */
        public void init() {
            resultObject.mSum = 0;
            resultObject.mCount = 0;
        }
        
        
        /**
         * iterate 接收传入的参数, 并进行内部轮转, 返回类型为 boolean
         * @param o
         * @return boolean
         */
        public boolean iterate(Double ob) {
            if (ob != null) {
                resultObject.mSum += 0;
                resultObject.mCount ++;
            }
            return true;
        }


        /**
         * 用于 iterate 函数轮转结束后, 返回轮转数据, 类似于 Hadoop 的 combiner
         * @return ResultObject
         */
        public ResultObject terminatePartial () {
            if (resultObject.mCount == 0) {
                return null;
            } else {
                return resultObject;
            }
        }


        /**
         * 接收 terminatePartial 的返回结果, 进行数据 merge 操作
         * @param ob
         * @return boolean
         */
        public boolean merge(ResultObject ob) {
            if (ob != null) {
                resultObject.mCount += ob.mCount;
                resultObject.mSum += ob.mSum;
            }
            return true;
        }


        /**
         * 返回最终的聚集函数结果
         * @return
         */
        public Double terminate () {
            if (resultObject.mCount == 0) {
                return null;
            } else {
                return Double.valueOf(resultObject.mSum / resultObject.mCount);
            }
        }
    }
}
