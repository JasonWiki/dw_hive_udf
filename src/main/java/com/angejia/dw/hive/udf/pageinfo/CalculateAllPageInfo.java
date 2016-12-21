package com.angejia.dw.hive.udf.pageinfo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF; 
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;


/**
 * 已经废弃
 * @author jack
 */
public class CalculateAllPageInfo extends GenericUDTF {
	
	private static String filePath = "/umr-jdlg4d/hive/dw_db/dw_basis_dimension_pagename_lkp/part-m-00000";
	private static Map<String, String[]> equalsMap = new HashMap<String, String[]>();// 匹配全等字典
	private static Map<String, String[]> patternMap = new HashMap<String, String[]>();// 匹配正则字典
	
	public CalculateAllPageInfo(){
		//readLocal();
	}
    @Override
    public void close() throws HiveException {
        // TODO Auto-generated method stub
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args)
            throws UDFArgumentException {
        if (args.length != 1) {
            throw new UDFArgumentLengthException("ExplodeMap takes only one argument");
        }
        if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentException("ExplodeMap takes string as a parameter");
        }
        //初始化读文件
        getHdfsDic();
        
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();
        fieldNames.add("page_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("page_name");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        fieldNames.add("paltform_id");
        fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] args) throws HiveException {
    	
    	String[] equalsRe = new String[3];
    	if(args[0].toString().length() != 0){
    		URL url = null;
    		try {
    			url = new URL(args[0].toString());
    		} catch (MalformedURLException e) {
    			System.out.println("URL格式错误");
    			e.printStackTrace();
    		}
    		
    		if( url != null){
    			
    			String urlMatcher = url.getHost()+cutEndof(url.getPath());//去掉结尾/的干扰
    			equalsRe = equalsMap.get(urlMatcher);
    			if(equalsRe == null){
    				//前面hashmap匹配失败，后面正则匹配
        	        for (Map.Entry<String, String[]> entry : patternMap.entrySet()) {
        	        	Pattern pattern = Pattern.compile('^'+entry.getKey()+'$');
        				Matcher matcher = pattern.matcher(urlMatcher);
        	            if(matcher.matches()){
        	            	equalsRe = entry.getValue();
        	            }
        	        }
    			}
    			
    		}
    	}

    	equalsRe = (equalsRe == null || equalsRe[0] == null) ? (new String[] {"","",""}): equalsRe;
    	
		forward(equalsRe);
    }
    
    public String[] processTest(String args)  {
    	
    	String[] equalsRe = new String[3];
    	if(args.toString().length() != 0){
    		URL url = null;
    		try {
    			url = new URL(args.toString());
    		} catch (MalformedURLException e) {
    			System.out.println("URL格式错误");
    			e.printStackTrace();
    		}
    		
    		if( url != null){
    			
    			String urlMatcher = url.getHost()+cutEndof(url.getPath());//去掉结尾/的干扰
    			equalsRe = equalsMap.get(urlMatcher);
    			if(equalsRe == null){
    				//前面hashmap匹配失败，后面正则匹配
        	        for (Map.Entry<String, String[]> entry : patternMap.entrySet()) {
        	        	Pattern pattern = Pattern.compile('^'+entry.getKey()+'$');
        				Matcher matcher = pattern.matcher(urlMatcher);
        	            if(matcher.matches()){
        	            	equalsRe = entry.getValue();
        	            }
        	        }
    			}
    			
    		}
    	}

    	equalsRe = (equalsRe == null || equalsRe[0] == null) ? (new String[] {"","",""}): equalsRe;
    	
		return equalsRe;
    }
    
    /**
	 * 从hdfs获取字典
	 */
	private static void getHdfsDic() {
		BufferedReader br = null;
		try {
			String encoding = "UTF-8";
			FileSystem fs = FileSystem.get(new Configuration());
			FSDataInputStream inputStream = fs.open(new Path(filePath));
			br = new BufferedReader(new InputStreamReader(inputStream, encoding));
			String line = null;
			while (null != (line = br.readLine())) {
				String[] patterns = line.split("\001");
				patterns[5] = cutEndof(patterns[5]);
				String[] value = { patterns[1], patterns[2],patterns[4] };
				String tmpKey = patterns[3] + patterns[5];
				//System.out.println(tmpKey+value[0]+value[1]);
				//检测是哪种匹配方法
				if(patterns[6].equals("1")){
					
					equalsMap.put(tmpKey, value);
					
				}else if(patterns[6].equals("2")){//按照正则匹配

					patternMap.put(tmpKey, value);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (null != br) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * 去掉结尾的'/'
	 * @param line
	 * @return
	 */
	private static String cutEndof(String line){
		
		if(line.endsWith("/") && line.length() > 1){
			line = line.substring(0,line.length() - 1);
		}
		
		return line;
	}
	
	private static void readLocal() {
		filePath = "/Users/ray/part-m-00000";
		try {
			String encoding = "UTF-8";
			File file = new File(filePath);
			if (file.isFile() && file.exists()) { // 判断文件是否存在
				InputStreamReader read = new InputStreamReader(
						new FileInputStream(file), encoding);// 考虑到编码格式
				BufferedReader bufferedReader = new BufferedReader(read);
				String line = null;

				while ((line = bufferedReader.readLine()) != null) {
					
					String[] patterns = line.split("\001");
					patterns[5] = cutEndof(patterns[5]);
					String[] value = { patterns[1], patterns[2],patterns[4] };
					String tmpKey = patterns[3] + patterns[5];
					//System.out.println(tmpKey+value[0]+value[1]);
					//检测是哪种匹配方法
					if(patterns[6].equals("1")){
						
						equalsMap.put(tmpKey, value);
						
					}else if(patterns[6].equals("2")){//按照正则匹配

						patternMap.put(tmpKey, value);
					}
				}
				read.close();
			} else {
				System.out.println("找不到指定的文件");
			}
		} catch (Exception e) {
			System.out.println("读取文件内容出错");
			e.printStackTrace();
		}

	}
}
