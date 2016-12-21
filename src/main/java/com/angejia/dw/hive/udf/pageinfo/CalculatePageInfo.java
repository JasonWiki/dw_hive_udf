package com.angejia.dw.hive.udf.pageinfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;


/**
 * 获取pagename 或者 pageid
 *
 * @param hostName
 *            m.anjuke.com
 * @param method
 *            GET/POST/PUT
 * @param urlPath
 *            /sh/sale/1235
 * @param type
 *            name,id返回pagename还是pageid
 * @param sourcePath
 *            字典文件路径
 * @returnString
 */
public class CalculatePageInfo extends UDF {

    // 待匹配的 HDFS 文件
    private static String filePath = "/user/hive/dw_db/dw_basis_dimension_pagename_lkp/part-m-00000";
    private static String fileSeparator = "\001";

    // 匹配全等字典
    private static Map<String, Map<String, String>> equalsMap = new HashMap<String, Map<String, String>>();
    // 匹配正则字典
    private static Map<String, Map<String, String>> patternMap = new HashMap<String, Map<String, String>>();
    // 结果字典
    private static Map<String, String> resultMap = new HashMap<String, String>();


    public CalculatePageInfo() {
        getHdfsDic();
        //readLocal();
    }


    public static String evaluate(String inputUrl, String inputKey) {

        String result = "";
        if (inputUrl != null && inputKey != null && inputUrl.length() != 0
                && inputKey.length() != 0) {
            URL url = null;
            try {
                url = new URL(inputUrl.toString());
            } catch (MalformedURLException e) {
                System.out.println("URL格式错误");
                //e.printStackTrace();
                return result;
            }

            if (url != null) {

                String urlMatcher = url.getHost() + cutEndof(url.getPath());// 去掉结尾/的干扰
                resultMap = equalsMap.get(urlMatcher);
                if (resultMap == null) {
                    // 前面hashmap匹配失败，后面正则匹配
                    for (Map.Entry<String, Map<String, String>> entry : patternMap
                            .entrySet()) {
                        Pattern pattern = Pattern
                                .compile('^' + entry.getKey() + '$');
                        Matcher matcher = pattern.matcher(urlMatcher);
                        if (matcher.matches()) {
                            resultMap = entry.getValue();
                        }
                    }
                }
            }
            
	    if (resultMap != null ) {
                try{
		    result = resultMap.get(inputKey);
		}catch ( Exception e ) {
        	    result = "";	
		    e.printStackTrace();
	        }
            }		

        }
        return result;
    }


    /**
     * 从 hdfs 获取字典
     */
    private static void getHdfsDic() {
        BufferedReader br = null;
        try {
            String encoding = "UTF-8";
            FileSystem fs = FileSystem.get(new Configuration());
            FSDataInputStream inputStream = fs.open(new Path(filePath));
            br = new BufferedReader(
                    new InputStreamReader(inputStream, encoding));
            String line = null;
            while (null != (line = br.readLine())) {
                /**
                 * 分隔行
                 * patterns[0]  id
                 * patterns[1]  page_id
                 * patterns[2]  page_name
                 * patterns[3]  page_hostname
                 * patterns[4]  platform_id   平台编号 1-Touchweb; 2-PC; 3-APP; 4 API
                 * patterns[5]  page_pattern  匹配的正则表达式,或者 URL
                 * patterns[6]  match_type    匹配类型 1:全匹配，2:正则匹配
                 */
                String[] patterns = line.split(fileSeparator);
                patterns[5] = cutEndof(patterns[5]);
                Map<String, String> value = new HashMap<String, String>();
                value.put("page_id", patterns[1]);
                value.put("page_name", patterns[2]);
                value.put("platform_id", patterns[4]);

                String tmpKey = patterns[3] + patterns[5];
                // System.out.println(tmpKey+value[0]+value[1]);
                // 检测是哪种匹配方法
                if (patterns[6].equals("1")) {

                    equalsMap.put(tmpKey, value);

                } else if (patterns[6].equals("2")) {// 按照正则匹配

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
     *
     * @param line
     * @return
     */
    private static String cutEndof(String line) {

        if (!line.equals("/") && line.endsWith("/")) {
            line = line.substring(0, line.length() - 1);
        }

        return line;
    }


//	private static void readLocal() {
//		filePath = "/Users/ray/part-m-00000";
//		try {
//			String encoding = "UTF-8";
//			File file = new File(filePath);
//			if (file.isFile() && file.exists()) { // 判断文件是否存在
//				InputStreamReader read = new InputStreamReader(
//						new FileInputStream(file), encoding);// 考虑到编码格式
//				BufferedReader bufferedReader = new BufferedReader(read);
//				String line = null;
//
//				while ((line = bufferedReader.readLine()) != null) {
//
//					String[] patterns = line.split("\001");
//					patterns[5] = cutEndof(patterns[5]);
//					Map<String, String> value = new HashMap<String, String>();
//					value.put("page_id", patterns[1]);
//					value.put("page_name", patterns[2]);
//					value.put("platform_id", patterns[4]);
//
//					String tmpKey = patterns[3] + patterns[5];
//					// System.out.println(tmpKey+value[0]+value[1]);
//					// 检测是哪种匹配方法
//					if (patterns[6].equals("1")) {
//
//						equalsMap.put(tmpKey, value);
//
//					} else if (patterns[6].equals("2")) {// 按照正则匹配
//
//						patternMap.put(tmpKey, value);
//					}
//				}
//				read.close();
//			} else {
//				System.out.println("找不到指定的文件");
//			}
//		} catch (Exception e) {
//			System.out.println("读取文件内容出错");
//			e.printStackTrace();
//		}
//
//	}
}
