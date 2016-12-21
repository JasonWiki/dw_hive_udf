package com.angejia.dw.hive.udf;

import com.angejia.dw.hive.udf.UdfBase;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;


public class Decode extends UdfBase {
    private String result;


    public String evaluate(String str) {
       if (str == null) {
           return null;
       }
       return this.decode(str, "utf-8");
    }

    public String evaluate(String str,String character) {
        if (str == null || character == null) {
            return null;
        }
        return this.decode(str, character);
        
     }

    //解码
    private String decode(String str,String character) {
        try {
            this.result = URLDecoder.decode(str,character);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return this.result;
    }
    

    //测试
    public static void main(String[] args) throws UnsupportedEncodingException {
        Decode decode = new Decode();
        String result = decode.evaluate("150-200\u4e07\u3001\u4e8c\u5ba4\u3001\u6f4d\u574a\u3001\u66f9\u6768");
        System.out.println(result);
        
    }
    

}


