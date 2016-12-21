package com.angejia.dw.hive.udf.parse;

import java.util.Map;

import javax.crypto.Cipher;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import org.apache.hadoop.hive.ql.exec.UDF;

//import sun.misc.BASE64Decoder;
import org.apache.commons.codec.binary.Base64;

import org.codehaus.jackson.map.ObjectMapper;

import java.net.URLDecoder;

/**
 * 解密 token 
 */
public class ParseMobileToken extends UDF {

    public String evaluate(String s, String index) throws Exception {

        if ( s.equals("null") || s == null || s.length() <= 0) { return ""; }

        String token = Decrypt(s);

        // 兼容 url encoder 后的数据
        try {
            token = URLDecoder.decode(token, "utf-8");
        } catch (Exception e) {
           System.out.println(e.getMessage());
           System.out.println(token);
           return "";
        }
        //System.out.println(token);
        //System.exit(0);

        if( token != null && token.length() > 0 ){//json decode
            Map<String, Map<String, Object>>  maps;
            ObjectMapper objectMapper = new ObjectMapper();
            try {
                maps = objectMapper.readValue(token, Map.class);
   
                // System.out.println(maps);

                if(maps.containsKey(index)){
                    return String.valueOf(maps.get(index));
                }
            } catch(Exception e) {
                System.out.println(e.toString());
                return "";
            }
        }

        return "";
    }
    public static String Decrypt(String data) throws Exception {
        try
        {
            String key = "12345678123456xx";
            String iv = "12345678123456xx";

            //byte[] encrypted1 = new Base64().decodeBuffer(data);
            byte[] encrypted1 = new Base64().decode(data);
  
            Cipher cipher = Cipher.getInstance("AES/CBC/NoPadding");
            SecretKeySpec keyspec = new SecretKeySpec(key.getBytes(), "AES");
            IvParameterSpec ivspec = new IvParameterSpec(iv.getBytes());

            cipher.init(Cipher.DECRYPT_MODE, keyspec, ivspec);
            try {
                byte[] original = cipher.doFinal(encrypted1);
                String originalString = new String(original);
                return originalString;
            } catch(Exception e) {
                System.out.println(e.toString());
                return null;
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

   
      public static void main(String[] args) throws Exception{
         String  data;
        //data = "yYN53I9zZiAeNV0e7MMsUVse4ivMNzZB8qGTB3ZEMVanOKNEMAaaxai1pQ/CwPA0N6tfpGDw/ArAgwXJmtgEj297whx3I7q1aj9HMBNPeuA19glHF22CgItG6q9f7/S6DPBddG7sMPyFGbpiAbsWx25urUfL+xy3cizsMpxietlxMUClnSWpBsWpuiZMTZdSOnwDo0i79FXonTL7SJLE8w==";
        //data ="-";
        data = "ExhuBkt16RSHa0/0C+y9xwrh+DpuLOG+hcnAdFYUTAwvrbsD6c/1qtr/Mp9GD3m+W6FGd20/EqRwl/6IvoUZ6rLPhoXIfZDBa/WcYAKCIG/cniyZFdneteixki/tVBjWDU3Tg2a9QmZQ1wdkE+M9gaPx/YyC43WRvl08boqRkhxF3eE+f+7lWKFTHNGYqzCR6NZJcHnGoiXZPCpix/gS3094PXNeXxeyrNEcSrqeV1O/ZV/8jQFS9rXGyBFlyh9qXkg7iUJ3sEZzpbMyC4nV3HssvjZLK9ICO0ZL8MRftRLCHRGXq3CDHp0DKB6k58LI0ufNn1i17y9S0fEklgDD/k/0WnlwHaMnhfT0YraRNXc=";
        //data="R3Vq/NxLCVk/17Qk7Oz3No7xMZAr+etsJQGHirWuaMq7Cdv+s8HZGyeSZNoGnezX3YzfEh+/pmeSipc4p4XmKXWnHd7oQp9EEJJWwHEKDRUFSqO09AG2WF3Xf8JPWkBsoohHVF25SE79btWgJCJ5wryLgsFv4+82JcDpHWI55qXrqsn2SU47nirMomP8Jine7BWrbAFqIwxdc6BHtIdPLuJlwO/zunvUd9RZ3nez/RFZO/x1OSZdPy3GZL45Hf1uYo87Cr1bBXPCoLCr9CizdZATzAzUfrWvd0Qw1GUTdN42jRisJBoCi5sTUzcHEPPd0EBlCZTz27SBFsBWXuoqdPpS1muSPg/s+CFfCKoo/8OruYZS++kO8UwkkMlOdI8uuoUs6NJw62Jy+1ts7qiqj1zVsg1xNitvKWP5dV5gyORyEyIDRRZwA8wfwlX8prnUdjl4ShEWIgsccGUrpd6a2vYp0SAvlf4FNcDRiSgUVRHiNr4jwLlZnXHJwsgQAZAda4sWAsJs5YhsGUhCi06t7befV1A/rOrerF4L5ELI7/PUSrO08SWj00+PYACSkhIcBVWisNEyI1LF9BP5R7mtRked43gwTSX8cQum/m/Ft8rGh9lZT8wRrxDtL0DxRct60pUyaeFg4evL6/iu+WImf9XlKrPXTI4YPqA1Raf8SJRdwSjYAinLOsE0VyCA5C1kFPSqdPqzKSoYBrLhTnwQZhbeRx9gXrsX+SjHfHHjqhA=";
        
        ParseMobileToken obj = new ParseMobileToken();
        System.out.println(obj.evaluate(data, "user_id"));
        //System.out.println(obj.evaluate(data, "user_"));
      }
    
}
