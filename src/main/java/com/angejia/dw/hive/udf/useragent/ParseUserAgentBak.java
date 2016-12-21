package com.angejia.dw.hive.udf.useragent;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;

import eu.bitwalker.useragentutils.Browser;
import eu.bitwalker.useragentutils.OperatingSystem;
import eu.bitwalker.useragentutils.UserAgent;


/**
 * 解析浏览器类型的 agent 信息
 */
public class ParseUserAgentBak extends UDF {
    static int max = 5;
    static String[] str = new String[max];
    static Properties browserConfProp = new Properties();// 解析浏览器


    public static String evaluate(Object o_input, int index) {

        String _input = (o_input != null) ? o_input.toString() : "";//因为有的输入不一定是string，比如jsion的null。

        if (_input.length() == 0){
            return "";
        }

        // 数据初始化

        UserAgent userAgent = UserAgent.parseUserAgentString(_input);
        Browser browser = userAgent.getBrowser();
        OperatingSystem os = userAgent.getOperatingSystem();

        str[0] = os.getName();
        str[1] = "";
        str[2] = browser.getName();
        str[3] = userAgent.getBrowserVersion() == null ? "" : userAgent.getBrowserVersion().toString();
        str[4] = os.getDeviceType().getName();

        if(index == 0 || index == 1){
            ParseSystem(_input);
        }else if(index == 2 || index == 3){
            ParseBrowser(_input);
        }

        if (index < max) {
            return str[index];
        }
        return "";
    }
    private static void ParseSystem(String _input) {
        if (str[0].startsWith("Android")) {
            str[0] = "Android";
            Pattern p = Pattern.compile("Android (([0-9]|\\.)+)");
            Matcher m = p.matcher(_input);
            if(m.find()){
                str[1] = m.group(1);
            }
        } else if (str[0].startsWith("iOS") || str[0].startsWith("Mac") ) {
            str[0] = "iOS";
            Pattern p = Pattern.compile("OS (([0-9]|_)+)");
            Matcher m = p.matcher(_input);
            if(m.find()){
                str[1] = m.group(1).replace('_', '.');
            }
        } else if (str[0].startsWith("Windows")){
            Pattern p = Pattern.compile("Windows NT (([0-9]|\\.)+)");
            Matcher m = p.matcher(_input);
            if(m.find()){
                str[1] = m.group(1);
            }
        }

    }

    private static void ParseBrowser(String _input) {
        // 解析浏览器
        if(browserConfProp.isEmpty()){
            browserConfProp = getProperties("/com/angejia/hadoop/hive_udf/config/browser_parser_conf.properties");
        }
        @SuppressWarnings("rawtypes")
        Enumeration en = browserConfProp.keys();
            while (en.hasMoreElements()) {
                String broeser_name = en.nextElement().toString();
                String broeser_regexp = browserConfProp.getProperty(broeser_name);
                if (!str[2].equalsIgnoreCase(broeser_name)) {
                    Pattern p = Pattern.compile(broeser_regexp);
                    Matcher m = p.matcher(_input);
                    if (m.find()) {
                        str[2] = broeser_name;
                        str[3] = m.group(1);
                        break;//MobileSafari这个配置在最后一个，所以可以break掉
                    }
                }
            }

        //统一浏览器名称
        if ("Mobile Safari".equals(str[2])) {
            str[2] = "MobileSafari";
        } else if ("BaiduBoxApp".equals(str[2])) {
            str[2] = "baiduboxapp";
        } else if ("UCbrowser".equals(str[2])) {
            str[2] = "UCBrowser";
        } else if ("UC Browser".equals(str[2])) {
            str[2] = "UCBrowser";
        }
        // 格式化浏览器版本
        String version = str[3];
        if(version.startsWith("Version")){
            str[3] = version.replace("Version ", "");
        }

        Pattern pVersion = Pattern.compile("(^\\d+\\.\\d+)");
        Matcher mVersion = pVersion.matcher(version);
        if (mVersion.find()) {
            str[3] = mVersion.group(1);
        }
    }

    /**
     * 获取配置文件的内容
     *
     * @param slaveConfigPath
     * @return
     */
    private static Properties getProperties(String parserBrowserConfigPath) {
        Properties prop = new Properties();
        try {
            prop.load(ParseUserAgentBak.class
                    .getResourceAsStream(parserBrowserConfigPath));
        } catch (IOException e) {
            e.printStackTrace();

        }
        return prop;
    }
}
