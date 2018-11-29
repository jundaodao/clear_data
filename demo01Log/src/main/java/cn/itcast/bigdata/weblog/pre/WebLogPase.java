package cn.itcast.bigdata.weblog.pre;

import cn.itcast.bigdata.weblog.mrbean.WebLogBean;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashSet;
import java.util.Locale;

public class WebLogPase {
    //美国时间格式
    public static SimpleDateFormat df1 = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    //中国时间格式
    public static SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss", Locale.US);

    public static WebLogBean parse(String line) {
        WebLogBean webLogBean = new WebLogBean();
        //通过空格对一行数据进行切割
        String[] arr = line.split(" ");
        //对数组长度小于11的直接去掉
        if (arr.length > 11) {
            //获取里面的每一个元素，将其封装到webLogBean对象里面
            webLogBean.setRemot_addr(arr[0]); //访客ip地址
            webLogBean.setRemote_user(arr[1]); //访客用户信息
            //将时间转变成我们容易识别的时间[18/Sep/2013:06:52:32 +0000]
            String time_local = formDate(arr[3].substring(1));
            //补全日期数据
            if (time_local == null || "".equals(time_local)) {
                time_local = "-invalid_time-";
            }
            webLogBean.setTime_local(time_local);
            webLogBean.setRequest(arr[6]);
            webLogBean.setStatus(arr[8]);
            webLogBean.setBody_bytes_sent(arr[9]);
            webLogBean.setHttp_referer(arr[10]);
            //剩下的切的乱七八糟的都是客户端信息
            if (arr.length > 12) {
                StringBuilder sb = new StringBuilder();
                for (int i = 11; i < arr.length; i++) {
                    sb.append(arr[i]);
                }
                webLogBean.setHttp_user_agent(sb.toString());
            } else {
                webLogBean.setHttp_user_agent(arr[11]);
            }

            //虽然长度达到了，但是未获取到时间，我们就认为这条数据无效
            if ("-invalid_time-".equals(webLogBean.getTime_local())) {
                webLogBean.setValid(false);
            }
            //虽然前面都满足，但是如果请求的状态码大于400，我们也认为他是无效的
            if (Integer.parseInt(webLogBean.getStatus()) > 400) {
                webLogBean.setValid(false);
            }
            //其他的我们就认为数据是有效的
            webLogBean.setValid(true);
        } else {
            //如果切出来长度小于11，我们直接将此条数据丢弃
            webLogBean = null;
        }
        return webLogBean;

    }

    //格式化时间的方法
    private static String formDate(String USDate) {
        try {
            return df2.format(df1.parse(USDate));
        } catch (ParseException e) {
            return null;
        }
    }







    //自定义一个方法，过滤js/图片/css等静态资源
    public static void filtStaticResource(WebLogBean webLogBean, HashSet pages) {
        if(!pages.contains(webLogBean.getRequest())){
           webLogBean.setValid(false);
        }

    }
}

