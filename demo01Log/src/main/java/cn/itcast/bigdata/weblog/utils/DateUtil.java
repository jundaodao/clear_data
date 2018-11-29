package cn.itcast.bigdata.weblog.utils;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateUtil {
    //获取昨天的日期
    public static String getYesterday(){
        Calendar instance = Calendar.getInstance();
        instance.add(Calendar.DATE,-1);
        Date time = instance.getTime();
        String yesterday = new SimpleDateFormat("yyyy-MM-dd").format(time);
        return yesterday;
    }
}
