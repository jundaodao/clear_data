package cn.itcast.bigdata.weblog.clickstream.pageview;

import cn.itcast.bigdata.weblog.mrbean.WebLogBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class ClickStreamPageViewMapper extends Mapper<LongWritable,Text,Text,WebLogBean> {
    WebLogBean webLogBean=new WebLogBean();
    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //读取一行数据
        String line = value.toString();
        //按照、\001进行切割
        String[] arr = line.split("\001");
        if(arr.length<9) return;
        //将切分出来的放入对象之中
        webLogBean.setValid("true".equals(arr[0])?true:false);
        webLogBean.setRemot_addr(arr[1]);
        webLogBean.setRemote_user(arr[2]);
        webLogBean.setTime_local(arr[3]);
        webLogBean.setRequest(arr[4]);
        webLogBean.setStatus(arr[5]);
        webLogBean.setBody_bytes_sent(arr[6]);
        webLogBean.setHttp_referer(arr[7]);
        webLogBean.setHttp_user_agent(arr[8]);
        //将ip地址作为k2，v作为对象,做一次聚合
        k.set(webLogBean.getRemot_addr());//以ip作为k
        context.write(k,webLogBean);
    }
}
