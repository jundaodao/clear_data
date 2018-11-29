package cn.itcast.bigdata.weblog.clickstream.pageview.vist;

import cn.itcast.bigdata.weblog.mrbean.PageVistBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class ClickStreamVistMapper extends Mapper<LongWritable,Text,Text,PageVistBean> {
    PageVistBean pageVistBean = new PageVistBean();
    Text k = new Text();
    /*
    * 读取clickStreampageVies的输出结果，将其封装在PageVistBean中，k用
    * private String session;      //session
      private String remot_addr;  //ip地址，远程地址
      private String timestr;      //访问时间
      private String request;      //请求路径
      private int step;            //在同一个session中的步骤
      private String staylong;     //页面停留时长
      private String referal;      //从哪里跳转过来的
      private String useragent;    //客户端详情
      private String bytes_send;   //服务端相应的流量大小
      private String status;        //相应状态码
    * */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] arr = line.split("\001");
        int step = Integer.parseInt(arr[5]);
        //封装
        pageVistBean.setSession(arr[0]);
        pageVistBean.setRemot_addr(arr[1]);
        pageVistBean.setTimestr(arr[3]);
        pageVistBean.setRequest(arr[4]);
        pageVistBean.setStep(step);
        pageVistBean.setStaylong(arr[6]);
        pageVistBean.setReferal(arr[7]);
        pageVistBean.setUseragent(arr[8]);
        pageVistBean.setBytes_send(arr[9]);
        pageVistBean.setStatus(arr[10]);
        //发送k1,v1,用session作为k
        k.set(pageVistBean.getSession());
        context.write(k,pageVistBean);
    }
}
