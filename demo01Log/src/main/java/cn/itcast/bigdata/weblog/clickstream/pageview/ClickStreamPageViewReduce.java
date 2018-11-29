package cn.itcast.bigdata.weblog.clickstream.pageview;

import cn.itcast.bigdata.weblog.mrbean.WebLogBean;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class ClickStreamPageViewReduce extends Reducer<Text,WebLogBean,NullWritable,Text> {
    public static final long MINITES = 1800000;
    @Override
    protected void reduce(Text key, Iterable<WebLogBean> values, Context context) throws IOException, InterruptedException {
        ArrayList<WebLogBean> beans = new ArrayList<>();
        //遍历上游发过来的v2,里面都是一个一个的WebLogBean,1个ip地址的bean都在一起了,
        //通过深拷贝，将迭代器中的bean都深拷贝到list集合中
        try {
            for (WebLogBean bean : values) {
                   //将获取到的webLogBean通过深拷贝，拷贝到list集合中
                WebLogBean webLogBean = new WebLogBean();
                try {
                    BeanUtils.copyProperties(webLogBean,bean);//深拷贝
                } catch (Exception e) {
                    e.printStackTrace();
                }
                beans.add(webLogBean);
            }

            //功能：计算有多少个session，也就是一天访问了多少次，如果1个ip访问时间间隔大于30分钟就就是两个session
            //对list集合中的对象按照访问时间进行排序,使用工具类Collectiions
            Collections.sort(beans, new Comparator<WebLogBean>() {
                @Override
                public int compare(WebLogBean o1, WebLogBean o2) {
                    //获取时间，用时间进行比较
                    DateFormat format = SimpleDateFormat.getDateInstance();
                    try {
                        Date d1 = format.parse(o1.getTime_local());
                        Date d2 = format.parse(o2.getTime_local());
                        if(d1==null || d2==null){
                           return 0;
                        }else {
                            return d1.compareTo(d2);
                        }
                    } catch (ParseException e) {
                        e.printStackTrace();
                        return 0;
                    }

                }
            });
            //定义一个step用户标记不同的session
            int step=1;
            //用uuid定义一个session
            String ssession = UUID.randomUUID().toString();
            //对排好序的对象进行时间，就是比较相邻的两条数据的时间差，时间差大于30分钟则认为是2个ssession
            Text v = new Text();
            for(int i=0;i<beans.size();i++){
                WebLogBean bean = beans.get(i);//获取其中一个

                //如果只有1个webLogBean则直接输出,并且设置默认时间60s
                if(beans.size()==1){

                    v.set(ssession+"\001"+key.toString()+"\001"+bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + (60) + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001"+ bean.getStatus());
                    //直接将结果输出k3,v3
                    context.write(NullWritable.get(),v);
                }

                //如果有多个webLogBean则需要判断里面的session时间是否大于30分钟
               if(i==0){continue;}

               //求取相邻两个session的时间差
                SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                try {
                    long t1 = format.parse(beans.get(i).getTime_local()).getTime();
                    long t2 = format.parse(beans.get(i-1).getTime_local()).getTime();
                    long MINITES= 1800000; //30分钟
                    if(t1-t2>MINITES){ //本次减上次时间大于30分钟说明是2个session，step从1开始计算，uuid变化
                        v.set(ssession+"\001"+key.toString()+"\001"+beans.get(i - 1).getRemote_user() + "\001" + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001" + (step) + "\001" + (60) + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
                                + beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent() + "\001" + beans.get(i - 1).getStatus());
                        step=1;//重置step
                        ssession=UUID.randomUUID().toString();//重新生成uuid
                        context.write(NullWritable.get(),v);

                    }else { //小于30分钟 //step需要+1,uuid不变
                        v.set(ssession+"\001"+key.toString()+"\001"+beans.get(i - 1).getRemote_user() + "\001" + beans.get(i - 1).getTime_local() + "\001" + beans.get(i - 1).getRequest() + "\001" + (step) + "\001" +(t1-t2)/1000 + "\001" + beans.get(i - 1).getHttp_referer() + "\001"
                                + beans.get(i - 1).getHttp_user_agent() + "\001" + beans.get(i - 1).getBody_bytes_sent() + "\001" + beans.get(i - 1).getStatus());
                        step++; //step+1
                        context.write(NullWritable.get(),v);
                    }

                    //如果此时i是最后一个，那么i-1就越界了，所以，当i是最后一个时，默认输出自己本身
                    if(i==beans.size()){
                        v.set(ssession+"\001"+key.toString()+"\001"+bean.getRemote_user() + "\001" + bean.getTime_local() + "\001" + bean.getRequest() + "\001" + step + "\001" + (60) + "\001" + bean.getHttp_referer() + "\001" + bean.getHttp_user_agent() + "\001" + bean.getBody_bytes_sent() + "\001"+ bean.getStatus());
                    context.write(NullWritable.get(),v);
                    }

                } catch (ParseException e) {
                    e.printStackTrace();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
