package cn.itcast.bigdata.weblog.pre;

import cn.itcast.bigdata.weblog.mrbean.WebLogBean;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.HashSet;

public class WebLogPreProcessMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
    HashSet<String> pages = new HashSet<>();
    Text k=new Text();
    //过滤日志文件中的静态资源，初始化
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        pages.add("/about");
        pages.add("/black-ip-list/");
        pages.add("/cassandra-clustor/");
        pages.add("/finance-rhive-repurchase/");
        pages.add("/hadoop-family-roadmap/");
        pages.add("/hadoop-hive-intro/");
        pages.add("/hadoop-zookeeper-intro/");
        pages.add("/hadoop-mahout-roadmap/");
    }

    //处理将输入的内容转变成k1,v1
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //获取一行数据
        String line = value.toString();
        //对一行的数据进行整理，比如切割，补全,获取到格式化的数据
        WebLogBean bean = WebLogPase.parse(line);
        if(bean!=null){
            //过滤js/图片/css等静态资源
            WebLogPase.filtStaticResource(bean,pages);
            //写出k1,v1
            k.set(bean.toString());
            context.write(k,NullWritable.get());
        }

    }
}
