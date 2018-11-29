package cn.itcast.bigdata.weblog.clickstream.pageview.vist;

import cn.itcast.bigdata.weblog.mrbean.PageVistBean;
import cn.itcast.bigdata.weblog.mrbean.VistBean;
import cn.itcast.bigdata.weblog.utils.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.net.URI;

public class ClickStreamVistMain extends Configured implements Tool {
    @Override
    public int run(String[] strings) throws Exception {
        //获取job
        Job job = Job.getInstance(super.getConf());
        //本地模式运行开始--------------------------------------------
        //输入文件
        //TextInputFormat.addInputPath(job,new Path("file:///D:\\大数据课程\\day08_sqoop\\8、大数据离线第八天\\日志文件数据\\pageview"));
        //输入文件类型
        //job.setInputFormatClass(TextInputFormat.class);
        //输出文件
        //TextOutputFormat.setOutputPath(job,new Path("file:///D:\\大数据课程\\day08_sqoop\\8、大数据离线第八天\\日志文件数据\\vist"));
        //输出文件格式
        //job.setOutputFormatClass(TextOutputFormat.class);
        //本地模式运行结束--------------------------------------------
        //集群模式运行开始
         String inputPath="hdfs://node01:8020/weblog/"+ DateUtil.getYesterday()+"/pageview";
         String outputPath="hdfs://node01:8020/weblog/"+DateUtil.getYesterday()+"" +
          "/visit";
         //获取文件系统
        FileSystem fileSystem = FileSystem.get(new URI("hdfs:///node01:8020"), super.getConf());
        if(fileSystem.exists(new Path(outputPath))){
            fileSystem.delete(new Path(outputPath),true);
        }
        fileSystem.close();
        //定义输入路径
        FileInputFormat.setInputPaths(job,new Path(inputPath));
        //定义输入文件类型
        job.setInputFormatClass(TextInputFormat.class);
        //定义输出路径
        FileOutputFormat.setOutputPath(job,new Path(outputPath));
        job.setOutputFormatClass(TextOutputFormat.class);
        //定义打包方式
        job.setJarByClass(ClickStreamVistMain.class);
        //配置mapper
        job.setMapperClass(ClickStreamVistMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PageVistBean.class);
        //配置reduce
        job.setReducerClass(ClickStreamVistReduce.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(VistBean.class);
        //开启
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new Configuration(), new ClickStreamVistMain(), args);
        System.exit(run);
    }
}
