package cn.itcast.bigdata.weblog.clickstream.pageview;

import cn.itcast.bigdata.weblog.mrbean.WebLogBean;
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

public class ClickStremPageViewMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //获取job
        Job job = Job.getInstance(super.getConf());
        //本地模式先运行开始--------------------------------------------------
        //输入文件
        // TextInputFormat.addInputPath(job,new Path("file:///D:\\大数据课程\\day08_sqoop\\8、大数据离线第八天\\日志文件数据\\output"));
        //设置输入文件的类型
        //job.setInputFormatClass(TextInputFormat.class);
        //设置输出文件
        //TextOutputFormat.setOutputPath(job,new Path("file:///D:\\大数据课程\\day08_sqoop\\8、大数据离线第八天\\日志文件数据\\pageview2"));
        //设置输出文件类型
        //job.setOutputFormatClass(TextOutputFormat.class);
        //本地模式运行结束--------------------------------------------------
        //集群模式
        String inputPath="hdfs://node01:8020/weblog/"+ DateUtil.getYesterday()+"/output";
        String outputPath="hdfs://node01:8020/weblog/"+DateUtil.getYesterday()+"/pageview";
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), super.getConf());
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
        //定义输出文件类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //打包方式是jar包
        job.setJarByClass(ClickStremPageViewMain.class);
        //设置mapper
        job.setMapperClass(ClickStreamPageViewMapper.class);
        //设置mapper输出的k和v类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(WebLogBean.class);
        //设置reduce
        job.setReducerClass(ClickStreamPageViewReduce.class);
        //设置reduce的输出k和v结果
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        //设置一直读取完成
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf, new ClickStremPageViewMain(), args);
        System.exit(run);
    }
}


