package cn.itcast.bigdata.weblog.pre;


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

/*
* 处理原始日志，过滤出真实pv,ip,转换时间格式，对缺失字段填充默认值，对记录标记有效和无效
* */
public class WebLogPreProcessMain extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        //获取job
        Job job = Job.getInstance(super.getConf());
        //本地读取模式开始------------------------------------------------------------
        //输入文件
        //FileInputFormat.addInputPath(job,new Path("file:///D:\\大数据课程\\day08_sqoop\\8、大数据离线第八天\\日志文件数据\\input\\"));
        //输入文件类型
        //job.setInputFormatClass(TextInputFormat.class);
        //输出文件
        //FileOutputFormat.setOutputPath(job,new Path("file:///D:\\大数据课程\\day08_sqoop\\8、大数据离线第八天\\日志文件数据\\output\\"));
        //        输出文件类型
        //job.setOutputFormatClass(TextOutputFormat.class);
        //本地读取模式结束------------------------------------------------------------
        //集群模式运行如下*****************************************
        String inputPath="hdfs://node01:8020/weblog/"+ DateUtil.getYesterday()+"/input";
        String outputPath="hdfs://node01:8020/weblog/"+ DateUtil.getYesterday()+"/output";
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://node01:8020"), super.getConf());
        if(fileSystem.exists(new Path(outputPath))){  //输出路径存在的情况下
            fileSystem.delete(new Path(outputPath),true);  //先删除路径
        }
        fileSystem.close();
        //设置输入路径
        FileInputFormat.setInputPaths(job,new Path(inputPath));
        //输入的文件类型
        job.setInputFormatClass(TextInputFormat.class);
        //设置输出路径
        FileOutputFormat.setOutputPath(job,new Path(outputPath));
        //设置输出文件类型
        job.setOutputFormatClass(TextOutputFormat.class);
        //设置打包方式jar包
        job.setJarByClass(WebLogPreProcessMain.class);
        //mapper
        job.setMapperClass(WebLogPreProcessMapper.class);
        //设置K1，v1输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //没有reduce,reduce数量设置为0
        job.setNumReduceTasks(0);
        boolean b = job.waitForCompletion(true);

        return b?0:1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf, new WebLogPreProcessMain(), args);
        System.out.println(run);
        System.exit(run);
    }
}
