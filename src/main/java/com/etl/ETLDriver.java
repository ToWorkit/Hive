package com.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class ETLDriver implements Tool {

    // 配置conf
    private Configuration configuration;

    @Override
    public int run(String[] args) throws Exception {
        // 1. 获取job对象
        Job job = Job.getInstance();

        // 2. 封装driver类
        job.setJarByClass(ETLDriver.class);

        // 3. 关联Mapper类
        job.setMapperClass(ETLMapper.class);

        // 4. Mapper 输出KV类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        // 5. 最终输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        // 6. 输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 没有reduce时设置
        job.setNumReduceTasks(0);

        // 7. 提交任务
        boolean result = job.waitForCompletion(true);

        return result ? 0 : 1;
    }

    @Override
    public void setConf(Configuration conf) {
        configuration = conf;
    }

    @Override
    public Configuration getConf() {
        return null;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new ETLDriver(), args);
        System.out.println(run);
    }
}
