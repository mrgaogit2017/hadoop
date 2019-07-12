package com.jhh.word;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 *
 * MapReduce自定义程序
 *
 * 统计指定文件夹下文件字符出现次数
 *
 * 若在本地跑,还需要本地有Hadoop一套文件并配置到PATH变量
 * @author gaozhaolu
 * @date 2019/7/3 17:26
 */
public class WordCount {
    /**入参个数*/
    public static final Integer PARAMS_SIZE = 2;

    /**Object, Text, Text, IntWritable
     *
     * Object key参数表示文本内容的偏移量, 从0开始
     * Text value表示一行文本的值
     * Context context参数为MapReduce模型Map端的上下文对象
     * 后面2个输出类型可灵活变动
     */
    public static class WordMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 文件内每行内容
            String line = value.toString();
            StringTokenizer itr = new StringTokenizer(line);
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().toLowerCase());

                //Map函数的输出key-value
                context.write(word, one);
            }
        }
    }

    public static class WordReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);

            //Reduce函数的输出key-value
            context.write(key, result);
        }
    }

    /**协助修改最终结果输出文件名*/
    public static class MyOutputFormat extends TextOutputFormat<Text, IntWritable> {
        @Override
        public Path getDefaultWorkFile(TaskAttemptContext context, String extension) throws IOException {
            FileOutputCommitter committer = (FileOutputCommitter) getOutputCommitter(context);
            return new Path(committer.getWorkPath(), getOutputName(context));
        }
    }

    public static void main(String[] args) throws Exception {
        // 指定Hadoop文件系统的输入文件夹
        String defaultInput = "hdfs://172.16.11.207:9000/tpp/";
        //指定Hadoop文件系统的输出文件夹
        String defaultOutput = "hdfs://172.16.11.207:9000/out/";

        Configuration conf = new Configuration();
        // 设置最终结果输出文件名, 默认part-r-00000
        conf.set("mapreduce.output.basename", "result.txt");
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != PARAMS_SIZE) {
            System.err.println("Usage: word count <in> <out>");

            otherArgs = new String[2];
            otherArgs[0] = defaultInput;
            otherArgs[1] = defaultOutput;
        }

        Job job = Job.getInstance(conf,"word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordMapper.class);
        job.setCombinerClass(WordReducer.class);
        job.setReducerClass(WordReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.setOutputFormatClass(MyOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
