package cc.hiifong.bigdata;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Author hiifong
 * @Date 2023/6/6 16:55
 * @Email i@hiif.ong
 */
public class MergeTables extends Configured implements Tool {
    public static class Student {
        public String id;
        public String value;

        public Student() {
        }

        public Student(String id, String value) {
            this.id = id;
            this.value = value;
        }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }
    }

    static java.util.Map<String,String> courseInfo = new HashMap<>();
    public static class Map extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void setup(Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            Path path = new Path(cacheFiles[0]);
            FileReader fileReader = new FileReader(path.getName());
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            String line;
            while ((line = bufferedReader.readLine()) != null){
                String[] fields = line.toString().split(",");
                courseInfo.put(fields[0],fields[1]);
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {

            // 1 获取输入文件类型
            FileSplit split = (FileSplit) context.getInputSplit();
            String filename = split.getPath().getName();
            String[] fields = value.toString().split(",");
            if (filename.startsWith("t1")){
                if (!Objects.equals(fields[0], "") && !Objects.equals(fields[1], "") && !Objects.equals(fields[2], "")) {
                    context.write(new Text(fields[0]),
                            new Text(fields[0] + "," + courseInfo.get(fields[1]) + "," + fields[2]));
                }
            }
            if (filename.startsWith("t2")){
                if (!Objects.equals(fields[0], "") && !Objects.equals(fields[1], "")) {
                    context.write(new Text(fields[0]), new Text(fields[0] + "," + fields[1]));
                }
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, NullWritable> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // 成绩表
            List<Student> students1 = new ArrayList<>();
            // 学生表
            List<Student> students2 = new ArrayList<>();

            for (Text text: values){
                Student student = new Student();
                String[] fields = text.toString().split(",");
                if (fields.length == 3){
                    student.setId(fields[0]);
                    student.setValue(fields[1] + "," + fields[2]);
                    students1.add(student);
                }
                // 判断是否是学生信息表
                if (fields.length == 2){
                    student.setId(fields[0]);
                    student.setValue(fields[1]);
                    students2.add(student);
                }
            }
            for (Student stu1: students1){
                for (Student stu2: students2){
                    if (Objects.equals(stu1.getId(), stu2.getId())){
                        context.write(new Text(stu2.getValue() + "," + stu1.getValue()), NullWritable.get());
                    }
                }
            }
        }
    }

    public int run(String[] args) throws Exception {
        // 1 获取配置信息，或者job对象实例
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "MergeTables");

        // 2 指定本程序的jar包所在的本地路径
        job.setJarByClass(MergeTables.class);

        // 3 指定本业务job要使用的Mapper/Reducer业务类
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        // 4 指定mapper输出数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 5 把课程信息表添加到cache中
        job.addCacheFile(new Path(args[0]).toUri());

        // 6 指定最终输出的数据的kv类型
        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));

        // 7 将job中配置的相关参数，以及job所用的java类所在的jar包， 提交给yarn去运行
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        //创建配置文件对象
        Configuration configuration = new Configuration();
        //生成文件系统对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://master:8020"), configuration, "root");
        int length = args.length;
        if (length != 3){
            args = new String[]{ "/input/t3.csv", "/input", "/output" };
            System.out.println("args is none, use default args, args[0] = \"/input/t3.csv\", args[1] = \"/input\", args[2] = \"/output\"");
        }
        // 递归删除
        if (fileSystem.delete(new Path(args[2]),true)) System.out.println("delete: " + args[2] + ", successfully!");
        fileSystem.close();
        int res = ToolRunner.run(new Configuration(), new MergeTables(), args);
        System.exit(res);
    }
}
