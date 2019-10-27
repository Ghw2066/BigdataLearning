
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;


public class Tablejoin {

    public static class stdMarkWritable implements Writable {

        private Text name;
        private IntWritable Assign1;
        private IntWritable Assign2;

        public stdMarkWritable(){
            this.name = new Text("");
            this.Assign1 = new IntWritable(-1);
            this.Assign2 = new IntWritable(-1);
        }

        public stdMarkWritable(Text name, IntWritable assign1, IntWritable assign2) {
            this.name = name;
            Assign1 = assign1;
            Assign2 = assign2;
        }

        public Text getName() {
            return name;
        }

        public void setName(Text name) {
            this.name = name;
        }

        public IntWritable getAssign1() {
            return Assign1;
        }

        public void setAssign1(IntWritable assign1) {
            Assign1 = assign1;
        }

        public IntWritable getAssign2() {
            return Assign2;
        }

        public void setAssign2(IntWritable assign2) {
            Assign2 = assign2;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            this.name.write(dataOutput);
            this.Assign1.write(dataOutput);
            this.Assign2.write(dataOutput);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.name.readFields(dataInput);
            this.Assign1.readFields(dataInput);
            this.Assign2.readFields(dataInput);
        }

        @Override
        public String toString() {
            return  this.name.toString() + " " +
                    this.Assign1.toString() + " " +
                    this.Assign2.toString() ;
        }
    }

    public static class StudentMapper extends Mapper<LongWritable, Text, Text, stdMarkWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String parts[] = value.toString().split(",");
            stdMarkWritable val = new stdMarkWritable();
            val.setName(new Text(parts[1]));
            context.write(new Text(parts[0]), val);
        }
    }

    public static class MarkMapper extends Mapper<LongWritable, Text, Text, stdMarkWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String parts[] = value.toString().split(",");
            stdMarkWritable val = new stdMarkWritable();
            val.setAssign1(new IntWritable(Integer.parseInt(parts[1])));
            val.setAssign2(new IntWritable(Integer.parseInt(parts[2])));
            context.write(new Text(parts[0]), val);
        }
    }

    public static class MyReducer extends Reducer<Text, stdMarkWritable, Text, stdMarkWritable>{
        @Override
        protected void reduce(Text key, Iterable<stdMarkWritable> values, Context context) throws IOException, InterruptedException {

            stdMarkWritable value = new stdMarkWritable();

            for(stdMarkWritable a: values)
                if(a.getName().toString().equals("")){
                    value.setAssign1(new IntWritable(a.getAssign1().get()));
                    value.setAssign2(new IntWritable(a.getAssign2().get()));
                }else {
                    value.setName(new Text(a.getName().toString()));
                }

            context.write(key, value);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Table join");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(stdMarkWritable.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(stdMarkWritable.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, StudentMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, MarkMapper.class);

        job.setReducerClass(MyReducer.class);
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);

    }
}
