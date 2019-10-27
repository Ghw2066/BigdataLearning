/*
Author: Haowen Guo
Created Time: 2019-10-23 18:55
Last Modified: 2019-10-27 20:57

Explanation:
This solution contains two jobs and each of them contains one mapper and one reducer respectively. 

The first mapper in job one, MapperOne, receives the key and value from raw data by line, as well
as regards the offset as the key with LongWritable type and the text of this line as the value with
Text type. Over the period of this MapperOne,  userID is sent to ReducerOne as Text key, as well as
movie and rate are stored in a custom Writable object---MovieRateWritable.

The first reducer in job one, ReducerOne, after receiving the key and values from MapperOne, adds 
iterable values into "movieRateList". By the way, I need to create a new object to avoid reuse. 
Then, sort this "movieRateList" by ascending according to "movie", in case the situation of (M1, M2) 
and (M2, M1) happen. Lastly, create movie pair and corresponding  (user, rate1, rate2) by double 
loops, and write movie pair and (user, rate1, rate2) as key and value respectively into "out1" file, 
which is an intermediate data file, also as an input file for the second job.

The second job is quite simple. During the MapperTwo phase, just split the whole Text value into two 
parts----parts[0]: movie pair and parts[1]: (user, rate1, rate2). Over the process of ReducerTwo, 
after the stage of shuffle, all movie pairs with the same hashcode would be sent to one reduce task, 
so just add the complex object("userRates": is a custom writable to store (user, rate1, rate2)) into 
a string ArrayList one by one. Then, use a loop to format the output of list by 
"StringBuffer.append()" which is a cost-effective way, better than "+". Finally, write the 
key(movie pair) and result(userRatesList) into "out2" file.


*/

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class AssigOnez5251113 {

    //literally contain the string value of movie and the integer value of rate,
    //which are corresponding to each other.
    //Also, override the toString() method to generate "movie,rate" to convert to reducer.
    public static class MovieRateWritable implements Writable{

	    private String movie;
	    private int rate;

	    public MovieRateWritable(){

        }

	    public MovieRateWritable(String movie, int rate){
	        this.movie = movie;
	        this.rate = rate;
        }


        public String getMovie() {
            return movie;
        }

        public void setMovie(String movie) {
            this.movie = movie;
        }

        public int getRate() {
            return rate;
        }

        public void setRate(int rate) {
            this.rate = rate;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(movie);
	        dataOutput.writeInt(rate);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.movie = dataInput.readUTF();
            this.rate = dataInput.readInt();
        }

        @Override
        public String toString() {
            return  movie + ":"+ rate;
        }
    }

    //Literally contain "String user, int rate1, int rate2" to satisfy the requirement of output value.
    //Also override the toString() method to generate "(user,rate1,rate2)"
    public static class userRates implements Writable{

	    private String user;
	    private int rate1;
	    private int rate2;

	    public userRates() {
        }

        public userRates(String user, int rate1, int rate2){
	        this.user = user;
	        this.rate1 = rate1;
	        this.rate2 = rate2;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }

        public int getRate1() {
            return rate1;
        }

        public void setRate1(int rate1) {
            this.rate1 = rate1;
        }

        public int getRate2() {
            return rate2;
        }

        public void setRate2(int rate2) {
            this.rate2 = rate2;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
            dataOutput.writeUTF(user);
            dataOutput.writeInt(rate1);
            dataOutput.writeInt(rate2);
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            this.user = dataInput.readUTF();
            this.rate1 = dataInput.readInt();
            this.rate2 = dataInput.readInt();
        }

        @Override
        public String toString() {
            return  "(" + user + "," + rate1 + "," + rate2 + ')';
        }
    }
    
    public static class MapperOne extends Mapper<LongWritable, Text, Text, MovieRateWritable>{

        @Override
        protected void map(LongWritable key, Text value,
                           Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("::");
            MovieRateWritable mr = new MovieRateWritable();
            mr.setMovie(parts[1]);
            mr.setRate(Integer.parseInt(parts[2]));
            context.write(new Text(parts[0]), mr);   //"user" as key, "movie,rate" as value
        }
    }

    public static class ReducerOne extends Reducer<Text, MovieRateWritable, Text, Text>{

        @Override
        protected void reduce(Text user, Iterable<MovieRateWritable> values,
                              Context context) throws IOException, InterruptedException {

            ArrayList<MovieRateWritable> movieRateList = new ArrayList<>();
            
            for(MovieRateWritable movieRate: values) {
                movieRateList.add(new MovieRateWritable(movieRate.getMovie(), movieRate.getRate()));
		//create a new object to store the temp value.
            }
	    //sort the list by the value of movie, in case the situation of (M1,M2) and (M2,M1) happen.
            Collections.sort(movieRateList, new Comparator<MovieRateWritable>() {
                @Override
                public int compare(MovieRateWritable movieRateWritable, MovieRateWritable t1) {
                    return movieRateWritable.getMovie().compareTo(t1.getMovie());
                }
            });
	    //generate movie pair and coresponding (user,rate1,rate2)
            for(int i=0; i<movieRateList.size()-1; i++) {
                for(int j=i+1;j<movieRateList.size();j++) {
                    context.write(new Text("("+movieRateList.get(i).getMovie()+"," +
                                    movieRateList.get(j).getMovie()+")"),
                            new Text(user.toString()+","+movieRateList.get(i).getRate()+"," +
                                    movieRateList.get(j).getRate()));
                }
            }

        }

    }
    
    public static class MapperTwo extends Mapper<LongWritable, Text, Text, Text>{
	//split the whole Text value into movie pair and (user,rate1,rate2)
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split("\t");
            context.write(new Text(parts[0]), new Text(parts[1]));
	    
        }
    }

    public static class ReducerTwo extends Reducer<Text, Text, Text, Text>{
	//add userRates object into a list and output it.
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            
            ArrayList<String> userRatesList = new ArrayList<>();
	    userRates urs = new userRates();

	    String[] parts;
            for(Text temp:values){
                parts = new Text(temp).toString().split(",");
                urs.setUser(parts[0]);
                urs.setRate1(Integer.parseInt(parts[1]));
                urs.setRate2(Integer.parseInt(parts[2]));
                userRatesList.add(urs.toString());                
            }
	    //StringBuffer.append() could save time in comparison to "+".
            StringBuffer buf = new StringBuffer("[");

            for(int i=0; i < userRatesList.size()-1; i++) {
                buf.append(userRatesList.get(i).toString());
                buf.append(",");
            }
            buf.append(userRatesList.get(userRatesList.size()-1).toString());
            buf.append("]");
            String result = buf.toString();

            context.write(key, new Text(result));

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Path out = new Path(args[1]);

        Job job1 = Job.getInstance(conf, "Job One");
        job1.setJarByClass(AssigOnez5251113.class);
        job1.setMapperClass(MapperOne.class);
        job1.setReducerClass(ReducerOne.class);
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(MovieRateWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path(out,"out1"));  //out1 stores the intermediate data
        if (!job1.waitForCompletion(true)) {
            System.exit(1);
        }

        Job job2 = Job.getInstance(conf, "Job Two");
        job2.setJarByClass(AssigOnez5251113.class);
        job2.setMapperClass(MapperTwo.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setReducerClass(ReducerTwo.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job2, new Path(out, "out1"));
        FileOutputFormat.setOutputPath(job2, new Path(out, "out2"));  //out2 stores the final result, pls check this file.
        if (!job2.waitForCompletion(true)) {
            System.exit(1);
        }

    }
}

