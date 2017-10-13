package WC;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.Inet4Address;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q7 {

	public static class Tokeniizermapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text atrtistID = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");

			if (key.get() != 0) {
			//	System.out.println("artist");
			//	System.out.println(datasplit[11] + "songid");

				context.write(new Text(datasplit[11].toString()), one);

			}

		}

	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
		       int sum=0;
		       
		       for(IntWritable value : values)
			   {
			     sum += value.get();
			   }
		       
		       context.write(key, new IntWritable(sum));
		}

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Q7");
			job.setJarByClass(Q7.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(IntWritable.class);

			job.setMapperClass(Tokeniizermapper.class);
			// job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(IntSumReducer.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		}
	}
}
