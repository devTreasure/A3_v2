package WC;



import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q4OLD {

	public static double median(double[] m) {
	    int middle = m.length/2;
	    if (m.length%2 == 1) {
	        return m[middle];
	    } else {
	        return (m[middle-1] + m[middle]) / 2.0;
	    }
	}
	

	public static class Tokeniizermapper extends Mapper<LongWritable, Text, FloatWritable,Text > {
		private final static FloatWritable hottness = new FloatWritable();
		private Text artistName = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");

			if (key.get() != 0) {
				
		
				String artist = "";
				
				float songHotttness = 0;

				artist = datasplit[11];
				
				songHotttness = Float.parseFloat(datasplit[41]);

				hottness.set(songHotttness);
				artistName.set(artist);
				
				context.write(hottness, artistName);
			}

		}

	}

	public static class IntSumReducer extends Reducer<FloatWritable, Text ,Text, FloatWritable> {
		
	
		}
		

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(Q4OLD.class);
		job.setMapperClass(Tokeniizermapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
