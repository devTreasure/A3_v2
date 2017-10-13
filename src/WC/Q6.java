package WC;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q6 {

	public static class Tokeniizermapper extends Mapper<LongWritable, Text, Text, FloatWritable> {
		private final static FloatWritable loudness = new FloatWritable();
		private Text atrtistID = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");

			if (key.get() != 0) {

				// System.out.println(datasplit[43]);
				// System.out.println(datasplit[47]);

				String year = "";
				float loudness_val = 0;

				year = datasplit[53];

				loudness_val = Float.parseFloat(datasplit[27]);

				loudness.set(loudness_val);

				System.out.println(year + ":" + loudness_val);

				context.write(new Text(year), loudness);
			}

		}

	}

	public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, DoubleWritable> {
		private DoubleWritable result = new DoubleWritable();

		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {

			int counter_for_AverageCalc = 0;
			float total_loudness = 0;
			ArrayList<Float> loudnesscollection = new ArrayList<Float>();
			for (FloatWritable val : values) {

				counter_for_AverageCalc += 1;
				total_loudness += val.get();
				loudnesscollection.add(val.get());
			}

			float average_loudness = total_loudness / counter_for_AverageCalc;

			// double finalVar =vari1*vari1/ counter_for_AverageCalc ;

			Double sum2 = 0.0;
			for (int i = 0; i < loudnesscollection.size(); i++) {
				Double d = Math.pow(loudnesscollection.get(i) - average_loudness, 2);

				sum2 += d;
			}
			Double variance=0.0;
			if(loudnesscollection.size()>1)
			{
				 variance = sum2 / (loudnesscollection.size() - 1);
			}

			//System.out.println("sum2"+sum2 +" "+loudnesscollection.size());
			context.write(new Text("Total variance for year : " + key.toString()), new DoubleWritable(variance));

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q6");
		job.setJarByClass(Q6.class);
		job.setMapperClass(Tokeniizermapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(DoubleWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
