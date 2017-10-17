package WC;

import org.apache.hadoop.mapreduce.Mapper;

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
import org.apache.jasper.tagplugins.jstl.core.ForEach;

public class Q3 {

	public static double median(double[] m) {
		int middle = m.length / 2;
		if (m.length % 2 == 1) {
			return m[middle];
		} else {
			return (m[middle - 1] + m[middle]) / 2.0;
		}
	}

	public static class Tokeniizermapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

		private Text atrtistID = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");
			try {
				if (key.get() != 0) {

					String songID = "";

					float fltdanceability = 0;

					songID = datasplit[43];

					String strDancebility = "";
					strDancebility = datasplit[21];

					boolean arrayFound = false;
					if (strDancebility.contains("[")) {
						arrayFound = true;
					}

					if (arrayFound) {
						float avgDbality = 0;
						float avgSumDbality = 0;
						String[] listDbality = null;
						ArrayList<Float> floatDbality = new ArrayList<Float>();

						strDancebility = strDancebility.trim().replaceAll("\\[", "").replaceAll("\\]", "");
						listDbality = strDancebility.split(",");

						if (listDbality.length > 0) {
							for (int i = 0; i < listDbality.length; i++) {
								floatDbality.add(Float.parseFloat(listDbality[i]));

							}

							if (floatDbality.size() > 0) {
								for (Float float1 : floatDbality) {
									avgSumDbality += float1;
								}

								avgDbality = avgSumDbality / (floatDbality.size() - 1);
							}
						}
						FloatWritable dancability = new FloatWritable();
						dancability.set(avgDbality);

						context.write(new Text("song-dancability"), dancability);
					} else {
						fltdanceability = Float.parseFloat(datasplit[21]);
						FloatWritable dancability = new FloatWritable();
						dancability.set(fltdanceability);
					//	if (fltdanceability != 0.0)
						context.write(new Text("song-dancability"), dancability);
					}

				}
			} catch (Exception ex) {
				System.out.println("Exception for key " + key + " " + ex.getMessage());
			}

		}

	}

	public static class IntSumReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			FloatWritable result = new FloatWritable();
			ArrayList<Float> daceabilityList = new ArrayList<Float>();

			int totalcounter = 0;
			for (FloatWritable val : values) {

				daceabilityList.add(val.get());
				totalcounter += 1;
			}

			Collections.sort(daceabilityList);
			int size = daceabilityList.size();

			Float median;

			if (size % 2 == 0) {
				int half = size / 2;

				median = daceabilityList.get(half);
			} else {
				int half = (size + 1) / 2;
				median = daceabilityList.get(half - 1);
			}

			result.set(median);

			context.write(new Text("Tatal median dancabilty across  all songs: "), result);

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q3");
		job.setJarByClass(Q3.class);
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
