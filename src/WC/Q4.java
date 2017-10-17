package WC;

import java.awt.datatransfer.FlavorTable;
import java.awt.event.FocusAdapter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Console;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.joda.time.DateTime;
import org.apache.hadoop.mapreduce.Mapper;

public class Q4 {

	public static class MyMapper extends Mapper<LongWritable, Text, Text, FloatWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] datasplit = value.toString().split("\t");

			if (key.get() != 0) {

				Pattern regex = Pattern.compile("[,|]");

				Matcher matcher = regex.matcher(datasplit[47].toString().trim());

				if (matcher.find()) {

					String newstr = datasplit[47].toString().trim().replaceAll("\\[", "").replaceAll("\\]", "");
					//System.out.println(newstr);

					String[] finalS = newstr.split(",");

					float[] int_tempo = new float[finalS.length];
					//System.out.println(finalS.length);
					float sum = 0;
					float avg_tempoo = 0;

					if (finalS.length > 0) {
						for (int i = 0; i < finalS.length; i++) {
							//System.out.println(finalS[i]);
							int_tempo[i] = Float.parseFloat((finalS[i]));
						}

						for (int j = 0; j < int_tempo.length; j++) {
							sum += int_tempo[j];
						}

						avg_tempoo = sum / int_tempo.length;
						//System.out.println(avg_tempoo);
						//if (avg_tempoo != 0.0)
						context.write(new Text(datasplit[11].toString().trim()), new FloatWritable(avg_tempoo));

					}

				} else {
					FloatWritable f = new FloatWritable();
					//System.out.println(f.toString() + ":" + datasplit[11].toString().trim());
					float val1 = Float.parseFloat(datasplit[47].toString().trim());
					f.set(val1);

					Text t = new Text();
					t.set(datasplit[11].toString().trim());
					//if (val1 != 0.0)
						context.write(t, f);
				}

			}

		}



	}
	

	public static class MyReducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {

		// HashMap<String, Float> top10ArtistHotttness = new HashMap<String, Float>();

		ArrayList<tempo1> artistGlobalTempoCollection = new ArrayList<tempo1>();

		@SuppressWarnings("unchecked")
		@Override
		public void reduce(Text key, Iterable<FloatWritable> values, Context context)
				throws IOException, InterruptedException {
			ArrayList<tempo1> artistTempoCollection = new ArrayList<tempo1>();

			for (FloatWritable val : values) {
				artistTempoCollection.add(new tempo1(key.toString(), val.get()));

			}

			Collections.sort(artistTempoCollection, new tempo1());

			if (artistTempoCollection.size() > 0) {
				artistGlobalTempoCollection.add(artistTempoCollection.get(0));
			}

			artistTempoCollection.clear();
		}

		@SuppressWarnings("unchecked")
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			Collections.sort(artistGlobalTempoCollection, new tempo1());
			try {
				for (int i = 0; i < 10; i++) {

					{
						context.write(new Text(artistGlobalTempoCollection.get(i).artistName),
								new FloatWritable(artistGlobalTempoCollection.get(i).tempo));

					}
				}
			} catch (Exception ex) {
				System.out.println(ex.getMessage());
			}

		}

	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf, "Word Count");

		job.setJarByClass(Q4.class);

		job.setNumReduceTasks(1);

		job.setMapperClass(MyMapper.class);

		job.setReducerClass(MyReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FloatWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FloatWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
