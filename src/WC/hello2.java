package WC;






import java.awt.datatransfer.FlavorTable;
import java.awt.event.FocusAdapter;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;

public class hello2 {
	
	public static class MyMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
	
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");

			if (key.get() != 0) {

				Pattern regex = Pattern.compile("[,|]");

				Matcher matcher = regex.matcher(datasplit[47].toString().trim());

				if (matcher.find()) {

					String newstr = datasplit[47].toString().trim().replaceAll("\\[", "").replaceAll("\\]", "");

					String[] finalS = newstr.split(",");
					Integer[] int_tempo = new Integer[finalS.length];
					int sum = 0;
					float avg_tempoo = 0;

					if (finalS.length > 0) {
						for (int i = 0; i < finalS.length; i++) {
							int_tempo[i] = Integer.parseInt(finalS[i]);
						}

						for (int j = 0; j < int_tempo.length; j++) {
							sum += int_tempo[j];
						}

						avg_tempoo = sum / int_tempo.length;

						//context.write(new FloatWritable(avg_tempoo), new Text(datasplit[11].toString().trim()));

					}

				} else {
					FloatWritable f = new FloatWritable();
					f.set(Float.parseFloat(datasplit[47].toString().trim()));

					Text t = new Text();
					t.set(datasplit[11].toString().trim());
					//context.write(f, t);
				}

			}

		}

		public static class MyReducer extends Reducer<FloatWritable, Text, Text, FloatWritable> {

			HashMap<String, Float> top10ArtistHotttness = new HashMap<String, Float>();

			int TOP_10_RECORDS = 0;

			@Override
			public void reduce(FloatWritable key, Iterable<Text> values, Context context)
					throws IOException, InterruptedException {

				for (Text val : values) {

					if (TOP_10_RECORDS < 10) {
						context.write(val, key);
					} else {
						break;
					}

					TOP_10_RECORDS += 1;

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

			job.setMapOutputKeyClass(FloatWritable.class);
			job.setMapOutputValueClass(Text.class);

			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));

			System.exit(job.waitForCompletion(true) ? 0 : 1);

		}
	}
}
