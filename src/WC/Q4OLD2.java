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
import org.joda.time.DateTime;
import org.apache.hadoop.mapreduce.Mapper;

public class Q4OLD2 {

	public static class MyMapper extends Mapper<LongWritable, Text, FloatWritable, Text> {
		Pattern regex = Pattern.compile("[,|]");
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String[] datasplit = value.toString().split("\t");
			
			
			if (key.get() != 0) {

				Pattern regex = Pattern.compile("[,|]");

				Matcher matcher = regex.matcher(datasplit[47].toString().trim());

				if (matcher.find()) {

					String newstr = datasplit[47].toString().trim().replaceAll("\\[", "").replaceAll("\\]", "");
					System.out.println(newstr);
				
					String[] finalS = newstr.split(",");
				
					float[] int_tempo = new float[finalS.length];
					System.out.println(finalS.length);
					float sum = 0;
					float avg_tempoo = 0;

					if (finalS.length > 0) {
						for (int i = 0; i < finalS.length; i++) {
							System.out.println(finalS[i]);
							int_tempo[i] = Float.parseFloat((finalS[i]));
						}

						for (int j = 0; j < int_tempo.length; j++) {
							sum += int_tempo[j];
						}

						avg_tempoo = sum / int_tempo.length;
					    System.out.println(avg_tempoo);
					    if(avg_tempoo!=0.0)
						 context.write(new FloatWritable(avg_tempoo), new Text(datasplit[11].toString().trim()));
						
					}

				} else {
					FloatWritable f = new FloatWritable();
					System.out.println(f.toString() + ":" +datasplit[11].toString().trim());
					float val1=Float.parseFloat(datasplit[47].toString().trim());
					f.set(val1);

					Text t = new Text();
					t.set(datasplit[11].toString().trim());
					if(val1!=0.0)
					 context.write(f, t);
				}

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

			Job job = Job.getInstance(conf, "Q4");

			job.setJarByClass(Q4OLD2.class);

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
