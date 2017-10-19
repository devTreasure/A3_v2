package WC;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q8 {

	public static class Tokeniizermapper extends Mapper<LongWritable, Text, Text, IntWritable> {

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());
			String[] datasplit = value.toString().split("\t");

			if (key.get() != 0) {
				// System.out.println("artist");
				// System.out.println(datasplit[11]);
				String strGenere = datasplit[9];
				if (strGenere.length() >= 1) {

					strGenere = strGenere.substring(1, datasplit[9].length() - 1);
					strGenere = strGenere.replace("\"", "");

					if (strGenere.contains("[")) {
						strGenere = strGenere.replaceAll("\\[|\\]", "");
					}

					String[] strFGenere = strGenere.split(",");

					String strmbTagFrequency = datasplit[10].substring(1, datasplit[10].length() - 1);

					String[] strmbtagsCount = strmbTagFrequency.split(",");

					int[] mbtagFre = new int[strmbtagsCount.length];

					if (mbtagFre.length >= 1 && strmbtagsCount.length >= 1) {

						for (int i = 0; i < strmbtagsCount.length; i++) {
							if (!(strmbtagsCount[i].trim().isEmpty()))
								mbtagFre[i] = Integer.parseInt(strmbtagsCount[i].trim());
						}
						Pattern aplhanmer = Pattern.compile("^[0-9a-zA-Z]+$");
						Pattern guidString = Pattern.compile(
								"^(\\{){0,1}[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}(\\}){0,1}$");
						Pattern DigitsInString = Pattern.compile("\\d");

						for (int i = 0; i < strFGenere.length; i++) {

							if (mbtagFre.length >= 1)
								if (mbtagFre[i] != 0) {

									String strgenere = strFGenere[i];


									Matcher matcherGuid = guidString.matcher(strGenere);
									Matcher matcherdigits = DigitsInString.matcher(strGenere);

									boolean isGUID = false;
									isGUID = matcherGuid.find();
									boolean isnumeric = matcherdigits.find();

									if (!isGUID && !isnumeric)
										context.write(new Text(strFGenere[i].trim()), new IntWritable(mbtagFre[i]));

								}

						}
					}

				}
			}

		}

	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		//ArrayList<Genere> lstGenere = new ArrayList<>();
		TreeMap<Integer, String> lstGenere = new TreeMap<>(Collections.reverseOrder());
		
		@Override
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException{
			int sum = 0;

			for (IntWritable val : values) {
				sum += val.get();

			}
			
			lstGenere.put(sum, key.toString());
		
		
		

		
		

	}
		
		
		@SuppressWarnings("unchecked")
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			int iterateCount = 0;
		
			
			int mycounter=0;
			for(Map.Entry<Integer,String> entry : lstGenere.entrySet()) {
				  Integer INTkeys = entry.getKey();
				  String value = entry.getValue();
				  if( mycounter < 10)
				  {
				    if(mycounter>10) break;
				     context.write(new Text(value), new IntWritable(INTkeys));
				  }
				  mycounter+=1;
				}
		}
	}
	
	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q8");
		job.setJarByClass(Q8.class);
		job.setMapperClass(Tokeniizermapper.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
