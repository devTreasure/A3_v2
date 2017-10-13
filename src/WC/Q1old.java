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

public class Q1old {

	public static class Tokeniizermapper extends Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text atrtistID = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");

			if (key.get() != 0) {
				System.out.println("artist");
				System.out.println(datasplit[11]);
				String strGenere = datasplit[9];
				if (strGenere.length() >= 1) {
					strGenere = strGenere.substring(1, datasplit[9].length() - 1);
					strGenere = strGenere.replace("\"", "");
					// System.out.println(strGenere);

					if (strGenere.contains("[")) {
						strGenere = strGenere.replaceAll("\\[|\\]", "");
					}

					String[] strFGenere = strGenere.split(",");

					String strmbTagFrequency = datasplit[10].substring(1, datasplit[10].length() - 1);
					// System.out.println(strmbTagFrequency);

					String[] strmbtagsCount = strmbTagFrequency.split(",");

					// System.out.println(strmbtagsCount[0]);

					int[] mbtagFre = new int[strmbtagsCount.length];
					System.out.println(strmbtagsCount.length);
					if (mbtagFre.length >= 1 && strmbtagsCount.length >= 1) {

						for (int i = 0; i < strmbtagsCount.length; i++) {
							// System.out.println(strmbtagsCount[i]);
							if (!(strmbtagsCount[i].trim().isEmpty()))
								mbtagFre[i] = Integer.parseInt(strmbtagsCount[i].trim());
						}

						// System.out.println(strmbTagFrequency);

						for (int i = 0; i < strFGenere.length; i++) {
							// System.out.println(strFGenere);

							// context.write(atrtistID.set(datasplit[1].toString()+string), one);
							// context.write(new Text(datasplit[1].toString()+string) , one);

							if (mbtagFre.length >= 1)
								if (mbtagFre[i] != 0) {
									System.out.println(datasplit[11] + "--" + strFGenere[i] + ":" + mbtagFre[i]);
									context.write(new Text(datasplit[11]), new Text(strFGenere[i] + ":" + mbtagFre[i]));
								}

						}
					}

				}
			}

		}

	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int sum = 0;

			int findsamecount = 1;

			ArrayList<Integer> tagFrq = new ArrayList<Integer>();
			ArrayList<String> strGenre = new ArrayList<String>();

			for (Text val : values)

			{
				// System.out.println(val);
				String[] str = val.toString().split(":");
				System.out.println(key);
				if (str.length > 1) {
					strGenre.add(str[0]);
					tagFrq.add(Integer.parseInt(str[1]));
					// context.write(key, new Text(str[1].trim()));
				}
			}

			boolean allEqual = tagFrq.stream().distinct().limit(2).count() <= 1;

			if (tagFrq.size() == 1 || allEqual)

				for (String string : strGenre) {
					context.write(key, new Text(string));
				}

			else {
				// how to short both array same time
				Map<Integer, String> dictionary = new HashMap<Integer, String>();

				for (int i = 0; i < tagFrq.size(); i++) {
					dictionary.put(tagFrq.get(i), strGenre.get(i));
				}

				if (dictionary.size() > 0) {
					// context.write(dictionary, new Text(string));
					for (String k : dictionary.values()) {
						context.write(key, new Text(k));
					}
				}
			}
		}

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Word Count");
			job.setJarByClass(Q1old.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

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
