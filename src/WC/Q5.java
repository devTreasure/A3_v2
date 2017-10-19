package WC;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.Text;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q5 {

	public static class Tokeniizermapper extends Mapper<LongWritable, Text, Text, Text> {
		private final static IntWritable one = new IntWritable(1);
		private Text atrtistID = new Text();
		Pattern guidString = Pattern.compile(
				"^(\\{){0,1}[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}(\\}){0,1}$");

		Pattern aplhanmer = Pattern.compile("^[0-9a-zA-Z]+$");

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");

			if (key.get() != 0) {
				// System.out.println("artist");
				// System.out.println(datasplit[11]);
				String strGenere = datasplit[9];
				// System.out.println(strGenere);
				// boolean test = strGenere.equalsIgnoreCase("[]");
				Matcher matcher = guidString.matcher(strGenere);
				// System.out.println(strGenere);

				boolean isGUID = false;
				isGUID = matcher.find();

				if (strGenere != null && strGenere.length() >= 1 && (!isGUID)
						&& (!(strGenere.toString() == "") && (!strGenere.equalsIgnoreCase("[]"))
								&& (datasplit[42] != null && !datasplit[42].trim().equalsIgnoreCase("nan")))) {

					strGenere = strGenere.substring(1, datasplit[9].length() - 1);
					strGenere = strGenere.replace("\"", "");
					// System.out.println(strGenere);

					if (strGenere.contains("[")) {
						strGenere = strGenere.replaceAll("\\[|\\]", "");
					}

					String[] strFGenere = strGenere.split(",");

					String strmbTagFrequency = datasplit[10].substring(1, datasplit[10].length() - 1);

					String[] strmbtagsCount = strmbTagFrequency.split(",");

					int[] tagRating = new int[strmbtagsCount.length];

					if (tagRating.length >= 1 && strmbtagsCount.length >= 1) {

						for (int i = 0; i < strmbtagsCount.length; i++) {

							if (!(strmbtagsCount[i].trim().isEmpty()))
								tagRating[i] = Integer.parseInt(strmbtagsCount[i].trim());
						}

						int[] tagRatingunshorted = Arrays.copyOf(tagRating, tagRating.length);

						Arrays.sort(tagRating);

						boolean isSameVal = false;

						int index = 0;
						int largest = -1;
						if (tagRating.length > 1) {
							if (tagRating[0] == tagRating[tagRating.length - 1]) {
								isSameVal = true;
							}
						}

						if (!isSameVal) {

							for (int k = 0; k < tagRatingunshorted.length; k++) {
								if (tagRatingunshorted[k] > largest) {
									largest = tagRatingunshorted[k];
									index = k;
								}
							}

						}

						if (isSameVal) {
							if (tagRatingunshorted.length > 0) {
								for (int i = 0; i < strFGenere.length; i++) {

									if (tagRating.length >= 1)
										if (tagRating[i] != 0) {
											// System.out.println("key:" + strFGenere[i]);

											// System.out.println(
											// datasplit[11] + "--" + strFGenere[i] + ":" + tagRatingunshorted[i]);
											// String strgenere=strFGenere[i] ;
											String strHotness = datasplit[42];
											strHotness = datasplit[42].trim();
											if (strHotness.contains("[")) {
												System.out.println("#################################");
												System.out.println("Hotness is an araay");
												System.out.println("#################################");
											}

											if (!strHotness.equalsIgnoreCase("0.0")
													&& (!strHotness.equalsIgnoreCase("nan"))) {

												if (strHotness.contains("AR4SBWX1187FB4D6BE)")) {
													System.out.println(
															"***********H9tness wrongly formed****************");
													System.out.println(datasplit[50]);
												}

												// System.out.println(
												// strHotness + ":" + datasplit[11] + ":" + datasplit[50]);

												context.write(new Text(strFGenere[i].toString().trim()), new Text(
														strHotness + ":" + datasplit[11] + ":" + datasplit[50]));
											}
										}

								} // for loop end
							}
						} else {

							String strHotness = datasplit[42];
							strHotness = datasplit[42].trim();
							System.out.println(strHotness);

							if (!strHotness.equalsIgnoreCase("0.0") && (!strHotness.contains("/"))
									&& (!datasplit[11].contains(":")) && (!datasplit[50].contains(":"))
									&& (!strHotness.equalsIgnoreCase("nan"))) {

								// System.out.println("key: " + strFGenere[index] + " -- " + strHotness + ":"
								// + datasplit[11] + ":" + datasplit[50]);

								context.write(new Text(strFGenere[index].toString().trim()),
										new Text(strHotness + ":" + datasplit[11] + ":" + datasplit[50]));
							}
						}

					}

				}
			}

		}

	}

	public static class reducer extends Reducer<Text, Text, Text, Text> {

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			// ArrayList<Hotness> hotnesses = new ArrayList<Hotness>();
			TreeMap<Float, String> hot = new TreeMap<>(Collections.reverseOrder());
			try {

				for (Text val : values)

				{

					// System.out.println(val.toString());
					String[] str = val.toString().split(":");

					boolean allowed = false;
					Float myval = 0f;
					try {
						myval = Float.parseFloat(str[0].toString().trim());
						allowed = true;
					} catch (Exception e) {
						// ignore
					}

					if (allowed) {
						if (str.length > 1)
							hot.put(myval, str[1].toString().trim() + " : " + str[2].toString().trim());
					}

				}

				// Collections.sort(hotnesses, new HotnessComparator());

				int iterateCount = 0;
				iterateCount = hot.size();
				
				int mycounter=0;
				for(Map.Entry<Float,String> entry : hot.entrySet()) {
					  Float keys = entry.getKey();
					  String value = entry.getValue();
					  if( mycounter < 10)
					  {
					    if(mycounter>10) break;
					    context.write(new Text(key +":"+keys.toString()), new Text(value));
					  }
					  mycounter+=1;
					}

				
				hot.clear();

			} catch (Exception ex) {
				System.out.println("falied for " + key);
				System.out.println(ex.getMessage());
			}

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q5");
		job.setNumReduceTasks(10);
		job.setJarByClass(Q5.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(Tokeniizermapper.class);
		job.setReducerClass(reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
