package WC;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Q1 {

	public static class Q1Mappaer extends Mapper<LongWritable, Text, Text, Text> {

		Pattern guidString = Pattern.compile("^(\\{){0,1}[0-9a-fA-F]{8}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{4}\\-[0-9a-fA-F]{12}(\\}){0,1}$");

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");
			if (key.get() != 0) {
				System.out.println("artist");
				System.out.println(datasplit[11]);
				String strGenere = datasplit[9];
				System.out.println(strGenere);
				boolean test = strGenere.equalsIgnoreCase("[]");
				Matcher matcher = guidString.matcher(strGenere);
				System.out.println(strGenere);

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
											System.out.println("key:" + strFGenere[i]);

											context.write(new Text(datasplit[11]),
													new Text(strFGenere[i] + ":" + tagRatingunshorted[i]));
										}
								}

							} // for loop end
						} else {

							context.write(new Text(datasplit[11]),
									new Text(strFGenere[index] + ":" + tagRatingunshorted[index]));
						}
					}

				}
			}

		}

	}

	public static class Q1Reducer extends Reducer<Text, Text, Text, Text> {

		@SuppressWarnings("unchecked")
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			ArrayList<ArtistTag> artistTagCol = new ArrayList<ArtistTag>();
			try {
				for (Text val : values)

				{
					// System.out.println(val);

					String[] str = val.toString().split(":");

					if (str.length > 1) {
						artistTagCol.add(new ArtistTag(str[0], Integer.parseInt(str[1])));
					}
				}

				Collections.sort(artistTagCol, new ArtistTag());

				int iterateCount = artistTagCol.size();

				context.write(new Text(key), new Text(artistTagCol.get(0).genere));

			} catch (Exception ex) {
				System.err.println("falied for " + key);
				System.err.println(ex.getMessage());
			}

			artistTagCol.clear();
		}


	}
	
	public static void main(String[] args) throws Exception {

		for(String str:args)
		{
			System.out.println(str);
		}
		
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Q1");
		job.setJarByClass(Q1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setMapperClass(Q1Mappaer.class);
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(Q1Reducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
