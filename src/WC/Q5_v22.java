package WC;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.net.Inet4Address;
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
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.omg.PortableInterceptor.SYSTEM_EXCEPTION;

public class Q5_v22 {

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
						int index = 0;
						int largest = -1;
					
						for (int i = 0; i < mbtagFre.length; i++) {
							  if ( mbtagFre[i] > largest )
							    {
							        largest = mbtagFre[i];
							        index = i;
							    }
						}
						
						if(largest==1 && mbtagFre.length>0)
						{
							//if all values in array are same
							for (int i = 0; i < strFGenere.length; i++) {
												
								if (mbtagFre.length >= 1)
									if (mbtagFre[i] != 0) {
										System.out.println("key:" + strFGenere[i]);
										
										
										
										System.out.println(datasplit[11] + "--" + strFGenere[i] + ":" + mbtagFre[i]);
										// String strgenere=strFGenere[i] ;
										String strHotness=datasplit[42];
										strHotness = datasplit[42].trim();
										
										int sz = strHotness.length();
										

										if(! strHotness.equalsIgnoreCase("0.0") &&  (!strHotness.contains("/")  &&  (!strHotness.equalsIgnoreCase("nan"))))
										{
											
										//System.out.println(strHotness+ ":" + datasplit[11] + ":" + datasplit[50]);
										
										context.write(new Text(strFGenere[i]),
												new Text(strHotness + ":" + datasplit[11] + ":" + datasplit[50]));
										}
									}

							} //for loop end
						}
						else
						{
							
						     
						 	String strHotness=datasplit[42];
							strHotness = datasplit[42].trim();
							
					
							if(! strHotness.equalsIgnoreCase("0.0") &&  (!strHotness.contains("/")  &&  (!strHotness.equalsIgnoreCase("nan"))))
							{
								
							System.out.println(strHotness+ ":" + datasplit[11] + ":" + datasplit[50]);
							
							 context.write(new Text(strFGenere[index]),	new Text(strHotness + ":" + datasplit[11] + ":" + datasplit[50]));
							}
						}
		
					}

				}
			}

		}

	}

	public static class reducer extends Reducer<Text, Text, Text, Text> {

		@SuppressWarnings("unchecked")
		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

			ArrayList<Hotness> hotnesses = new ArrayList<>();

			for (Text val : values)

			{
				// System.out.println(val);

				String[] str = val.toString().split(":");
				// System.out.println(key+ ":" + str);

				if (str.length > 1) {
					if (!str[0].equals("nan")) {
						Float myval = Float.parseFloat(str[0].trim());
						if (!myval.isNaN()) // handle NAN for file 16.tsv
							hotnesses.add(new Hotness(myval, str[1], str[2]));
					}
				}
			}

			Collections.sort(hotnesses, new Hotness());

			for (int i = 0; i < hotnesses.size(); i++) {

				context.write(new Text(key), new Text(
						hotnesses.get(i).hotNum + ":" + hotnesses.get(i).artistname + ":" + hotnesses.get(i).songName));
			}
			
			hotnesses.clear();
		}

		public static void main(String[] args) throws Exception {

			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Word Count");
			job.setJarByClass(Q5_v22.class);

			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setMapperClass(Tokeniizermapper.class);
			// job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(reducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(IntWritable.class);

			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		}
	}
}
