package WC;

import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.examples.WordCount.IntSumReducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import WC.Q2.Q2Mapper.Q2Reducer;

public class Q2 {

	public static class Q2Mapper extends Mapper<LongWritable, Text, Text, FloatWritable> {


		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			// StringTokenizer itr = new StringTokenizer(value.toString());

			String[] datasplit = value.toString().split("\t");
			try {
				if (key.get() != 0) {
					// System.out.println("songid");

					// System.out.println(datasplit[43]);
					// System.out.println(datasplit[47]);

					String songID = "";
					float fltTempo = 0;

					songID = datasplit[43];

					String strTempo = "";
					strTempo = datasplit[47];
					System.out.println(strTempo);

					boolean arrayFound = false;
					if (strTempo.contains("[")) {
						arrayFound = true;
					}
					String[] listtempo = null;
					ArrayList<Float> floatTempo = new ArrayList<Float>();

					float avgtempo = 0;
					float avgSumtempo = 0;

					if (arrayFound) {
					//	System.out.println(strTempo);
						strTempo = strTempo.trim().replaceAll("\\[", "").replaceAll("\\]", "");
						listtempo = strTempo.split(",");

						if (listtempo.length > 0) {
							for (int i = 0; i < listtempo.length; i++) {
								floatTempo.add(Float.parseFloat(listtempo[i]));

							}

							if (floatTempo.size() > 0) {
								for (Float float1 : floatTempo) {
									avgSumtempo += float1;
								}

								avgtempo = avgSumtempo / (floatTempo.size() - 1);
							}
						}
						FloatWritable tempo = new FloatWritable();
						tempo.set(avgtempo);
					//	System.out.println(tempo);
					//	context.write(new Text("songTempo"), tempo);

					} else {
						FloatWritable tempo = new FloatWritable();
						fltTempo = Float.parseFloat(datasplit[47]);
						tempo.set(fltTempo);
						//context.write(new Text("songTempo"), tempo);
						System.out.println(tempo);

					}

				}
			} catch (Exception ex) {
				System.out.println(ex.toString());
			}

		}

		public static class Q2Reducer extends Reducer<Text, FloatWritable, Text, FloatWritable> {
			public FloatWritable result = new FloatWritable();

			@Override
			public void reduce(Text key, Iterable<FloatWritable> values, Context context)
					throws IOException, InterruptedException {
				float sum = 0;
				int counter_for_AverageCalc = 0;
				float total_Tempo = 0;
				try {
					for (FloatWritable val : values) {
						sum += val.get();
						counter_for_AverageCalc += 1;
						total_Tempo += val.get();
					}

					float average_tempo = total_Tempo / counter_for_AverageCalc;

					result.set(average_tempo);

				//	context.write(new Text("Avg Tempo:"), result);

				}

				
				
				catch (Exception ex) {
					System.out.println(ex.toString());
				}

			}

		}

	}

	public static void main(String[] args) throws Exception {
		for (String string : args) {
			System.out.println(string);
		}

		try {

			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "Q2Job");
			job.setJarByClass(Q2.class);
			job.setMapperClass(Q2Mapper.class);
			job.setReducerClass(Q2Reducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(FloatWritable.class);
			FileInputFormat.addInputPath(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job, new Path(args[1]));
			System.exit(job.waitForCompletion(true) ? 0 : 1);

		} catch (Exception ex) {
			System.out.println(ex.toString());
		}
		
		

	}
}