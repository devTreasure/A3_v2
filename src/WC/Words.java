package WC;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
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

public class Words {

	public static class Tokeniizermapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static IntWritable one = new IntWritable(1);
		private Text atrtistID = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			//StringTokenizer itr = new StringTokenizer(value.toString());
			
		    String[] datasplit = value.toString().split("\t");
		    
			if(key.get()!=0)
			{
				System.out.println(datasplit[1]);
				System.out.println(datasplit[13]);
				System.out.println(datasplit[14]);
				
				String newStr=datasplit[13].substring(1, datasplit[13].length()-1);
				
				newStr=newStr.replace("\"", "");
				
				System.out.println(newStr);
				
				String[] strGenere=newStr.split(",");
				
				
				for (String string : strGenere) {
					System.out.println(string);
					
					//context.write(atrtistID.set(datasplit[1].toString()+string), one);
					context.write(new Text(datasplit[1].toString()+string) , one);
					context.write(new Text(string) , one);
				}
				
				
			}
			//close else block later 
			else				
			{
				System.out.println(datasplit[1]);
			}
			
			}
		    /*
			System.out.println(key.get());
			System.out.println(value.toString());
			if(value.toString().contains("header"))
			{System.out.println("Printing header");}
			if(key.get()!=0)
			{
			while (itr.hasMoreTokens()) {
			//	word.set(itr.nextToken());
				System.out.println(itr.nextToken());
				//context.write(word, one);
			}
			}
		}
		    */
	}

	public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<IntWritable> values, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			result.set(sum);
			context.write(key, result);
		}
	}

	public static void main(String[] args)  throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Word Count");
		job.setJarByClass(Words.class);
		job.setMapperClass(Tokeniizermapper.class);
	//	job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
