import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SixthProgram {
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException {
			String cols[] = value.toString().split(",");
			String reviewID = cols[0];
			context.write(new Text(reviewID), new IntWritable(1));
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		private int total_unique_reviews = 0;
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable value : values) {
				sum += value.get();
			}
			total_unique_reviews++;
			context.write(key, new IntWritable(sum));
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException{
			context.write(new Text("Total unique reviews : "), new IntWritable(total_unique_reviews));
		}
	}
	
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"SixthProgram");
		
		job.setJarByClass(SixthProgram.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}