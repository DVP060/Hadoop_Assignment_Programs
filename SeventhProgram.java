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

public class SeventhProgram {
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String col[] = value.toString().split(",");
			String title = col[1].toString();
			String genres = col[2].toString();
			if(genres.contains("Comedy")) {
				context.write(new Text(title+" : "+genres),new IntWritable(1));
			}
			if(genres.contains("Documentary") && title.contains("1995")) {
				context.write(new Text("Documentry"),new IntWritable(1));
			}
			if(title.contains("Gold")) {
				context.write(new Text(title),new IntWritable(1));
			}
			if(genres.contains("Drama|Romance")) {
				context.write(new Text("Drama_Romance"),new IntWritable(1));
			}
			if(genres.isEmpty()) {
				context.write(new Text("Missing"), new IntWritable(1));
			}
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		private int count = 0;
		private int missing = 0;
		private int drama_romance = 0;
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException,InterruptedException{
			int sum = 0;
			for(IntWritable value : values) {
				sum += value.get();
			}
			if(!(key.toString().contains("Documentary") && key.toString().contains("Missing") && key.toString().contains("Drama_Romance"))) {
				context.write(key, new IntWritable(sum));
			}
			if(key.toString().contains("Documentary")) {
				count = sum;
			}
			if(key.toString().contains("Missing")) {
				missing = sum;
			}
			if(key.toString().contains("Drama_Romance")) {
				drama_romance = sum;
			}
		}
		
		public void cleanup(Context context) throws IOException,InterruptedException{
			context.write(new Text("Total documentry movie in 1995 : "), new IntWritable(count));
			context.write(new Text("Total missing genres : "), new IntWritable(missing));
			context.write(new Text("Total drama and romance both count : "), new IntWritable(drama_romance));
		}
	}
	
	public static void main(String args[]) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf,"SeventhProgram");
		
		job.setJarByClass(SeventhProgram.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		System.exit(job.waitForCompletion(true)?0:1);
	}
}
