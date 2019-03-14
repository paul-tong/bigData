package twitter;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CountPath2 extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(CountPath2.class);

	public static class TokenizerMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			// Got the id of user that has an out-edge (follower). 
			String outIdString = value.toString().split(",")[0];
			outKey.set(outIdString);
			outValue.set("out");
			context.write(outKey, outValue);
			
			// Got the id of user that has an in-edge (being followed). 
			String inIdString = value.toString().split(",")[1];
			outKey.set(inIdString);
			outValue.set("in");
			context.write(outKey, outValue);
		}
	}

	public static class IntSumReducer extends Reducer<Text, Text, Text, LongWritable> {
		
		private long totalPath2Count = 0;
		
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			
			// Count the path2 of current user and add it to the total count.
			long inCount = 0;
			long outCount = 0;
			long path2Count = 0;
			
			for (final Text val : values) {
				if (val.toString().equals("in")) {
					inCount++;
				}
				else {
					outCount++;
				}
			}
			
			path2Count = inCount * outCount;
			totalPath2Count += path2Count;
		}
		
		@Override
		public void cleanup(Context context) throws IOException, InterruptedException {
			/* 
			 * Write the total path2 count of current reducer task.
			 * Use LongWritable in case it will exceed the range of int.
			 */
			Text outKey = new Text("path2 count:");
			LongWritable outValue = new LongWritable(totalPath2Count);
			System.out.println("path2 count: " + totalPath2Count);
			context.write(outKey, outValue);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		// Configuration of the job.
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Path2 Count");
		job.setJarByClass(CountPath2.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static void main(final String[] args) {
		System.out.println("into the program");
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new CountPath2(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}