package twitter;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;


public class CountTriangleRS extends Configured implements Tool {
	// Counter for the total count of triangle.
	public static enum TRIANGLE_COUNTERS {
		TRIANGLE_COUNT
	}
	
	private static final Logger logger = LogManager.getLogger(CountTriangleRS.class);

	public static class Path2Mapper extends Mapper<Object, Text, Text, Text> {
		
		private Text outKey = new Text();
		private Text outValue = new Text();

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
	
			/* 
			 * For each path2, emit (start,end) with flag "p".
			 * We don't need max filter for path2 because it's already filtered at previous step. 
			 */
			String startIdString = value.toString().split(",")[0];
			String endIdString = value.toString().split(",")[2];
			
			outKey.set(startIdString.toString() + "," + endIdString.toString());
			outValue.set("p");
			context.write(outKey, outValue);
		}
	}

	public static class EdgeMapper extends Mapper<Object, Text, Text, Text> {
		
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		// Max value of user id being considered.
		private long MAX_ID_VALUE = MaxValue.MAX_ID_VALUE;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {

			/*
			 *  For each edge, emit (end,start) with flag "e".
			 *  Note the edge direction is reversed compare to path2 
			 *  and we need to apply max-filter for edges.  
			 */
			String startIdString = value.toString().split(",")[0];
			String endIdString = value.toString().split(",")[1];
			
			// Discard this record if user id exceed max value.
			if (Long.valueOf(startIdString) > MAX_ID_VALUE 
					|| Long.valueOf(endIdString) > MAX_ID_VALUE) {
				return;
			}
			
			outKey.set(endIdString.toString() + "," + startIdString.toString());
			outValue.set("e");
			context.write(outKey, outValue);
		}
	}
	
	public static class TriangleReducer extends Reducer<Text, Text, Text, Text> {
		
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			
			// If has a close edge, emit the count of path2 which is also the count of triangle with current start and end.
			long count = 0;
			boolean hasCloseEdge = false;
			for (Text value : values) {
				if (value.charAt(0) == 'e') {
					hasCloseEdge = true;
				}
				else if (value.charAt(0) == 'p') {
					count++;
				}
			}
			
			if (hasCloseEdge) {
				// Increase the triangle counter.
				context.getCounter(TRIANGLE_COUNTERS.TRIANGLE_COUNT).increment(count);
				//context.write(key, new Text(String.valueOf(count)));
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		// Configuration of the job.
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Count Triangle");
		job.setJarByClass(CountTriangleRS.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		
		// Set multiple inputs for Mappers, note the order of args.
		MultipleInputs.addInputPath(job, new Path(args[1]),
				TextInputFormat.class, Path2Mapper.class);
		MultipleInputs.addInputPath(job, new Path(args[0]),
				TextInputFormat.class, EdgeMapper.class);
		
		job.setReducerClass(TriangleReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		if (job.waitForCompletion(true)) {
			// Get the triangle count from counter.
			Counter triangleCounter = job.getCounters().findCounter(TRIANGLE_COUNTERS.TRIANGLE_COUNT);
			System.out.println("Triangle Count: " + triangleCounter.getValue() / 3);
			
			return 0;
		}
		else {
			return 1;
		}		
	}

	public static void main(final String[] args) {
		if (args.length != 3) {
			throw new Error("Three arguments required:\n<input-dir> <output-dir>");
		}

		// Run two jobs, note the order of args.
		try {
			ToolRunner.run(new FindPath2RS(), args);
			
			try {
				ToolRunner.run(new CountTriangleRS(), args);
			} catch (final Exception e) {
				logger.error("", e);
			}
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}