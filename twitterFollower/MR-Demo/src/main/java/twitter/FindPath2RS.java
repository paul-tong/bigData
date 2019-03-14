package twitter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
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

public class FindPath2RS extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(FindPath2RS.class);

	public static class Path2Mapper extends Mapper<Object, Text, Text, Text> {
		
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		// Max value of user id being considered.
		private long MAX_ID_VALUE = MaxValue.MAX_ID_VALUE;

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			// Got the id of start and end point of this edge.
			String startIdString = value.toString().split(",")[0];
			String endIdString = value.toString().split(",")[1];
			
			// Discard this record if user id exceed max value.
			if (Long.valueOf(startIdString) > MAX_ID_VALUE 
					|| Long.valueOf(endIdString) > MAX_ID_VALUE) {
				return;
			}
			
			// Emit start id as key, end id as value with tag "e".
			outKey.set(startIdString);
			outValue.set("e" + endIdString);
			context.write(outKey, outValue);
			
			// Emit end id as key, start id as value with tag "s".
			outKey.set(endIdString);
			outValue.set("s" + startIdString);
			context.write(outKey, outValue);
		}
	}

	public static class Path2Reducer extends Reducer<Text, Text, Text, NullWritable> {
		
		private List<String> listStart = new ArrayList<>();
		private List<String> listEnd = new ArrayList<>();
		
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			listStart.clear();
			listEnd.clear();
			
			// Add values into corresponding list based on tags.
			for (Text value : values) {
				if (value.charAt(0) == 's') {
					listStart.add(value.toString().substring(1));
				}
				else {
					listEnd.add(value.toString().substring(1));
				}
			}
			
			// Combine each start point with end point, while key is the mid of path2.
			for (String start : listStart) {
				for (String end : listEnd) {
					/*
					 *  Write(start, mid, end) into text as key, value is NullWritable.
					 *  To avoid circle, start != end.
					 */
					if (!start.equals(end)) {
						context.write(new Text(start + "," + key.toString() + "," + end), NullWritable.get());
					}
				}
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		// Configuration of the job.
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Get Path2");
		job.setJarByClass(FindPath2RS.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		job.setMapperClass(Path2Mapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(Path2Reducer.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
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
			ToolRunner.run(new FindPath2RS(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}