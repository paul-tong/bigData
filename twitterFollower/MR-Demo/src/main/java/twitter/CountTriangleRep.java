package twitter;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.Map;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CountTriangleRep extends Configured implements Tool {
	// Counter for the total count of triangle.
	public static enum TRIANGLE_COUNTERS {
		PATH2_COUNT,
		TRIANGLE_COUNT
	}

	private static final Logger logger = LogManager.getLogger(CountTriangleRep.class);

	public static class TriangleMapper extends Mapper<Object, Text, Text, NullWritable> {
		
		// Max value of user id being considered.
		private long MAX_ID_VALUE = MaxValue.MAX_ID_VALUE;
		
		/*
		 *  Key is the start of an edge, value is a set because 
		 *  one start point might have multiple end points,
		 */
		private Map<String, HashSet<String>> edgeMap = new HashMap<>();
		
		@SuppressWarnings("resource")
		@Override
		public void setup(Context context) throws IOException,
				InterruptedException {
			try {
				
				File edgeFile = new File("edges.csv");
			    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(edgeFile)));

				String line;
				
				// Add edge(start -> end) into map.
				while ((line = reader.readLine()) != null) {
					String start = line.split(",")[0];
					String end = line.split(",")[1];
					
					// Discard this record if user id exceed max value.
					if (Long.valueOf(start) > MAX_ID_VALUE 
							|| Long.valueOf(end) > MAX_ID_VALUE) {
						continue;
					}
					
					if (!edgeMap.containsKey(start)) {
						edgeMap.put(start, new HashSet<>());
					}
					edgeMap.get(start).add(end);
				}
				
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		
		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
	
			String edgeStart = value.toString().split(",")[0];
			String edgeEnd = value.toString().split(",")[1];
			
			// Discard this record if user id exceed max value.
			if (Long.valueOf(edgeStart) > MAX_ID_VALUE 
					|| Long.valueOf(edgeEnd) > MAX_ID_VALUE) {
				return;
			}
			
			// We find a potential path2 if end of an edge is in the map
			if (edgeMap.containsKey(edgeEnd)) {
				String pathStart = edgeStart;
				String pathMid = edgeEnd;
				
				// For each possible path2, verify whether it's a real path2(not a circle).
				for (String pathEnd : edgeMap.get(pathMid)) {
					// End == start, find a circle.
					if (pathEnd.equals(pathStart)) {
						continue;
					}
					
					context.getCounter(TRIANGLE_COUNTERS.PATH2_COUNT).increment(1);
					
					// Find a real path2, verify whether it's a triangle.
					if (edgeMap.containsKey(pathEnd) && edgeMap.get(pathEnd).contains(pathStart)) {
						// Increase the triangle counter.
						context.getCounter(TRIANGLE_COUNTERS.TRIANGLE_COUNT).increment(1);
						// context.write(new Text(pathStart + "," + pathMid + "," + pathEnd), NullWritable.get());
					}
				}
			}
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		// Configuration of the job.
		final Configuration conf = getConf();
		final Job job = Job.getInstance(conf, "Count Triangle Rep");
		job.setJarByClass(CountTriangleRep.class);
		final Configuration jobConf = job.getConfiguration();
		jobConf.set("mapreduce.output.textoutputformat.separator", "\t");
		
		job.setMapperClass(TriangleMapper.class);
		job.setNumReduceTasks(0); // Without reducer.
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		/*
		 *  Configure the DistributedCache, the data needs to be cached is the same as input.
		 *  path has to be a file instead of a folder, we can get the file in mapper with 
		 *  the name after #.
		 */
		job.addCacheFile(new URI(args[0] + "#edges.csv"));
		
		if (job.waitForCompletion(true)) {
			// Get the triangle count from counter.
			Counter triangleCounter = job.getCounters().findCounter(TRIANGLE_COUNTERS.TRIANGLE_COUNT);
			System.out.println("Triangle Count: " + triangleCounter.getValue() / 3);
			
			//Counter path2Counter = job.getCounters().findCounter(TRIANGLE_COUNTERS.PATH2_COUNT);
			//System.out.println("path2 Count: " + path2Counter.getValue());
			
			return 0;
		}
		else {
			return 1;
		}		
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		// Run two jobs, note the order of args.
		try {
			ToolRunner.run(new CountTriangleRep(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}