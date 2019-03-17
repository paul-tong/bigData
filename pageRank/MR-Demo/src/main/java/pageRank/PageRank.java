package pageRank;


import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class PageRank extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(PageRank.class);

	// Counter for the dangling page score
	public static enum PAGE_RANK_COUNTERS {
		DANGLING_SCORE_COUNT,
		TOTAL_PAGE_COUNT
	}
	
	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		private double danglingScore = 0;
		
		@Override
		public void setup(Context context) throws IOException,
        InterruptedException {
			Configuration conf = context.getConfiguration();
			danglingScore = Double.valueOf(conf.get("danglingScore"));
			System.out.println("danglingScore in map: " + danglingScore);
		}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			// Got page id, score and out links from given value string
			System.out.println("value: " + value.toString());
			String[] valueArray = value.toString().split(" ");
			String page = valueArray[0];
			
			Configuration conf = context.getConfiguration(); // add dangling score from last iteration, sum of total page ranks should == |V|
			double score = Double.valueOf(valueArray[1]) + Double.valueOf(conf.get("danglingScore"));
			
			// get out links of given page
			StringBuilder outLinkSb = new StringBuilder();
			List<String> outLinks = new ArrayList<>();
			for (int i = 2; i < valueArray.length; i++) {
				outLinks.add(valueArray[i]);
				outLinkSb.append(valueArray[i] + " ");
			}
			
			// remove last space
			if (outLinkSb.length() > 0) {
				outLinkSb.deleteCharAt(outLinkSb.length() - 1);
			}
						
			// emit current page and its outLinks
			outKey.set(page);
			outValue.set("p," + outLinkSb.toString()); // flag p means passing the graph structure 
			context.write(outKey, outValue);
			
			// emit its score as dangling score if its a dangling page
			if(outLinks.isEmpty()) {
				outValue.set("d," + String.valueOf(score)); // flag d means passing a dangling page score
				context.write(outKey, outValue);
			}
			
			// emit the page score passed to each out link
			if (outLinks.size() > 0) {
				double outScore = score / outLinks.size();
				
				for (String outLink : outLinks) {
					outKey.set(outLink);
					outValue.set("o," + outScore); // flag o means passing an out link
					context.write(outKey, outValue);
				}
			}
		}
	}

	public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		private Text outKey = new Text();
		private Text outValue = new Text();
		
		@Override
		public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
			double score = 0;
			String outLinks = "";
			
			for (Text value : values) {				
				String[] valueArray = value.toString().split(",");
				
				if (valueArray[0].equals("p")) { // find outlinks structure
					outLinks = (valueArray.length > 1) ? valueArray[1] : "";
				}
				else if (valueArray[0].equals("d")) { // find a dangling page
					double danglingScore = Double.valueOf(valueArray[1]);
					int scoreInt = (int) (danglingScore * 100000); // convert double to int, so that can be applied to counter
					context.getCounter(PAGE_RANK_COUNTERS.DANGLING_SCORE_COUNT).increment(scoreInt);
					System.out.println("find a dangling page: " + key.toString());
					System.out.println("scoreDouble1: " + danglingScore);
				}
				else { // find part of page rank score
					score += Double.valueOf(valueArray[1]);
				}
			}
	
			// increase the total page count counter
			context.getCounter(PAGE_RANK_COUNTERS.TOTAL_PAGE_COUNT).increment(1);
			
			// compute overall page rank score
			score = 0.15 + 0.85 * score;
			
			outKey.set(key);
			outValue.set(score + " " + outLinks);
			context.write(outKey, outValue);
		}
	}

	@Override
	public int run(final String[] args) throws Exception {
		// need to modify path for each iteration
		String path = args[0];
		String pathIn = path;
		String pathOut = path;
		int k = Integer.parseInt(args[1]);
		int iterationCount = Integer.parseInt(args[2]);
		
		// write initial graph to file, for each line: page score link1 link2 link3...
		StringBuilder sb = new StringBuilder();
		long pageCount = k * k;
		for (int i = 1; i <= pageCount; i++) {
			sb.append(i + " " + 1.0);
			if (i % k != 0) {
				sb.append(" " + String.valueOf(i + 1));
			}
			
			if (i != pageCount) {
				sb.append("\n");
			}
		}
		
		// write initial graph to file
		try (PrintWriter out = new PrintWriter(path + "/initial.txt")) {
		    out.println(sb.toString());
		}
		
		// dangling score distributed to each page (avoid dangling page score loss)
		double danglingScore = 0;
		
		for (int i = 1; i <= iterationCount; i++) {
			// Configuration of the job.
			final Configuration conf = getConf();
			
			// send dangliing score into context so that it can be used by mapper
			conf.set("danglingScore", danglingScore + ""); // can set after mapper class was setted
			
			final Job job = Job.getInstance(conf, "Path Rank" + i);
			job.setJarByClass(PageRank.class);
			

	        
			final Configuration jobConf = job.getConfiguration();
			jobConf.set("mapreduce.output.textoutputformat.separator", " "); // add space between key and value
			job.setMapperClass(PageRankMapper.class);
			//job.setCombinerClass(IntSumReducer.class);
			job.setReducerClass(PageRankReducer.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setOutputValueClass(Text.class);

			pathOut = path + "/" + i; // modify out path for each iteration
			System.out.println("This is iteration: " + i);
			System.out.println("In path: " + pathIn);
			System.out.println("Out path: " + pathOut);
			
			// set exactly how many lines should go to an mapper, thus can set the number of mappers 
			// if we know the line number of data
			job.setInputFormatClass(NLineInputFormat.class);
	        NLineInputFormat.setInputPaths(job, new Path(pathIn));
	        job.getConfiguration().setInt("mapreduce.input.lineinputformat.linespermap", (int)(pageCount / 20)); // 20 tasks
	        
			// FileInputFormat.addInputPath(job, new Path(pathIn));
			FileOutputFormat.setOutputPath(job, new Path(pathOut));
			
			pathIn = pathOut; // change input path to last output path
			
			if (!job.waitForCompletion(true)) {
				return 1;
			}
			
			// total score of dangling pages
			Counter danglingScoreCounter = job.getCounters().findCounter(PAGE_RANK_COUNTERS.DANGLING_SCORE_COUNT);
			double totalDanglingScore = danglingScoreCounter.getValue() * 1.0 / 100000;
			System.out.println("total danglingScore of iteration " + i + " : " + totalDanglingScore);
			
			// total number of pages
			Counter totalPageCounter = job.getCounters().findCounter(PAGE_RANK_COUNTERS.TOTAL_PAGE_COUNT);
			long totalPageCount = totalPageCounter.getValue();
			System.out.println("total page count: " + totalPageCount);
			
			// compute dangling score distributed to each page and add it into next iteration
			danglingScore = totalDanglingScore * 0.85 / totalPageCount; // note: need to *0.85
			System.out.println("dangling score for each page: " + danglingScore);
		}
		
		return 0;
	}

	public static void main(final String[] args) {
		System.out.println("into the program");
		if (args.length != 3) {
			throw new Error("Arguments number wrong!");
		}

		try {
			ToolRunner.run(new PageRank(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}
}