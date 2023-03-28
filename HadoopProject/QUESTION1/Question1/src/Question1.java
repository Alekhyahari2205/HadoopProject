import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
 



public class Question1{
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {

		private Text friends_Set = new Text();

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String[] parsed_line = value.toString().split("\t");
			if (parsed_line.length == 2) {
				String friend_1 = parsed_line[0];
				List<String> values = Arrays.asList(parsed_line[1].split(","));
				for (String friend_2 : values) {
					int f1 = Integer.parseInt(friend_1);
					int f2 = Integer.parseInt(friend_2);
					if (f1 > f2)
						friends_Set.set(friend_2 + "," + friend_1);
					else
						friends_Set.set(friend_1 + "," + friend_2);
					context.write(friends_Set, new Text(parsed_line[1]));
				}
			}
		}

	}


	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		private Text result = new Text();
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			HashMap<String, Integer> map = new HashMap<String, Integer>();
			StringBuilder common_friends = new StringBuilder();
			for (Text frnds : values) {
				List<String> temp = Arrays.asList(frnds.toString().split(","));
				for (String frnd : temp) {
					if (map.containsKey(frnd))
						common_friends.append(frnd + ',');
					else
						map.put(frnd, 1);

				}
			}
			
			if (common_friends.lastIndexOf(",") > -1) {
				common_friends.deleteCharAt(common_friends.lastIndexOf(","));
			}
			result.set(new Text(common_friends.toString()));
			context.write(key, result);
		
		
	}
	}

	// Driver program
	public static void main(String[] args) throws Exception {

		
		
		Configuration config = new Configuration();
		String[] otherArgs = new GenericOptionsParser(config, args).getRemainingArgs();
		// get all args1
		if (otherArgs.length != 2) {
			System.err.println("Usage: mutualfriends <inputfile hdfs path> <output file hdfs path>,");
			System.exit(2);
		}

		@SuppressWarnings("deprecation")
		Job job = new Job(config, "Question1");
		job.setJarByClass(Question1.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);

		job.setOutputValueClass(Text.class);
		// set the HDFS path of the input data
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		// set the HDFS path for the output
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		// Wait till job completion
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
	
	
	
	
