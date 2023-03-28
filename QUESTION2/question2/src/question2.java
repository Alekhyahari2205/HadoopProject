import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.LocalDate;
import java.time.Period;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class question2 {

    static Map<String, Integer> map = new HashMap<>();

    public static class Avg_Age_Mapper
            extends Mapper<LongWritable, Text, Text, Text> {
        private final Text user_id = new Text();
        private final Text friends_List = new Text();

        public int calculate_Age(String date_of_Birth) {
            String[] input = date_of_Birth.split("/");
            LocalDate end_date = LocalDate.of(2023,01, 01);
            LocalDate birth_day = LocalDate.of(
                    Integer.parseInt(input[2]),
                    Integer.parseInt(input[0]),
                    Integer.parseInt(input[1]));

            Period p = Period.between(birth_day, end_date);
            return p.getYears();
        }

    

        @Override
        public void setup(Context context)
                throws IOException, InterruptedException {
            super.setup(context);
            Configuration conf = context.getConfiguration();
            Path f_Path = new Path(conf.get("reduce.input"));
            FileSystem file_system = FileSystem.get(conf);
            FileStatus[] status = file_system.listStatus(f_Path);
            for(FileStatus s : status) {
                BufferedReader buffread = new BufferedReader(
                        new InputStreamReader(file_system.open(s.getPath())));
                String line;
                line = buffread.readLine();
                while (line != null) {
                    String[] arr = line.split(",");
                    if (arr.length == 10)
                        map.put(arr[0], calculate_Age(arr[9]));
                    line = buffread.readLine();
                }
            }
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] input = value.toString().split("\\t");
            if (input.length < 2)
                return;
            user_id.set(input[0]);
            friends_List.set(input[1]);
            context.write(user_id, friends_List);
        }
    }

    public static class Avg_Age_Reducer
            extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            for (Text value: values) {
                String[] friends = value.toString().split(",");
                int avg_Age = 0;
                for (String friend: friends) {
                    if (map.get(friend) != null)
                        avg_Age += map.get(friend);
                }
                avg_Age /= friends.length;
                context.write(key, new Text(Integer.toString(avg_Age)));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("reduce.input", args[1]);
        Job job = new Job(conf, "Direct Friends Average Age");
        job.setJarByClass(question2.class);
        job.setMapperClass(Avg_Age_Mapper.class);
        job.setReducerClass(Avg_Age_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}