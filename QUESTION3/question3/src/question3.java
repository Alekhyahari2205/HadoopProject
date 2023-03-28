import java.io.*;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


// InMemory join at Reducer

public class question3 {
    public static class Mutualfriends_DOB_Mapper
            extends Mapper<LongWritable, Text, Text, Text> {
        Map<String, String> map = new HashMap<>();
        private final Text user_ID = new Text();
        private final Text friends_List = new Text();

       
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration config = context.getConfiguration();
            Path f_Path = new Path(config.get("map.input"));
            FileSystem fs = FileSystem.get(config);
            FileStatus[] status = fs.listStatus(f_Path);
            for(FileStatus s : status)
            {
                BufferedReader buffread = new BufferedReader(new InputStreamReader(fs.open(s.getPath())));

                String line;
                line = buffread.readLine();
//                parsing the dates to map
                while (line != null)
                {
                    String[] arr = line.split(",");
                    if (arr.length == 10)
                        map.put(arr[0], arr[9]);
                    line = buffread.readLine();
                }
            }
        }

        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] input = value.toString().split("\\t");
            // If no friends
            if (input.length < 2)
                return;
            String userId = input[0];
            String[] friends = input[1].split(",");
            String str = "";

            for (String frnd: friends)
                str += map.get(frnd) + ",";
            str = str.substring(0, str.length()-1);
            for (String frnd: friends) {
                if (userId.equals(frnd))
                    continue;
                
                String userKey = (userId.compareTo(frnd) < 0) ?
                        userId + "," + frnd : frnd + "," + userId;
                user_ID.set(userKey);
                friends_List.set(str);
                context.write(user_ID, friends_List);
            }
        }
    }

    public static class Mutualfriends_DOB_Reducer
            extends Reducer<Text, Text, Text, Text> {

        public long calculate_Age(String dob) {
//       
            long days_Difference = 0;
            try {
                // Convert `String` datatype  to `Date` format
                SimpleDateFormat simple_df = new SimpleDateFormat("MM/dd/yyyy", Locale.ENGLISH);
                Date date_Before = simple_df.parse(dob);
                Date date_After = simple_df.parse("01/01/2023");

                // Calculate the number of days between dates
                long time_Difference = Math.abs(date_After.getTime() - date_Before.getTime());
                days_Difference = TimeUnit.DAYS.convert(time_Difference, TimeUnit.MILLISECONDS);
//                System.out.println("The number of days between dates: " + daysDiff);
            }catch(Exception e){
                e.printStackTrace();
            }
            return days_Difference;
        }

        private String mutual_Friends(String list_1, String list_2) {
            if (list_1 == null || list_2 == null)
                return null;
            Set<String> set1 = new HashSet<>(Arrays.asList(list_1.split(",")));
            Set<String> set2 = new HashSet<>(Arrays.asList(list_2.split(",")));
            set1.retainAll(set2);
            return set1.toString();
        }
        // in the reduce class we calculate the length and print the dob.
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            List<String> list = new ArrayList<>();
            for (Text val: values)
                list.add(val.toString());
            if (list.size() == 1)
                list.add("");
            String m_Friends = mutual_Friends(list.get(0), list.get(1));
            String[] m_FriendsList = m_Friends.split(",");
            int len= m_FriendsList.length;
            String mutual_Friends = "";
            long min  = Long.MAX_VALUE;
            for (String friend: m_FriendsList) {
                friend = friend.replace("[","");
                friend = friend.replace("]","");
                friend = friend.replace(" ","");
//               
                long age_in_days;
                if(!friend.equals("")) {
                    age_in_days = calculate_Age(friend);
                    if(age_in_days < min) {
                        min = age_in_days;
                    }
                }
                mutual_Friends += friend + ", ";
            }
            mutual_Friends = mutual_Friends.substring(0, mutual_Friends.length()-2);

            if(mutual_Friends.length()<3)
                context.write(key, new Text("["+mutual_Friends+"],"+0));
            else
                context.write(key, new Text("[" + mutual_Friends+"],"+ min));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("map.input", args[1]);
        @SuppressWarnings("deprecation")
        Job job = new Job(conf, "Mutual Friends DOB");
        job.setJarByClass(question3.class);
        job.setMapperClass(Mutualfriends_DOB_Mapper.class);
        job.setReducerClass(Mutualfriends_DOB_Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
