import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.util.ArrayList;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join2Tables
{

    public static class PageViewMapper extends Mapper<LongWritable, Text, Text, Text>
    {   
        private Text userID = new Text();
        private Text table_PageID = new Text();
       
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {   
            String[] token = value.toString().split("\t");
            if (Character.isLetter(token[0].charAt(0)))
            {
                for (int i = 1; i < token.length; i++)
                {
                       token[i] += token[i];;
                   }
               }
               userID.set(token[1]);
               table_PageID.set("1" + "\t" + token[0]);
               context.write(userID, table_PageID);
           }
    }
   
    public static class UserMapper extends Mapper<LongWritable, Text, Text, Text>
    {   
        private Text userID = new Text();
        private Text table_Age = new Text();
       
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
        {   
            String[] token = value.toString().split("\t");
               if (Character.isLetter(token[0].charAt(0)))
               {
                   for (int i = 1; i < token.length; i++)
                   {
                       token[i] += token[i];
                   }
               }
               userID.set(token[0]);
               table_Age.set("2" + "\t" + token[1]);
               context.write(userID, table_Age);
           }
    }
   
    public static class JoinReducer extends Reducer<Text,Text,Text,Text>
    {   
        private Text Key = new Text();
        private Text Value = new Text();   
   
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException
        {   
            ArrayList<Text> PageID_list = new ArrayList<Text>();
            ArrayList<Text> Age_list = new ArrayList<Text>();
            for (Text val : values)
            {
                   String[] recordCheck = val.toString().split("\t");
                  if (recordCheck[0].equals("1"))
                {
                       PageID_list.add(new Text(recordCheck[1]));   
                   }
               
                else if (recordCheck[0].equals("2"))
                {
                    Age_list.add(new Text(recordCheck[1]));
                }                   
            }

            for (Text PageID : PageID_list)
            {
                   for (Text Age : Age_list)
                   {
                       Key.set(PageID.toString());
                       Value.set(Age.toString());
                    context.write(Key, Value);
                 }
             }
         }
    }
   
    public static void main(String[] args) throws Exception
   
    {   
         Configuration conf = new Configuration();
         Job job = Job.getInstance(conf, "join 2 tables");
         job.setJarByClass(Join2Tables.class);
         MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,PageViewMapper.class);
         MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,UserMapper.class);
         job.setReducerClass(JoinReducer.class);
         job.setOutputKeyClass(Text.class);
         job.setOutputValueClass(Text.class);
         FileOutputFormat.setOutputPath(job, new Path(args[2]));
         System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}