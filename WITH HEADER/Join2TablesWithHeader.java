import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join2TablesWithHeader
{

	public static class PageViewMapper extends Mapper<LongWritable, Text, Text, Text> 
    {    
		private Text userID = new Text();
        private Text table_PageID = new Text();
        
        public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException 
        {    
            String[] token = value.toString().split("\t");
            for (int i = 0; i < token.length; i++)
            {
            	if (Character.isDigit(token[0].charAt(0))) 
            	{
               		token[i] = token[i];
           		}
            	else
            	{
            		token[i] = "$" + token[i];
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
            for (int i = 0; i < token.length; i++)
            {
            	if (Character.isDigit(token[0].charAt(0))) 
            	{
               		token[i] = token[i];
           		}
            	else
            	{
            		token[i] = "$" + token[i];
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
                   	if (Character.isDigit(recordCheck[1].charAt(0)))
                   	{
                   		PageID_list.add(new Text(recordCheck[1]));
                   	}
                   	else 
                   	{
                   		PageID_list.add(new Text(recordCheck[1].substring(1))); 
                   	}
               	}
              	
                else if (recordCheck[0].equals("2")) 
                {
                   	if (Character.isDigit(recordCheck[1].charAt(0)))
                   	{
                   		Age_list.add(new Text(recordCheck[1]));
                   	}
                   	else 
                   	{
                   		Age_list.add(new Text(recordCheck[1].substring(1)));
                   	}
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
     	job.setJarByClass(Join2TablesWithHeader.class);
     	MultipleInputs.addInputPath(job, new Path(args[0]),TextInputFormat.class,PageViewMapper.class);
     	MultipleInputs.addInputPath(job, new Path(args[1]),TextInputFormat.class,UserMapper.class);
     	job.setReducerClass(JoinReducer.class);
     	job.setOutputKeyClass(Text.class);
     	job.setOutputValueClass(Text.class);
     	FileOutputFormat.setOutputPath(job, new Path(args[2]));
     	System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
