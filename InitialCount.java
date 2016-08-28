package adelaide.edu.edc.a1e3.initialcount;

import java.io.IOException;
import java.util.Arrays;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InitialCount extends Configured implements Tool {

	   public static void main(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      int response = ToolRunner.run(new Configuration(), new InitialCount(), args);
	      
	      System.exit(response);
	   }

	   @Override
	   public int run(String[] args) throws Exception {
	      System.out.println(Arrays.toString(args));
	      Job job = new Job(getConf(), "InitialCount");
	      job.setJarByClass(InitialCount.class);
	      job.setOutputKeyClass(Text.class);
	      job.setOutputValueClass(IntWritable.class);

	      job.setMapperClass(Map.class);
	      job.setReducerClass(Reduce.class);

	      job.setInputFormatClass(TextInputFormat.class);
	      job.setOutputFormatClass(TextOutputFormat.class);

	      FileInputFormat.addInputPath(job, new Path(args[0]));
	      FileOutputFormat.setOutputPath(job, new Path(args[1]));

	      job.waitForCompletion(true);
	      
	      return 0;
	   }
	   
	   public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
	      private final static IntWritable one = new IntWritable(1);
	      private Text word = new Text();

	      @Override
	      public void map(LongWritable key, Text value, Context context)
	              throws IOException, InterruptedException {
	         StringTokenizer readstr = new StringTokenizer(value.toString());
	         while (readstr.hasMoreTokens()){
	        	 word.set(readstr.nextToken()); 
	        	 //change word to String type
	        	 String letter = word.toString();
	        	 //find out the initial letter
	        	 String intletter = letter.substring(0,1);
	        	 //Case 1: the initial letter is A to Z, set its value to word
	        	 if ( intletter.charAt(0) >= 'A' && intletter.charAt(0) <= 'Z'){
	        		 word.set(intletter);
	        		 context.write(word, one);
	        	   }
	        	 //Case 2: the initial letter is a to z, change it to upper case and then set its value to word
	        	 else if (intletter.charAt(0) >= 'a' && intletter.charAt(0) <= 'z'){
	        		 word.set(intletter.toUpperCase());
	        		 context.write(word, one);
	        	 }       	
	         }
	      }
	   }

	   public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
	      @Override
	      public void reduce(Text key, Iterable<IntWritable> values, Context context)
	              throws IOException, InterruptedException {
	         int sum = 0;
	         for (IntWritable val : values) {
	            sum += val.get();
	         }
	         context.write(key, new IntWritable(sum));
	      }
	   }
}
