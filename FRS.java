
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class FRS {

    static public class FriendOrNotWritable implements Writable {
    	// toUser
        public Long user;
        // indicator of friend of fromUser-toUser, set to TRUE if they are friends, otherwise FALSE 
        public Boolean isFriend;

        public FriendOrNotWritable(final Long user, final Boolean alreadyFriend) {
            this.user = user;
            this.isFriend = alreadyFriend;
        }

        public FriendOrNotWritable() {
            this(-1L, false);
        }

        @Override
        public void write(final DataOutput out) throws IOException {
            out.writeLong(user);
            out.writeBoolean(isFriend);
        }

        @Override
        public void readFields(final DataInput in) throws IOException {
            user = in.readLong();
            isFriend = in.readBoolean();
        }
    }
    

    public static class Map extends Mapper<LongWritable, Text, LongWritable, FriendOrNotWritable> {
        private Text word = new Text();

        @Override
        public void map(final LongWritable key, final Text value, final Context context) throws IOException, InterruptedException {
        	// value is each of input line
            String line[] = value.toString().split("\t");
            Long fromUser = Long.parseLong(line[0]);
            
            if(line.length != 2) {
            	return;
            }
            
            String[] str = line[1].split(",");
            for(String s : str) {
            	// get all the toUsers
            	Long toUser = Long.parseLong(s);
            	// write down all the fromUser-toUser mapping
            	context.write(new LongWritable(fromUser), new FriendOrNotWritable(toUser, true));
            }
            
            // for loop all the toUsers, due to they have the same mutual friend(fromUser), so each toUser should be the recommendation friend of the other toUser
            for (int i = 0; i < str.length-1; i++) {
                for (int j = i + 1; j < str.length; j++) {
                	Long toUserI = Long.parseLong(str[i]);
                	Long toUserJ = Long.parseLong(str[j]);
                	// write down all the toUser-toUser mapping
                    context.write(new LongWritable(toUserI), new FriendOrNotWritable((toUserJ), false));
                    context.write(new LongWritable(toUserJ), new FriendOrNotWritable((toUserI), false));
                }
            }
        }
    }

    public static class Reduce extends Reducer<LongWritable, FriendOrNotWritable, LongWritable, Text> {
        @Override
        public void reduce(final LongWritable key, final Iterable<FriendOrNotWritable> values, final Context context)
                throws IOException, InterruptedException {

        	// the map key is toUser, the value is the number of the mutual friends
            final java.util.Map<Long, Integer> friendSize = new HashMap<Long, Integer>();

            for (FriendOrNotWritable val : values) {
                final Long toUser = val.user;
                if(val.isFriend) {
                	// set the map value to null when they are friends
                    friendSize.put(toUser, null);
                } else {
                	// the map does NOT contain this toUser, set the value to null if they are friend, otherwise initialize the size as 1
                	if(!friendSize.containsKey(toUser)) {
                		friendSize.put(toUser, 1);
                	} else {
                		Integer i = friendSize.get(toUser);
                		if(i != null) {
                			friendSize.put(toUser, ++i);
                		}
                	}
                }
            }
            
            java.util.Map<Long, Integer> b= new HashMap<Long, Integer>();
    		for(java.util.Map.Entry<Long, Integer> entry : friendSize.entrySet()) {
            	if(entry.getValue() != null) {
            		b.put(entry.getKey(), entry.getValue());
            	}
            }
    		
            // the comparator for the friendSize map above
            List<java.util.Map.Entry<Long, Integer>> entryList = new ArrayList<java.util.Map.Entry<Long, Integer>>(b.entrySet()); 
    		Collections.sort(entryList,  
                    new Comparator<java.util.Map.Entry<Long, Integer>>() {  

    					@Override
    					public int compare(final Entry<Long, Integer> o1, final Entry<Long, Integer> o2) {
    						if(o1.getValue() == null) {
    							return 1;
    						} 
    						if(o2.getValue() == null) {
    							return -1;
    						}
    						Long key1 = o1.getKey();
    						Long key2 = o2.getKey();
    						Integer v1 = o1.getValue();
    	                	Integer v2 = o2.getValue();
    	                	return v1-v2==0?(key1<key2?-1:1):v2-v1;
    					}  
                    });  
    		Iterator<java.util.Map.Entry<Long, Integer>> iter = entryList.iterator();  
    		java.util.Map.Entry<Long, Integer> tmpEntry = null;  
    		StringBuilder sb = new StringBuilder();
            while (iter.hasNext()) {  
                tmpEntry = iter.next();  
                if(tmpEntry.getValue() != null) {
                	sb.append(tmpEntry.getKey().toString()).append(",");
                }
            } 
            String output = sb.toString();
            if(output.endsWith(",")) {
            	output = output.substring(0, output.length()-1);
            }
            context.write(key, new Text(output));
        }
    }

    public static void main(final String[] args) throws Exception {
        Configuration conf = new Configuration();

        Job job = new Job(conf, "FRS");
        job.setJarByClass(FRS.class);
        job.setOutputKeyClass(LongWritable.class);
        job.setOutputValueClass(FriendOrNotWritable.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileSystem outFs = new Path(args[1]).getFileSystem(conf);
        outFs.delete(new Path(args[1]), true);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
    }
}
