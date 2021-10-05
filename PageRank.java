import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import java.util.*;
import java.util.Map.Entry;


public class PageRank {

	public static class MyMapper extends Mapper<Object, Text, Text, Text> {

		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			
			String[] nodes = value.toString().split("	");
			String nodeId = nodes[0];
			double initRank = 1.0;
			double damp = 0.85;
			
			String[] outLinks = nodes[1].split(",");								
			double ratio = damp * (initRank / outLinks.length);		
			double len_outLinks = outLinks.length;
			context.write(new Text(nodeId), new Text(1.0+"_"+len_outLinks));
			for (int i = 0; i < outLinks.length; i++) {
				context.write(new Text(outLinks[i]), new Text(ratio+"_"+len_outLinks));
			}
		}
	}

	public static class MyReducer extends Reducer<Text, Text, Text, Text> {
		

	    private Map<Text, Text> countMap = new HashMap<Text, Text>();

		@Override
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			
			double pagerank = 0.15;
			double inlinks = 0.0;
			double outlinks = 0.0;
			Map<String, Double> pagerankOutlink = new HashMap<>();
			for (Text value : values) {
				if (Double.compare(Double.parseDouble(value.toString().split("_")[0]), (Double)1.0) == 0) {
					pagerankOutlink.put(key.toString(), Double.parseDouble(value.toString().split("_")[1]));
				}
				pagerank += Double.parseDouble(value.toString().split("_")[0]);
				inlinks += 1;
				
			}
//			context.write(key, new Text((pagerank-1) + "  " + pagerankOutlink.get(key.toString()) + "  " + (inlinks-1) ));
			countMap.put(new Text(key), new Text((pagerank-1) + " " + pagerankOutlink.get(key.toString()) + " " + (inlinks-1)));
		}
	
	public void cleanup(Context context) throws IOException, InterruptedException {
		Integer numberOfRecords = 0;
    	List<Entry<Text, Text>> countList = new ArrayList<Entry<Text, Text>>(countMap.entrySet());
    	Collections.sort( countList, new Comparator<Entry<Text, Text>>(){
            public int compare( Entry<Text, Text> o1, Entry<Text, Text> o2 ) {
                return (o2.getValue().toString().split(" ")[0]).compareTo( o1.getValue().toString().split(" ")[0] );
            }
        } );
    	for(Entry<Text, Text> entry: countList) { 
    		if (numberOfRecords.equals(500)) {
    			break;
    		}
	    	context.write(entry.getKey(), entry.getValue());
	    	numberOfRecords++;
	    }	
    }
	}
	

	public static void main(String[] args) throws Exception {

		Job job = Job.getInstance(new Configuration());
		job.setJarByClass(PageRank.class);
		job.setJobName("Page Rank");
		
		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setNumReduceTasks(1);

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
