
//Objectives
//No. of serial Killers who rape before killing and percentage of serial killers who were rapist


package RapedBeforeKilling;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RapedBeforeKilling {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");

				
				if(str[23].equals("Enjoyment-Rape"))
				{
				
					
					context.write(new Text(str[23]), new Text(str[23]+","+str[0]));

				}
			}

			catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}

	public static class ReduceClass extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {

		    int rapist=0;
		    long total = 0;
			

			for (Text t : values) {
				
				String []str=t.toString().split(",");
				
				if(str[0].equals("Enjoyment-Rape"))
				{
					rapist++;
				}
				}
			String rapists=" the no of rapist serial killers are  "+rapist;
			
		

		
			context.write(key, new Text(rapists));
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(RapedBeforeKilling.class);
		job.setJobName(" No. and Type of Killers ");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(2);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}


