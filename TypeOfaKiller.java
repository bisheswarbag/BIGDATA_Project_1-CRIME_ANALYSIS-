//Objectives
//No. of serial team Killers In the US Country

package TypeOfaKiller;

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

public class TypeOfaKiller {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");

				
				if(str[8].equals("US"))
				{
					
					context.write(new Text(str[8]), new Text(str[2]+","));

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

		
			float total = 0;
			int serialteam = 0;

			for (Text t : values) {
				total++;
				String parts[] = t.toString().split(",");
				if (( parts[0].contains("Serial-Team"))) {
					serialteam++;
				}
			}
		

			String countingforengrs = "\n"
					+ "Individual who are from "  +key
				    +" are serial killers  "+" :-  "
					+ serialteam + "\n"
					+ "Percentage of People Who just are serial killers  :-  "
					+ (serialteam * 100) / total + "%";

		
			context.write(key, new Text(countingforengrs));
			
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(TypeOfaKiller.class);
		job.setJobName(" No. and Type of Killers ");
		job.setMapperClass(MapClass.class);
		job.setReducerClass(ReduceClass.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}
