//Objectives
//Types Killers killing more than 13 individual with name and area of Operation and reason

package KillingMoreThan13;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KillingMoreThan13 {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");
				long victims=Long.parseLong(str[6]);
				if(victims>13)
				{

				context.write(new Text(str[0]+str[1]), new Text(str[6] + ","+str[10] +"," +str[23]));
				}

			}

			catch (Exception e) {
				System.out.println(e.getMessage());
			}
		}
	}


	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(KillingMoreThan13.class);
		job.setJobName(" No. and Type of Killers ");
		job.setMapperClass(MapClass.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);

	}
}

