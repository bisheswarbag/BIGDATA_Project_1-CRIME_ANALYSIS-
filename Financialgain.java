//Objectives
//No. of serial Killers who Killed people for financial gain or for money

package Psyco;

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

public class Financialgain {
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");

				if (str[23].equals("FinancialGain")||str[23].equals("FinancialGain-Robbery")||str[23].equals("FinancialGain-Robbery-Retail")||str[23].equals("FinancialGain-Robbery-HomeInvasion")||str[23].equals("FinancialGain-LethalCareTaker")||str[23].equals("FinancialGain-BlackWidow")){

					context.write(new Text(str[23]), new Text(str[23] + ","
							+ str[0]));

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

			int rapist = 0;

			for (@SuppressWarnings("unused") Text t : values) {

				rapist++;
				
			}
			String rapists = " the no of serial killers who killed for FinancialGain are  "
					+ rapist;

			context.write(key, new Text(rapists));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Financialgain.class);
		job.setJobName(" Killers who Killed people for financial gain or for money");
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
