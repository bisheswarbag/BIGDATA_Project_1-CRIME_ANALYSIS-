//Objectives
//No. of Killing with Motive Of Anger(shortTempered)s


package racial;
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

public class Racial { 
	public static class MapClass extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) {

			try {
				String[] str = value.toString().split(",");
				 if(str[23].contains("Anger"))
				 {
					context.write(new Text(str[23]), new Text(str[23] + ","
							));
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

			int Spree = 0;

			for (Text t : values) {

				@SuppressWarnings("unused")
				String parts[] = t.toString().split(",");
           
            {
				Spree++;
			}
			}

			String countingserial14 = "  no of Serial killers " + " Type  :-  " + Spree
					+ "\n";

			context.write(key, new Text(countingserial14));

		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		job.setJarByClass(Racial.class);
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
