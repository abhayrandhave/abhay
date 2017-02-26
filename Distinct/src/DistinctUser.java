import java.io.IOException;
import java.util.Map;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
//this my first program

public class DistinctUser extends Configured implements Tool {

	public static class DistinctUserMapper extends
			Mapper<Object, Text, Text, NullWritable> {
		private Text outUserId = new Text();

		public void map(Object key, Text value, Context context)
				throws IOException, InterruptedException {
			Map<String, String> parsed = MRDPUtils.transformXmlToMap(value.toString());
			// Get the value for the UserId attribute
			String userId = parsed.get("UserId");

			if (userId == null) {
				return;
			}
			// Set our output key to the user's id
			outUserId.set(userId);
			// Write the user's id with a null value
			context.write(outUserId, NullWritable.get());
		}
	}

	public static class DistinctUserReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			// Write the user's id with a null value
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new DistinctUser(), args);
		System.exit(res);
	}

	@SuppressWarnings("deprecation")
	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = new Configuration();
		GenericOptionsParser parser = new GenericOptionsParser(conf, args);
		String[] otherArgs = parser.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: DistinctUser <in> <out>");
			ToolRunner.printGenericCommandUsage(System.err);
			System.exit(2);
		}
		Job job = new Job(conf, "Distinct User");
		job.setJarByClass(DistinctUser.class);
		job.setMapperClass(DistinctUserMapper.class);
		job.setReducerClass(DistinctUserReducer.class);
		job.setCombinerClass(DistinctUserReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		boolean success = job.waitForCompletion(true);

		return success ? 0 : 1;
	}

public static class MRDPUtils {

  public static final String[] REDIS_INSTANCES =
      { "p0", "p1", "p2", "p3", "p4", "p6" };

  // This helper function parses the stackoverflow into a Map for us.
  public static Map<String, String> transformXmlToMap(String xml) {
    Map<String, String> map = new HashMap<String, String>();
    try {
      String[] tokens =
          xml.trim().substring(5, xml.trim().length() - 3).split("\"");
    //  System.out.println("xml : "+xml);
    //  System.out.println("xml.trim().length() "+ xml.trim().length()); cut first 5 to last-3
      
      for (int i = 0; i < tokens.length - 1; i += 2) {
        String key = tokens[i].trim();
        String val = tokens[i + 1];
        map.put(key.substring(0, key.length() - 1), val);
      //  System.out.println("*********Key : "+key + " **Val"+val);
      //  System.out.println("----------------------------------------------------");
      }
    } catch (StringIndexOutOfBoundsException e) {
      System.err.println(xml);
    }

    return map;
  }
}

}
