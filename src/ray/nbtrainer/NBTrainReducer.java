package ray.nbtrainer;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import ray.TextPair;



public class NBTrainReducer extends Reducer<TextPair, IntWritable, TextPair, IntWritable>{
	private IntWritable result = new IntWritable();
	private static final Logger LOG = Logger.getLogger(NBTrainReducer.class);
	public void reduce(TextPair key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
		
		int sum = 0;
		for(IntWritable val : values) {
			sum += val.get();
		}
		
		result.set(sum);
		LOG.info("key: "+ key.toString() + result);
		
	//	Text formatPair = key.formatPair();
		
		context.write(key, result);
	}

}
