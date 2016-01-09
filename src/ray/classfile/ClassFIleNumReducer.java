package ray.classfile;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import ray.nbtrainer.NBTrainReducer;

public class ClassFIleNumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable result = new IntWritable();
	private static final Logger LOG = Logger.getLogger(NBTrainReducer.class);
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException{
		int sum = 0;
		for(IntWritable val: values){
			sum += val.get();
		}
		result.set(sum);
		LOG.info("key: "+ key.toString() + result);
		context.write(key, result);
	}
	
}
