package ray.classfile;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import ray.TextPair;

public class ClassFileNumMapper extends Mapper<TextPair, Text, Text, IntWritable>{

	private final static IntWritable one = new IntWritable(1);
	public void map(TextPair classAndFileName, Text value, Context context)
		throws IOException, InterruptedException{
		
		Text className = classAndFileName.getFirst();
		context.write(className, one);
		
	}
}
