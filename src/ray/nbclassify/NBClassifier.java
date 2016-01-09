package ray.nbclassify;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import ray.TextPair;




public class NBClassifier  extends Configured implements Tool{

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(NBClassifier.class);
		job.setJobName("Naive bayes classifier");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(SequenceFileInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(TextPair.class);
		job.setMapperClass(NBClassifierMapper.class);
		job.setReducerClass(NBClassifierReducer.class);
		return job.waitForCompletion(true) ? 0:1;
	}
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new NBClassifier(), args);
		System.exit(exitCode);
	}
	

}
