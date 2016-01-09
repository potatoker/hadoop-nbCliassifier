package ray.seqfgen;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

import ray.TextPair;

public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		Job job = new Job();
		job.setJarByClass(SmallFilesToSequenceFileConverter.class);
		job.setJobName("smallFile to squenceFile converter");
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.setInputFormatClass(WholeFileInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(SequenceFileMapper.class);
		return job.waitForCompletion(true) ? 0:1;
	}
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(), args);
		System.exit(exitCode);
	}
	
	static class SequenceFileMapper extends Mapper<NullWritable, Text, TextPair, Text>{
		
		private static final Logger LOG = Logger.getLogger(SequenceFileMapper.class);
		//private Text filenameKey
		private TextPair classAndFileName;
		
		protected void setup(Context context) throws IOException, InterruptedException{
			InputSplit split = context.getInputSplit();
			Path path = ((FileSplit) split).getPath();
	//		filenameKey = new Text(path.toString());
			File file = new File(path.toString());
			String fileName = file.getName();
			String className = file.getParentFile().getName();
			
			classAndFileName = new TextPair(className, fileName);
			
	//		filenameKey = new Text(className);
		//	filenameKey = new Text(path.getParent().getParent().toString());
			LOG.info("class name is : " + classAndFileName);
		}
		
		protected void map (NullWritable key, Text value, Context context) 
				throws IOException, InterruptedException{
			context.write(classAndFileName, value);
		}
		
	}

}
