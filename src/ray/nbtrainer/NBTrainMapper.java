package ray.nbtrainer;

import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import ray.TextPair;


public  class NBTrainMapper extends Mapper<TextPair, Text, TextPair, IntWritable>{
	private static final Logger LOG = Logger.getLogger(NBTrainMapper.class);
	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private static String wordRegex = "[a-zA-Z]+";
	//private static final Pattern PATTERN = Pattern.compile(wordRegex);
	public void map(TextPair classAndFileName, Text value, Context context)
		throws IOException, InterruptedException{
		LOG.info("map key: " + classAndFileName );
		StringTokenizer itr = new StringTokenizer(value.toString());
		while(itr.hasMoreTokens()){
			word.set(itr.nextToken());
			
			if(word.toString().matches(wordRegex)){
				Text className = classAndFileName.getFirst();
				TextPair classwordPair = new TextPair(className, word);		
				LOG.info("mapPair: " + classwordPair.toString());
				
				context.write(classwordPair, one);
			}
			
		
		}
	}
}
