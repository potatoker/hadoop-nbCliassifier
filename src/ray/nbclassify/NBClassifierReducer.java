package ray.nbclassify;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import ray.TextPair;

public class NBClassifierReducer extends Reducer<Text, TextPair, Text, TextPair>{//输入<filename, (className, prob)>，输出<filename, (maxClass, prob)>
	private static final Logger LOG = Logger.getLogger(NBClassifierReducer.class);
	public void reduce(Text key, Iterable<TextPair> value, Context context)
		throws IOException, InterruptedException {
		
		double maxProb = -Double.MAX_VALUE;
		String maxClass = "noclass";
		String className;
		for(TextPair val : value){//遍历一个文件对应所有类的nb概率
			className = val.getFirst().toString();
			double prob  = Double.valueOf(val.getSecond().toString());
			
			
			LOG.info(className+prob);
			//迭代更新最大nb概率及其类名
			if(prob > maxProb){
				maxProb = prob;
				maxClass = className;
				LOG.info("get here");
			}	
		}
		TextPair maxClassAndProb = new TextPair(maxClass, Double.toString(maxProb));
		context.write(key, maxClassAndProb);
	}
	
}
