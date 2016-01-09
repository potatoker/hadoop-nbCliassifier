package ray.nbclassify;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import ray.TextPair;

public class NBClassifierMapper extends Mapper<TextPair, Text, Text, TextPair> {// 输入<(className,
																				// filename),
																				// content>
																				// 输出<filename,
																				// (className,
																				// prob)>
	private static final Logger LOG = Logger.getLogger(NBClassifierMapper.class);

//	private static final String CLASS_FILE_NUM_PATH = "/home/raymond/workspace/MaxTemperature/src/ray/nbclassify/part-r-00000"; // 存储<className,
																																// num>的sequencefile
																																// 的相对路径
	//private static final String TOKEN_IN_CLASS_NUM_PATH = "/home/raymond/workspace/MaxTemperature/trainout/part-r-00000"; // 存储<(className,
																															// word),
																															// num>的sqquencefile的相对路径
	
	private static final String TOKEN_IN_CLASS_NUM_PATH = "token_in_class_num_path";
	private static final String CLASS_FILE_NUM_PATH ="class_file_num_path";
			
	private ArrayList<String> classNames; // 所有类的名称
	private String wordRegex = "[a-zA-Z]+"; // 表示纯英文单词的正则
	private Map<String, Integer> tokenNumInClassHmap; // 存储每个类的单词总数<className,
														// tokenNum>的哈希表
	private Map<TextPair, Integer> tokenInClassNumHmap; // 存储不同类中某单词数<(className,
														// word), num>的哈希表
	private Map<String, Double> classProbHmap; // 存储每个类的先验概率
	private Map<TextPair, Double> tokenInClassProbHmap; // 存储每个单词对应每个类的p(token|class)的哈希表
	private double word_dict_num; // 所有训练文件的字典单词数
	private Configuration conf;
	private FileSystem fs;

	private Text fname;
	private TextPair classAndProb;

	protected void setup(Context context) throws IOException, InterruptedException {

		conf = context.getConfiguration();
		// classNames = conf.get(CLASS_NAMES).split("/");
		classNames = new ArrayList<String>();
		classProbHmap = new HashMap<String, Double>();
		tokenInClassProbHmap = new HashMap<TextPair, Double>();
		try {
			fs = FileSystem.get(new URI("hdfs:///"), conf);
		} catch (URISyntaxException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	//	fs = FileSystem.get(conf);
		getClassProbHmap();
		getTokenProbHmap();

		super.setup(context);
	}

	public void map(TextPair key, Text value, Context context) throws IOException, InterruptedException {

		if (key.getSecond() != null) {
			LOG.info("fname not null");
		}

		String fileName = key.getSecond().toString();// 得到文件名，将一直作为最终的key
		fname = new Text(fileName);
		String word;// 每次提取的单词

		double tokenProbSum;

		// 循环计算这个文件对于每个类的nb概率
		for (String className : classNames) {
			//对于每一个类都要遍历一遍文档内容，所以要重新声明itr,这可能是以后可以进一步优化的地方
			StringTokenizer itr = new StringTokenizer(value.toString());
			tokenProbSum = 0;
			// 拆解每个单词
			while (itr.hasMoreTokens()) {
				word = itr.nextToken();
				if (word.matches(wordRegex)) {// 是一个英文单词才会被计入
					TextPair classAndWord = new TextPair(className, word);// 组成(className,
																			// word)的pair

					if (tokenInClassProbHmap.containsKey(classAndWord)) {// 如果含有(className,
																			// word)的key则之前已经计算过p(token|class)
						tokenProbSum += Math.log10(tokenInClassProbHmap.get(classAndWord)); // 此时取出数据并进行log变换即可

					} else {

						tokenProbSum += Math.log10(1 / (word_dict_num + tokenNumInClassHmap.get(className)));// 若果不含有，表明这个类中没有该单词，但仍要进行平滑处理


					}
				} // if end
			} // while end

			tokenProbSum += Math.log10(classProbHmap.get(className));// 加上这个类的先验概率
			LOG.info("classProbHmap:" + classProbHmap.get(className));
			classAndProb = new TextPair(className, Double.toString(tokenProbSum));
			LOG.info(fileName + " " + classAndProb.toString());
			context.write(fname, classAndProb);
		} // for end

	}

	// 计算classProbHmap的函数
	public void getClassProbHmap() throws IOException {

		

		// 将<className, num>的sequencefile 每个类的文件数读入哈希表
		Path class_file_num_path = new Path(conf.get(CLASS_FILE_NUM_PATH));
	//	Path class_file_num_path = new Path(CLASS_FILE_NUM_PATH);
		SequenceFile.Reader classNumReader = new SequenceFile.Reader(fs, class_file_num_path, conf);
		if (classNumReader != null) {
			LOG.info("classNumReader not null");
		}
		Map<String, Integer> fileInClassNumHmap = new HashMap<String, Integer>();
		Text key = new Text();
		IntWritable val = new IntWritable();
		double fileTotal = 0;
		while (classNumReader.next(key, val)) {
			fileInClassNumHmap.put(key.toString(), val.get());
			fileTotal += val.get();
			classNames.add(key.toString());

		}
		classNumReader.close();

		// 通过fileInClassNumHmap得到类的先验概率
		Iterator classProbIt = fileInClassNumHmap.keySet().iterator();
		String itkey = "";
		while (classProbIt.hasNext()) {
			itkey = (String) classProbIt.next();
			double prob = fileInClassNumHmap.get(itkey) / fileTotal;
			classProbHmap.put(itkey, prob);
		}

	}

	// 计算tokenInClassProbHmap 每个单词对应每个类的p(token|class)的哈希表
	public void getTokenProbHmap() throws IOException {
		TextPair tmpkey = new TextPair();
		IntWritable tmpvalue = new IntWritable();
		int tmpintval;
		String tmpClassName;
		String tmpword;
		double fprob;
		// 把存储有<(className, word), num>的sequencefile读入哈希表tokenInClassNumHmap

		 Path token_in_class_num_path = new  Path(conf.get(TOKEN_IN_CLASS_NUM_PATH));
		//Path token_in_class_num_path = new Path(TOKEN_IN_CLASS_NUM_PATH);
		SequenceFile.Reader reader = new SequenceFile.Reader(fs, token_in_class_num_path, conf);

		if (reader != null) {
			LOG.info("reader not null");
		}

		tokenInClassNumHmap = new HashMap<TextPair, Integer>();
		while (reader.next(tmpkey, tmpvalue)) {
			TextPair nkey = new TextPair(tmpkey); // 之前这里没有注意，必须重新声明变量TextPait作为key，否则TextPair作为可变变量将会被修改，而hashmap的key是必须保证不被修改的
			tokenInClassNumHmap.put(nkey, tmpvalue.get());
		}
		reader.close();

		// 通过哈希表tokenInClassNumHmap<(className, word), num>得到每个类的单词总数哈希
		tokenNumInClassHmap = new HashMap<String, Integer>();

		Iterator<TextPair> it = tokenInClassNumHmap.keySet().iterator();

		while (it.hasNext()) {

			tmpkey = (TextPair) it.next();
			tmpClassName = tmpkey.getFirst().toString();
			tmpintval = tokenInClassNumHmap.get(tmpkey);

			// 如果该类第一次被读入，则直接把记录读入hash表，如果不是，则将单词数累加到hash表中
			if (!tokenNumInClassHmap.containsKey(tmpClassName)) {
				tokenNumInClassHmap.put(tmpClassName, tmpintval);
			} else {
				tokenNumInClassHmap.put(tmpClassName, tokenNumInClassHmap.get(tmpClassName) + tmpintval);
			}
		}

		// 通过哈希表tokenInClassNumHmap<(className, word),
		// num>得到字典dictHmap及其中的单词数word_dict_num
		Map<String, Integer> dictHmap = new HashMap<String, Integer>();
		Iterator<TextPair> it1 = tokenInClassNumHmap.keySet().iterator();

		while (it1.hasNext()) {
			tmpkey = (TextPair) it1.next();
			tmpword = tmpkey.getSecond().toString();
			dictHmap.put(tmpword, null);
		}
		
		word_dict_num = dictHmap.size();//得到字典数

		// 使用上述所有数据得到最终的p(token|class)哈希表tokenInClassProbHmap
		Iterator<TextPair> it2 = tokenInClassNumHmap.keySet().iterator();

		int Tct;
		int segmaTct;

		while (it2.hasNext()) {
			TextPair nkey2 = (TextPair) it2.next();
			tmpClassName = nkey2.getFirst().toString();
			Tct = tokenInClassNumHmap.get(nkey2);
			segmaTct = tokenNumInClassHmap.get(tmpClassName);

			fprob = (Tct + 1) / (segmaTct + word_dict_num);

			tokenInClassProbHmap.put(nkey2, fprob);

		}
	}

}
