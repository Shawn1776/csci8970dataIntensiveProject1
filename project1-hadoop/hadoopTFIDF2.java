import java.io.IOException; 
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
/**
 * This Mapper function is goona count the total number of words for each file.
 * And then, calcultate the term frequency for each word.
 * @author Shengming, Soheyla, Roxana
 * @date Oct, 01, 2018
 */ 

 class TFMapper extends Mapper<LongWritable, Text, Text, Text> {
	 
    public TFMapper() {
    }
 
    /**
     * PreCondition:
     * Here we have the number of each word by its document.
     *   book@file1.txt 14 ...
     *   
     * PostCondition:
     * The output would be: the file name and the word frequency.
     * file1.txt, book=14
     */
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] wordAndDocCounter = value.toString().split("\t");
        String[] wordAndDoc = wordAndDocCounter[0].split("@");
        context.write(new Text(wordAndDoc[1]), new Text(wordAndDoc[0] + "=" + wordAndDocCounter[1]));
    }
}

/**
* This is the Reducer function that groups by the filename and the total number of the words.
* 
* 
*/ 
 class TFReducer extends Reducer<Text, Text, Text, Text> {
	 
    public TFReducer() {
    }
    
    /**
     * PreCondition:
     * The input would be: the file name and the word frequency.
     * <"file1.txt," {book=14, b2=3 ``}>
     *   
     * PostCondition:
     * The output would be: the  term frequency for each worh in every file.
     * book@file1.txt 14/[total number]
     */
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int sumOfWordsInDocument = 0;
        Map<String, Integer> tempCounter = new HashMap<String, Integer>();
        for (Text val : values) {
            String[] wordCounter = val.toString().split("=");
            tempCounter.put(wordCounter[0], Integer.valueOf(wordCounter[1]));
            sumOfWordsInDocument += Integer.parseInt(val.toString().split("=")[1]);
        }
        for (String wordKey : tempCounter.keySet()) {
            context.write(new Text(wordKey + "@" + key.toString()), new Text(tempCounter.get(wordKey) + "/"
                    + sumOfWordsInDocument));
        }
    }
}
    
/**
* This part is the Driver code.
* 
* 
*/ 
public class hadoopTFIDF2 extends Configured implements Tool {
	 
    // where to put the data in hdfs when we're done
    private static final String OUTPUT_PATH = "/output3";
 
    // where to read the data from.
    private static final String INPUT_PATH = "/output2";
 
    public int run(String[] args) throws Exception {
 
        Configuration conf = getConf();
        Job job = new Job(conf, "TF-IDF part_2");
 
        job.setJarByClass(hadoopTFIDF2.class);
        job.setMapperClass(TFMapper.class);
        job.setReducerClass(TFReducer.class);
 
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
 
        FileInputFormat.addInputPath(job, new Path(INPUT_PATH));
        FileOutputFormat.setOutputPath(job, new Path(OUTPUT_PATH));
 
        return job.waitForCompletion(true) ? 0 : 1;
    }
 
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new hadoopTFIDF2(), args);
        System.exit(res);
    }
}   
    
    
    