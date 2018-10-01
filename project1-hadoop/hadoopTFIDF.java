// implement TF-IDF using hadoop  
// Sept26 2018
// Group Soheyla, Shengming, Roxana
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class hadoopTFIDF {
	/**
	 * tfMaper: implements that the TF job for the TF-IDF framework
	 */
	
	public static class tfMapper 
			extends Mapper<Object, Text, Text, IntWritable>{
		 /**
	       * Google's search Stopwords
	       */
	      private static Set<String> googleStopwords;
	      static {
	    	  googleStopwords = new HashSet<String>();
	          googleStopwords.add("I"); googleStopwords.add("a");
	          googleStopwords.add("about"); googleStopwords.add("an");
	          googleStopwords.add("are"); googleStopwords.add("as");
	          googleStopwords.add("at"); googleStopwords.add("be");
	          googleStopwords.add("by"); googleStopwords.add("com");
	          googleStopwords.add("de"); googleStopwords.add("en");
	          googleStopwords.add("for"); googleStopwords.add("from");
	          googleStopwords.add("how"); googleStopwords.add("in");
	          googleStopwords.add("is"); googleStopwords.add("it");
	          googleStopwords.add("la"); googleStopwords.add("of");
	          googleStopwords.add("on"); googleStopwords.add("or");
	          googleStopwords.add("that"); googleStopwords.add("the");
	          googleStopwords.add("this"); googleStopwords.add("to");
	          googleStopwords.add("was"); googleStopwords.add("what");
	          googleStopwords.add("when"); googleStopwords.add("where");
	          googleStopwords.add("who"); googleStopwords.add("will");
	          googleStopwords.add("with"); googleStopwords.add("and");
	          googleStopwords.add("the"); googleStopwords.add("www");
	      }// static googleStopword
	      /**
	       * @param key is the byte offset of the current line in the file;
	       * @param value is the line from the file
	       * @param output has the method "collect()" to output the key,value pair
	       * @param reporter allows us to retrieve some information about the job (like the current filename)
	       *
	       *     POST-CONDITION: Output <"word", "filename@offset"> pairs
	       */
	      public void map(LongWritable key, Text value, Context context)) 
			throws IOException, InteruptedExcetion {
				// keep only words and numbers
				Pattern p = Pattern.compile("\\w+"); // http://www.ntu.edu.sg/home/ehchua/programming/howto/Regexe.html
		        Matcher m = p.matcher(value.toString()); // Alan's -> 1)Alan //->"2) Alans"<-// 3)Alan s
		        
		        //@TODO: import word-list.txt, then check it use a stemmer 
		        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
		        public static Set<String> fileNames;
		        fileNames.add(fileName);
		        if (fileName == "vocab.txt") { continue; /*do nothing*/ }
		        // build the values and write <k,v> pairs through the context
		        StringBuilder valuePairBuilder = new StringBuilder();
		        while ( m.find() ) {
		        	String mathedKey = m.group().toLowerCase():
		        	//@TODO using stemmer()
		        	// remove names starting with non letters, digits, considered stopwords or containing other chars
		            if (!Character.isLetter(matchedKey.charAt(0)) || Character.isDigit(matchedKey.charAt(0))
		            		|| googleStopwords.contains(matchedKey) || matchedKey.contains("_")) {
		                    continue; // do nothing
		            }
		        	valuePairBuilder.append(matchedKey);
		            valuePairBuilder.append("@");
		            valuePairBuilder.append(fileName);
		            // emit the pair <word@filename, 1>
		            //@ Blame Shengming for the IntWritable(1) or to "One" as a flag "private final static IntWritable one = new IntWritable(1);"
		            context.write(new Text(valueBuilder.toString()), new IntWritable(1));		            
		        }// while loop end
			}// map function
	      }// mapper class
}// class hadoopTFIDF