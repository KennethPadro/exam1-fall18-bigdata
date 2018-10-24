package mapreduce;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;


public class wordtweetid {

	public static class wordtweetidMapper extends Mapper<LongWritable, Text, Text, Text>{


		private Text word = new Text();
		private final static String[] inputArr = {"flu", "zika", "diarrhea", "ebola", "swamp", "change"};


		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String json = value.toString();
			String[] line = json.split("\\n");
			StringTokenizer tokenizer_input;
			String tweet;
			String id;
			
			try {
				
				for(int i=0;i<line.length; i++){
					JSONObject obj = new JSONObject(line[i]);
					tweet = (String) ((JSONObject) obj.get("extended_tweet")).get("full_text");
					tweet = tweet.replaceAll("[^a-zA-Z]", " ");
					tokenizer_input = new StringTokenizer(tweet,"\n \t \r \f : - ? !");

					String temp;
					while (tokenizer_input.hasMoreTokens()) {
						temp=tokenizer_input.nextToken();
						for (int i1=0;i1<inputArr.length;i1++){
							if (temp.equalsIgnoreCase(inputArr[i1])){
								word.set(inputArr[i1]);
								id = (String) obj.get("id_str");
								context.write(word, new Text(id));
							}
						}
					}
				}
			}catch(JSONException e){
				e.printStackTrace();
			}
		}
	}


	public static class wordtweetidReducer extends Reducer<Text, Text, Text, Text> {
		private IntWritable result = new IntWritable();

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

	        String contents = "";

	        for (Text value : values){
	            contents += value.toString()+", ";
	        }
	        context.write(key, new Text(contents));
	    }
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "word count");
		job.setJarByClass(wordtweetid.class);
		job.setMapperClass(wordtweetidMapper.class);
		job.setCombinerClass(wordtweetidReducer.class);
		job.setReducerClass(wordtweetidReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
