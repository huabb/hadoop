package PageRank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;

import javax.swing.text.AbstractDocument.Content;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.GenericOptionsParser;

public class PageRank {
   private static int times=10;
   public static class PRIterMapper extends Mapper<Object,Text,Text,Text>{
	   public void map(Object key,Text value,Context context)throws IOException, InterruptedException{
		   
	   }
   }
   
   public static class PRIterReducer extends Reducer<Text, Text, Text, Text>
   {
	   public void reduce(Text key,Iterable<Text> values,Context context)throws IOException,InterruptedException{
		   
	   }
   }
  
   public static class PRViewerMapper extends Mapper<LongWritable, Text, Text, Text> {
	   public void map(LongWritable key,Text value,Context context)throws IOException,InterruptedException{
		   
	   }   
   }

   public static void main(String[] args)throws Exception {
	    Configuration conf = new Configuration();
		String[] otherArgs=new GenericOptionsParser(conf,args).getRemainingArgs();
        if (otherArgs.length!=2) {
           System.err.println("Usage:PageRank<in><out>");
           System.exit(2);
        }
        
		Path outputPath = new Path(args[1]);
		outputPath.getFileSystem(conf).delete(outputPath, true);
		String outTempString = "TEMP";
		Path outputPath1 = new Path(outTempString);
		outputPath.getFileSystem(conf).delete(outputPath1, true);
		
		Job jobIter1=new Job(conf,"jobIter1");
		jobIter1.setJarByClass(PageRank.class);
		jobIter1.setMapperClass(PRIterMapper.class);
		jobIter1.setMapOutputKeyClass(Text.class);
		jobIter1.setMapOutputValueClass(Text.class);
		jobIter1.setReducerClass(PRIterReducer.class);
	    jobIter1.setOutputKeyClass(Text.class);
		jobIter1.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIter1, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(jobIter1, new Path(outTempString+"/Data1"));
        jobIter1.waitForCompletion(true);
        
        Job jobIter2=new Job(conf,"jobIter2");
		jobIter2.setJarByClass(PageRank.class);
		jobIter2.setMapperClass(PRIterMapper.class);
		jobIter2.setMapOutputKeyClass(Text.class);
		jobIter2.setMapOutputValueClass(Text.class);
		jobIter2.setReducerClass(PRIterReducer.class);
	    jobIter2.setOutputKeyClass(Text.class);
		jobIter2.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIter2, new Path(outTempString+"/Data1"));
        FileOutputFormat.setOutputPath(jobIter2, new Path(outTempString+"/Data2"));
        jobIter2.waitForCompletion(jobIter1.isComplete());
        
        Job jobIter3=new Job(conf,"jobIter3");
		jobIter3.setJarByClass(PageRank.class);
		jobIter3.setMapperClass(PRIterMapper.class);
		jobIter3.setMapOutputKeyClass(Text.class);
		jobIter3.setMapOutputValueClass(Text.class);
		jobIter3.setReducerClass(PRIterReducer.class);
	    jobIter3.setOutputKeyClass(Text.class);
		jobIter3.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIter3, new Path(outTempString+"/Data2"));
        FileOutputFormat.setOutputPath(jobIter3, new Path(outTempString+"/Data3"));
        jobIter3.waitForCompletion(jobIter2.isComplete());
        
        Job jobIter4=new Job(conf,"jobIter4");
		jobIter4.setJarByClass(PageRank.class);
		jobIter4.setMapperClass(PRIterMapper.class);
		jobIter4.setMapOutputKeyClass(Text.class);
		jobIter4.setMapOutputValueClass(Text.class);
		jobIter4.setReducerClass(PRIterReducer.class);
	    jobIter4.setOutputKeyClass(Text.class);
		jobIter4.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIter4, new Path(outTempString+"/Data3"));
        FileOutputFormat.setOutputPath(jobIter4, new Path(outTempString+"/Data4"));
        jobIter4.waitForCompletion(jobIter3.isComplete());
        
        Job jobIter5=new Job(conf,"jobIter5");
		jobIter5.setJarByClass(PageRank.class);
		jobIter5.setMapperClass(PRIterMapper.class);
		jobIter5.setMapOutputKeyClass(Text.class);
		jobIter5.setMapOutputValueClass(Text.class);
		jobIter5.setReducerClass(PRIterReducer.class);
	    jobIter5.setOutputKeyClass(Text.class);
		jobIter5.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIter5, new Path(outTempString+"/Data4"));
        FileOutputFormat.setOutputPath(jobIter5, new Path(outTempString+"/Data5"));
        jobIter5.waitForCompletion(jobIter4.isComplete());
        
        Job jobIter6=new Job(conf,"jobIter6");
		jobIter6.setJarByClass(PageRank.class);
		jobIter6.setMapperClass(PRIterMapper.class);
		jobIter6.setMapOutputKeyClass(Text.class);
		jobIter6.setMapOutputValueClass(Text.class);
		jobIter6.setReducerClass(PRIterReducer.class);
	    jobIter6.setOutputKeyClass(Text.class);
		jobIter6.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIter6, new Path(outTempString+"/Data5"));
        FileOutputFormat.setOutputPath(jobIter6, new Path(outTempString+"/Data6"));
        jobIter6.waitForCompletion(jobIter5.isComplete());
        
        Job jobIter7=new Job(conf,"jobIter7");
		jobIter7.setJarByClass(PageRank.class);
		jobIter7.setMapperClass(PRIterMapper.class);
		jobIter7.setMapOutputKeyClass(Text.class);
		jobIter7.setMapOutputValueClass(Text.class);
		jobIter7.setReducerClass(PRIterReducer.class);
	    jobIter7.setOutputKeyClass(Text.class);
		jobIter7.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIter7, new Path(outTempString+"/Data6"));
        FileOutputFormat.setOutputPath(jobIter7, new Path(outTempString+"/Data7"));
        jobIter7.waitForCompletion(jobIter6.isComplete());
        
        Job jobIter8=new Job(conf,"jobIter8");
		jobIter8.setJarByClass(PageRank.class);
		jobIter8.setMapperClass(PRIterMapper.class);
		jobIter8.setMapOutputKeyClass(Text.class);
		jobIter8.setMapOutputValueClass(Text.class);
		jobIter8.setReducerClass(PRIterReducer.class);
	    jobIter8.setOutputKeyClass(Text.class);
		jobIter8.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIter8, new Path(outTempString+"/Data7"));
        FileOutputFormat.setOutputPath(jobIter8, new Path(outTempString+"/Data8"));
        jobIter8.waitForCompletion(jobIter7.isComplete());
        
        Job jobIter9=new Job(conf,"jobIter9");
		jobIter9.setJarByClass(PageRank.class);
		jobIter9.setMapperClass(PRIterMapper.class);
		jobIter9.setMapOutputKeyClass(Text.class);
		jobIter9.setMapOutputValueClass(Text.class);
		jobIter9.setReducerClass(PRIterReducer.class);
	    jobIter9.setOutputKeyClass(Text.class);
		jobIter9.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIter9, new Path(outTempString+"/Data8"));
        FileOutputFormat.setOutputPath(jobIter9, new Path(outTempString+"/Data9"));
        jobIter9.waitForCompletion(jobIter8.isComplete());
        
        Job jobIter10=new Job(conf,"jobIter10");
		jobIter10.setJarByClass(PageRank.class);
		jobIter10.setMapperClass(PRIterMapper.class);
		jobIter10.setMapOutputKeyClass(Text.class);
		jobIter10.setMapOutputValueClass(Text.class);
		jobIter10.setReducerClass(PRIterReducer.class);
	    jobIter10.setOutputKeyClass(Text.class);
		jobIter10.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobIter10, new Path(outTempString+"/Data9"));
        FileOutputFormat.setOutputPath(jobIter10, new Path(outTempString+"/Data10"));
        jobIter10.waitForCompletion(jobIter9.isComplete());
        
        Job jobViewer=new Job(conf,"jobViewer");
        jobViewer.setJarByClass(PageRank.class);
        jobViewer.setMapperClass(PRViewerMapper.class);
        jobViewer.setNumReduceTasks(0);
        jobViewer.setOutputKeyClass(Text.class);
        jobViewer.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(jobViewer, new Path(outTempString+"/Data10"));
        FileOutputFormat.setOutputPath(jobViewer, new Path(otherArgs[1]));
        jobViewer.waitForCompletion(jobIter10.isComplete());
		
}
}
