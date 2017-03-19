/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.temp;

import proj.analysis.mappers.Q1Job1Mapper;
import proj.analysis.mappers.Q1Job2Mapper;
import proj.analysis.partitioners.Q1Job1Partitioner;
import proj.analysis.reducers.Q1Job1Reducer;
import proj.analysis.reducers.Q1Job2Reducer;
import proj.analysis.util.Q1CustomDataType;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 *
 * @author namanrs
 */
public class AnalysisJobCopy {

    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn
            // webapp.
            Job ques1Job1 = Job.getInstance(conf, "CopyData");
            // Current class.
            ques1Job1.setJarByClass(AnalysisJobCopy.class);
            // Mapper
            ques1Job1.setMapperClass(CopyMapper.class);
            // Reducer
            ques1Job1.setReducerClass(CopyReducer.class);
//            ques1Job1.setPartitionerClass(Q1Job1Partitioner.class);
            // Outputs from the Mapper.
            ques1Job1.setMapOutputKeyClass(Text.class);
            ques1Job1.setMapOutputValueClass(Text.class);
            ques1Job1.setOutputKeyClass(NullWritable.class);
            ques1Job1.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(ques1Job1, new Path(args[0]));
            FileSystem fileSystem = FileSystem.get(conf);
//            fileSystem.mkdirs(new Path(args[1] + "/Question1/temp"));
            String outputPath = args[1];

            if (fileSystem.exists(new Path(outputPath))) {
                fileSystem.delete(new Path(outputPath), true);
            }
            // path to output in HDFS
            FileOutputFormat.setOutputPath(ques1Job1, new Path(outputPath));
            // Block until the job is completed.
            boolean isCompleted = ques1Job1.waitForCompletion(true);
//            //Second job
//            if (isCompleted) {
//                Job ques2Job2 = Job.getInstance(conf, "ProcessData");
//                ques2Job2.setJarByClass(AnalysisJobCopy.class);
//                // Mapper
//                ques2Job2.setMapperClass(Q1Job2Mapper.class);
//                // Reducer
//                ques2Job2.setReducerClass(Q1Job2Reducer.class);
//                ques2Job2.setNumReduceTasks(1);
//                // Outputs from the Mapper.
//                ques2Job2.setMapOutputKeyClass(NullWritable.class);
//                ques2Job2.setMapOutputValueClass(Text.class);
//                // Outputs from Reducer. It is sufficient to set only the following
//                // two properties
//                // if the Mapper and Reducer has same key and value types. It is set
//                // separately for
//                // elaboration.
//                ques2Job2.setOutputKeyClass(NullWritable.class);
//                ques2Job2.setOutputValueClass(Text.class);
//                // path to input in HDFS
//                FileInputFormat.addInputPath(ques2Job2, new Path(outputPath));
////            FileSystem fileSystem = FileSystem.get(conf1);
//
//                String outputPath2 = args[1] + "/Question1";
//
//                if (fileSystem.exists(new Path(outputPath2))) {
//                    fileSystem.delete(new Path(outputPath2), true);
//                }
//                // path to output in HDFS
//                FileOutputFormat.setOutputPath(ques2Job2, new Path(outputPath2));
//                System.exit(ques2Job2.waitForCompletion(true) ? 0 : 1);
//            }
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }

}
