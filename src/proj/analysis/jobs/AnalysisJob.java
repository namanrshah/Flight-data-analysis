/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package proj.analysis.jobs;

import proj.analysis.combiners.Q1Job1Combiner;
import proj.analysis.combiners.Q3NewJob1Combiner;
import proj.analysis.combiners.Q5Job1Combiner;
import proj.analysis.combiners.Q6Job1Combiner;
import proj.analysis.combiners.Q7Job1Combiner;
import proj.analysis.mappers.Q1Job1Mapper;
import proj.analysis.mappers.Q1Job2Mapper;
import proj.analysis.mappers.Q3NewJob1Mapper;
import proj.analysis.mappers.Q3NewJob2Mapper;
import proj.analysis.mappers.Q4Job1Mapper;
import proj.analysis.mappers.Q4Job2Mapper;
import proj.analysis.mappers.Q5Job1Mapper;
import proj.analysis.mappers.Q5Job2Mapper;
import proj.analysis.mappers.Q6Job1Mapper;
import proj.analysis.mappers.Q6Job2Mapper;
import proj.analysis.mappers.Q7Job1Mapper;
import proj.analysis.mappers.Q7Job2Mapper;
import proj.analysis.reducers.Q1Job1Reducer;
import proj.analysis.reducers.Q1Job2Reducer;
import proj.analysis.reducers.Q3NewJob1Reducer;
import proj.analysis.reducers.Q3NewJob2Reducer;
import proj.analysis.reducers.Q4Job1Reducer;
import proj.analysis.reducers.Q4Job2Reducer;
import proj.analysis.reducers.Q5Job1Reducer;
import proj.analysis.reducers.Q5Job2Reducer;
import proj.analysis.reducers.Q6Job1Reducer;
import proj.analysis.reducers.Q6Job2Reducer;
import proj.analysis.reducers.Q7Job1Reducer;
import proj.analysis.reducers.Q7Job2Reducer;
import proj.analysis.util.Q1CustomDataType;
import proj.analysis.util.Q3CustomDataType;
import proj.analysis.util.Q5CustomDataType;
import proj.analysis.util.Q6Job1CustomDataType;
import proj.analysis.util.Q7Job1CustomDataType;
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
public class AnalysisJob {

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
//        try {
        Configuration conf = new Configuration();
        FileSystem fileSystem = FileSystem.get(conf);
//        args[0] = input location for data
//        args[1] = input location for airports
//        args[2] = input location for carriers
//        args[3] = input location for plane-data
//        args[4] = output file location
        // Give the MapRed job a name. You'll see this name in the Yarn
        // webapp.
        Job ques1Job1 = Job.getInstance(conf, "AnalyzeFlights");
        // Current class.
        ques1Job1.setJarByClass(AnalysisJob.class);
        // Mapper
        ques1Job1.setMapperClass(Q1Job1Mapper.class);
        // Reducer
        ques1Job1.setReducerClass(Q1Job1Reducer.class);
        ques1Job1.setCombinerClass(Q1Job1Combiner.class);
//        ques1Job1.setPartitionerClass(Q1Job1Partitioner.class);
        // Outputs from the Mapper.
        ques1Job1.setMapOutputKeyClass(Text.class);
        ques1Job1.setMapOutputValueClass(Q1CustomDataType.class);
        ques1Job1.setOutputKeyClass(NullWritable.class);
        ques1Job1.setOutputValueClass(Text.class);
        // path to input in HDFS
        FileInputFormat.addInputPath(ques1Job1, new Path(args[0]));

        fileSystem.mkdirs(new Path(args[4] + "_Question_1_temp"));
        String outputPath = args[4] + "_Question_1_temp";

        if (fileSystem.exists(new Path(outputPath))) {
            fileSystem.delete(new Path(outputPath), true);
        }
        // path to output in HDFS
        FileOutputFormat.setOutputPath(ques1Job1, new Path(outputPath));
        // Block until the job is completed.
        boolean isCompleted = ques1Job1.waitForCompletion(true);
        //Second job
        if (isCompleted) {
            Job ques1Job2 = Job.getInstance(conf, "ProcessData");
            ques1Job2.setJarByClass(AnalysisJob.class);
            // Mapper
            ques1Job2.setMapperClass(Q1Job2Mapper.class);
            // Reducer
            ques1Job2.setReducerClass(Q1Job2Reducer.class);
            ques1Job2.setNumReduceTasks(1);
            // Outputs from the Mapper.
            ques1Job2.setMapOutputKeyClass(NullWritable.class);
            ques1Job2.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties
            // if the Mapper and Reducer has same key and value types. It is set
            // separately for
            // elaboration.
            ques1Job2.setOutputKeyClass(NullWritable.class);
            ques1Job2.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(ques1Job2, new Path(outputPath));
//            FileSystem fileSystem = FileSystem.get(conf1);

            String outputPath2 = args[4] + "_Question1";

            if (fileSystem.exists(new Path(outputPath2))) {
                fileSystem.delete(new Path(outputPath2), true);
            }
            // path to output in HDFS
            FileOutputFormat.setOutputPath(ques1Job2, new Path(outputPath2));
            ques1Job2.waitForCompletion(true);
//            System.exit(ques1Job2.waitForCompletion(true) ? 0 : 1);
        }
        /////////////////////// Job for Ques3 /////////////////////////////////////////
        // Give the MapRed job a name. You'll see this name in the Yarn
        // webapp.
//        Job ques3Job1 = Job.getInstance(conf, "AnalyzeFlightsQ3");
//        // Current class.
//        ques3Job1.setJarByClass(AnalysisJob.class);
//        // Mapper
//        ques3Job1.setMapperClass(Q3Job1Mapper.class);
//        // Reducer
//        ques3Job1.setReducerClass(Q3Job1Reducer.class);
//        ques3Job1.setCombinerClass(Q3Job1Combiner.class);
////            ques3Job1.setPartitionerClass(Q1Job1Partitioner.class);
//        // Outputs from the Mapper.
//        ques3Job1.setMapOutputKeyClass(Text.class);
////        ques3Job1.setMapOutputValueClass(Text.class);
//        ques3Job1.setMapOutputValueClass(Q3CustomDataType.class);
//        ques3Job1.setOutputKeyClass(NullWritable.class);
//        ques3Job1.setOutputValueClass(Text.class);
//        // path to input in HDFS
//        FileInputFormat.addInputPath(ques3Job1, new Path(args[0]));
////                FileSystem fileSystem = FileSystem.get(conf);
////                fileSystem.mkdirs(new Path(args[4] + "_Question_3_temp"));
//        String outputPathQ3 = args[4] + "_Question_3_temp";
//
//        if (fileSystem.exists(new Path(outputPathQ3))) {
//            fileSystem.delete(new Path(outputPathQ3), true);
//        }
//        // path to output in HDFS
//        FileOutputFormat.setOutputPath(ques3Job1, new Path(outputPathQ3));
//        // Block until the job is completed.
//        boolean isCompletedQ3 = ques3Job1.waitForCompletion(true);
//        //Second job
//        if (isCompletedQ3) {
//            Job ques3Job2 = Job.getInstance(conf, "ProcessDataQ3");
//            ques3Job2.setJarByClass(AnalysisJob.class);
//            // Mapper
//            ques3Job2.setMapperClass(Q3Job2Mapper.class);
//            // Reducer
//            ques3Job2.setReducerClass(Q3Job2Reducer.class);
//            // Outputs from the Mapper.
//            ques3Job2.setMapOutputKeyClass(NullWritable.class);
//            ques3Job2.setMapOutputValueClass(Text.class);
//            // Outputs from Reducer. It is sufficient to set only the following
//            // two properties
//            // if the Mapper and Reducer has same key and value types. It is set
//            // separately for
//            // elaboration.
//            ques3Job2.setOutputKeyClass(NullWritable.class);
//            ques3Job2.setOutputValueClass(Text.class);
//            // path to input in HDFS
//            FileInputFormat.addInputPath(ques3Job2, new Path(outputPathQ3));
////            FileSystem fileSystem = FileSystem.get(conf1);
//
//            String outputPath2Q3 = args[4] + "_Question3";
//
//            if (fileSystem.exists(new Path(outputPath2Q3))) {
//                fileSystem.delete(new Path(outputPath2Q3), true);
//            }
//            // path to output in HDFS
//            FileOutputFormat.setOutputPath(ques3Job2, new Path(outputPath2Q3));
//            ques3Job2.waitForCompletion(true);
////            System.exit(ques3Job2.waitForCompletion(true) ? 0 : 1);
//
//        }
        Job ques3Job1 = Job.getInstance(conf, "AnalyzeFlightsQ3");
        // Current class.
        ques3Job1.setJarByClass(AnalysisJob.class);
        // Mapper
        ques3Job1.setMapperClass(Q3NewJob1Mapper.class);
        // Reducer
        ques3Job1.setReducerClass(Q3NewJob1Reducer.class);
        ques3Job1.setCombinerClass(Q3NewJob1Combiner.class);
//            ques3Job1.setPartitionerClass(Q1Job1Partitioner.class);
        // Outputs from the Mapper.
        ques3Job1.setMapOutputKeyClass(Text.class);
//        ques3Job1.setMapOutputValueClass(Text.class);
        ques3Job1.setMapOutputValueClass(Q3CustomDataType.class);
        ques3Job1.setOutputKeyClass(NullWritable.class);
        ques3Job1.setOutputValueClass(Text.class);
        // path to input in HDFS
        FileInputFormat.addInputPath(ques3Job1, new Path(args[0]));
        FileInputFormat.addInputPath(ques3Job1, new Path(args[1]));
//                FileSystem fileSystem = FileSystem.get(conf);
//                fileSystem.mkdirs(new Path(args[4] + "_Question_3_temp"));
        String outputPathQ3 = args[4] + "_Question_3_temp";

        if (fileSystem.exists(new Path(outputPathQ3))) {
            fileSystem.delete(new Path(outputPathQ3), true);
        }
        // path to output in HDFS
        FileOutputFormat.setOutputPath(ques3Job1, new Path(outputPathQ3));
        // Block until the job is completed.
        boolean isCompletedQ3 = ques3Job1.waitForCompletion(true);
//        System.exit(0);
        //Second job
        if (isCompletedQ3) {
            Job ques3Job2 = Job.getInstance(conf, "ProcessDataQ3");
            ques3Job2.setJarByClass(AnalysisJob.class);
            // Mapper
            ques3Job2.setMapperClass(Q3NewJob2Mapper.class);
            // Reducer
            ques3Job2.setReducerClass(Q3NewJob2Reducer.class);
            // Outputs from the Mapper.
            ques3Job2.setMapOutputKeyClass(NullWritable.class);
            ques3Job2.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties
            // if the Mapper and Reducer has same key and value types. It is set
            // separately for
            // elaboration.
            ques3Job2.setOutputKeyClass(NullWritable.class);
            ques3Job2.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(ques3Job2, new Path(outputPathQ3));
//            FileSystem fileSystem = FileSystem.get(conf1);

            String outputPath2Q3 = args[4] + "_Question3";

            if (fileSystem.exists(new Path(outputPath2Q3))) {
                fileSystem.delete(new Path(outputPath2Q3), true);
            }
            // path to output in HDFS
            FileOutputFormat.setOutputPath(ques3Job2, new Path(outputPath2Q3));
            ques3Job2.waitForCompletion(true);
//            System.exit(ques3Job2.waitForCompletion(true) ? 0 : 1);

        }
        /////////////////////// Job for Ques4 /////////////////////////////////////////
        // Give the MapRed job a name. You'll see this name in the Yarn
        // webapp.
        Job ques4Job1 = Job.getInstance(conf, "AnalyzeFlightsQ4");
        // Current class.
        ques4Job1.setJarByClass(AnalysisJob.class);
        // Mapper
        ques4Job1.setMapperClass(Q4Job1Mapper.class);
        // Reducer
        ques4Job1.setReducerClass(Q4Job1Reducer.class);
//            ques3Job1.setPartitionerClass(Q1Job1Partitioner.class);
        // Outputs from the Mapper.
        ques4Job1.setMapOutputKeyClass(Text.class);
        ques4Job1.setMapOutputValueClass(Text.class);
        ques4Job1.setOutputKeyClass(NullWritable.class);
        ques4Job1.setOutputValueClass(Text.class);
        // path to input in HDFS
        FileInputFormat.addInputPath(ques4Job1, new Path(args[0]));
        FileInputFormat.addInputPath(ques4Job1, new Path(args[1]));
//                FileSystem fileSystem = FileSystem.get(conf);
//                fileSystem.mkdirs(new Path(args[4] + "_Question_3_temp"));
        String outputPathQ4 = args[4] + "_Question_4_temp";

        if (fileSystem.exists(new Path(outputPathQ4))) {
            fileSystem.delete(new Path(outputPathQ4), true);
        }
        // path to output in HDFS
        FileOutputFormat.setOutputPath(ques4Job1, new Path(outputPathQ4));
        // Block until the job is completed.
        boolean isCompletedQ4 = ques4Job1.waitForCompletion(true);
        //Second job
        if (isCompletedQ4) {
            Job ques4Job2 = Job.getInstance(conf, "ProcessDataQ4");
            ques4Job2.setJarByClass(AnalysisJob.class);
            // Mapper
            ques4Job2.setMapperClass(Q4Job2Mapper.class);
            // Reducer
            ques4Job2.setReducerClass(Q4Job2Reducer.class);
            // Outputs from the Mapper.
            ques4Job2.setMapOutputKeyClass(NullWritable.class);
            ques4Job2.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties
            // if the Mapper and Reducer has same key and value types. It is set
            // separately for
            // elaboration.
            ques4Job2.setOutputKeyClass(NullWritable.class);
            ques4Job2.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(ques4Job2, new Path(outputPathQ4));
//            FileSystem fileSystem = FileSystem.get(conf1);

            String outputPath2Q4 = args[4] + "_Question4";

            if (fileSystem.exists(new Path(outputPath2Q4))) {
                fileSystem.delete(new Path(outputPath2Q4), true);
            }
            // path to output in HDFS
            FileOutputFormat.setOutputPath(ques4Job2, new Path(outputPath2Q4));
            ques4Job2.waitForCompletion(true);
//            System.exit(ques4Job2.waitForCompletion(true) ? 0 : 1);
        }

        /////////////////////// Job for Ques5 /////////////////////////////////////////
        // Give the MapRed job a name. You'll see this name in the Yarn
        // webapp.
        Job ques5Job1 = Job.getInstance(conf, "AnalyzeFlightsQ5");
        // Current class.
        ques5Job1.setJarByClass(AnalysisJob.class);
        // Mapper
        ques5Job1.setMapperClass(Q5Job1Mapper.class);
        // Reducer
        ques5Job1.setReducerClass(Q5Job1Reducer.class);
        ques5Job1.setCombinerClass(Q5Job1Combiner.class);
//            ques3Job1.setPartitionerClass(Q1Job1Partitioner.class);
        // Outputs from the Mapper.
        ques5Job1.setMapOutputKeyClass(Text.class);
        ques5Job1.setMapOutputValueClass(Q5CustomDataType.class);
        ques5Job1.setOutputKeyClass(NullWritable.class);
        ques5Job1.setOutputValueClass(Text.class);
        // path to input in HDFS
        FileInputFormat.addInputPath(ques5Job1, new Path(args[0]));
        FileInputFormat.addInputPath(ques5Job1, new Path(args[2]));
//                FileSystem fileSystem = FileSystem.get(conf);
//                fileSystem.mkdirs(new Path(args[4] + "_Question_3_temp"));
        String outputPathQ5 = args[4] + "_Question_5_temp";

        if (fileSystem.exists(new Path(outputPathQ5))) {
            fileSystem.delete(new Path(outputPathQ5), true);
        }
        // path to output in HDFS
        FileOutputFormat.setOutputPath(ques5Job1, new Path(outputPathQ5));
        // Block until the job is completed.
        boolean isCompletedQ5 = ques5Job1.waitForCompletion(true);
//        System.exit(0);
        //Second job
        if (isCompletedQ5) {
            Job ques5Job2 = Job.getInstance(conf, "ProcessDataQ5");
            ques5Job2.setJarByClass(AnalysisJob.class);
            // Mapper
            ques5Job2.setMapperClass(Q5Job2Mapper.class);
            // Reducer
            ques5Job2.setReducerClass(Q5Job2Reducer.class);
            // Outputs from the Mapper.
            ques5Job2.setMapOutputKeyClass(NullWritable.class);
            ques5Job2.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties
            // if the Mapper and Reducer has same key and value types. It is set
            // separately for
            // elaboration.
            ques5Job2.setOutputKeyClass(NullWritable.class);
            ques5Job2.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(ques5Job2, new Path(outputPathQ5));
//            FileSystem fileSystem = FileSystem.get(conf1);

            String outputPath2Q5 = args[4] + "_Question5";

            if (fileSystem.exists(new Path(outputPath2Q5))) {
                fileSystem.delete(new Path(outputPath2Q5), true);
            }
            // path to output in HDFS
            FileOutputFormat.setOutputPath(ques5Job2, new Path(outputPath2Q5));
            ques5Job2.waitForCompletion(true);
//            System.exit(ques5Job2.waitForCompletion(true) ? 0 : 1);
        }
        /////////////////////// Job for Ques6 /////////////////////////////////////////
        // Give the MapRed job a name. You'll see this name in the Yarn
        // webapp.
        Job ques6Job1 = Job.getInstance(conf, "AnalyzeFlightsQ6");
        // Current class.
        ques6Job1.setJarByClass(AnalysisJob.class);
        // Mapper
        ques6Job1.setMapperClass(Q6Job1Mapper.class);
        // Reducer
        ques6Job1.setReducerClass(Q6Job1Reducer.class);
        ques6Job1.setCombinerClass(Q6Job1Combiner.class);
//            ques3Job1.setPartitionerClass(Q1Job1Partitioner.class);
        // Outputs from the Mapper.
        ques6Job1.setMapOutputKeyClass(Text.class);
        ques6Job1.setMapOutputValueClass(Q6Job1CustomDataType.class);
        ques6Job1.setOutputKeyClass(NullWritable.class);
        ques6Job1.setOutputValueClass(Text.class);
        // path to input in HDFS
        FileInputFormat.addInputPath(ques6Job1, new Path(args[0]));
        FileInputFormat.addInputPath(ques6Job1, new Path(args[3]));
//                FileSystem fileSystem = FileSystem.get(conf);
//                fileSystem.mkdirs(new Path(args[4] + "_Question_3_temp"));
        String outputPathQ6 = args[4] + "_Question_6_temp";

        if (fileSystem.exists(new Path(outputPathQ6))) {
            fileSystem.delete(new Path(outputPathQ6), true);
        }
        // path to output in HDFS
        FileOutputFormat.setOutputPath(ques6Job1, new Path(outputPathQ6));
        // Block until the job is completed.
        boolean isCompletedQ6 = ques6Job1.waitForCompletion(true);
//        System.exit(0);
        //Second job
        if (isCompletedQ6) {
            Job ques6Job2 = Job.getInstance(conf, "ProcessDataQ6");
            ques6Job2.setJarByClass(AnalysisJob.class);
            // Mapper
            ques6Job2.setMapperClass(Q6Job2Mapper.class);
            // Reducer
            ques6Job2.setReducerClass(Q6Job2Reducer.class);
            // Outputs from the Mapper.
            ques6Job2.setMapOutputKeyClass(NullWritable.class);
            ques6Job2.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties
            // if the Mapper and Reducer has same key and value types. It is set
            // separately for
            // elaboration.
            ques6Job2.setOutputKeyClass(NullWritable.class);
            ques6Job2.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(ques6Job2, new Path(outputPathQ6));
//            FileSystem fileSystem = FileSystem.get(conf1);

            String outputPath2Q6 = args[4] + "_Question6";

            if (fileSystem.exists(new Path(outputPath2Q6))) {
                fileSystem.delete(new Path(outputPath2Q6), true);
            }
            // path to output in HDFS
            FileOutputFormat.setOutputPath(ques6Job2, new Path(outputPath2Q6));
            ques6Job2.waitForCompletion(true);
//            System.exit(ques6Job2.waitForCompletion(true) ? 0 : 1);
        }
        Job ques7Job1 = Job.getInstance(conf, "AnalyzeFlightsQ7");
//        // Current class.
        ques7Job1.setJarByClass(AnalysisJob.class);
        // Mapper
        ques7Job1.setMapperClass(Q7Job1Mapper.class);
        // Reducer
        ques7Job1.setReducerClass(Q7Job1Reducer.class);
        ques7Job1.setCombinerClass(Q7Job1Combiner.class);
//            ques3Job1.setPartitionerClass(Q1Job1Partitioner.class);
        // Outputs from the Mapper.
        ques7Job1.setMapOutputKeyClass(Text.class);
        ques7Job1.setMapOutputValueClass(Q7Job1CustomDataType.class);
        ques7Job1.setOutputKeyClass(NullWritable.class);
        ques7Job1.setOutputValueClass(Text.class);
        // path to input in HDFS
        FileInputFormat.addInputPath(ques7Job1, new Path(args[0]));
//        FileInputFormat.addInputPath(ques7Job1, new Path(args[3]));
//                FileSystem fileSystem = FileSystem.get(conf);
//                fileSystem.mkdirs(new Path(args[4] + "_Question_3_temp"));
        String outputPathQ7 = args[4] + "_Question_7_temp";

        if (fileSystem.exists(new Path(outputPathQ7))) {
            fileSystem.delete(new Path(outputPathQ7), true);
        }
        // path to output in HDFS
        FileOutputFormat.setOutputPath(ques7Job1, new Path(outputPathQ7));
        // Block until the job is completed.
        boolean isCompletedQ7 = ques7Job1.waitForCompletion(true);
//        System.exit(0);
        //Second job
        if (isCompletedQ7) {
            Job ques7Job2 = Job.getInstance(conf, "ProcessDataQ7");
            ques7Job2.setJarByClass(AnalysisJob.class);
            // Mapper
            ques7Job2.setMapperClass(Q7Job2Mapper.class);
            // Reducer
            ques7Job2.setReducerClass(Q7Job2Reducer.class);
            // Outputs from the Mapper.
            ques7Job2.setMapOutputKeyClass(NullWritable.class);
            ques7Job2.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties
            // if the Mapper and Reducer has same key and value types. It is set
            // separately for
            // elaboration.
            ques7Job2.setOutputKeyClass(NullWritable.class);
            ques7Job2.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(ques7Job2, new Path(outputPathQ7));
//            FileSystem fileSystem = FileSystem.get(conf1);

            String outputPath2Q7 = args[4] + "_Question7";

            if (fileSystem.exists(new Path(outputPath2Q7))) {
                fileSystem.delete(new Path(outputPath2Q7), true);
            }
            // path to output in HDFS
            FileOutputFormat.setOutputPath(ques7Job2, new Path(outputPath2Q7));
//            ques6Job2.waitForCompletion(true);
            System.exit(ques7Job2.waitForCompletion(true) ? 0 : 1);
        }
    }
}
