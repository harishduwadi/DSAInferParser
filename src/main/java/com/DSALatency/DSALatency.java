package com.DSALatency;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;

public class DSALatency {

    public static String[] DSARequests = {
            "/?q=",
            "/pdf/download.php",
    };

    public static String[] InferRequest = {
            "/search/dpn_mappings",
            "/search?term"
    };

    public static String[] DSALocations = {
            "10.27.0", // Oregon
            "10.27.1", // Seoul
    };

    public static String[] logFiles = {
            "dsa",
            "infer",
    };

    public static double[] Percentiles = {
            100.0,
            99.0,
            95.0,
            80.0,
            50.0,
    };

    public static int HistMin = 1;
    public static int HistMax = 60000000; // 1 minute in microsecond
    public static int HistSigfig = 1;

    private static int year = 2018;
    private static int month = 8;
    private static int hour = 0;
    private static int minute = 0;
    private static DateTimeZone timeZone = DateTimeZone.UTC;


    public static DateTime inferTime() {
        int inferday = 1;
        return new DateTime(year, month, inferday, hour, minute, timeZone);
    }

    public static DateTime dsaTime(){
        int dsaday = 23;
        return new DateTime(year, month, dsaday, hour, minute, timeZone);
    }

    public static void main(String []args){
        if (args.length != 3) {
            System.out.println("Usage: calculateRate <Job name> <InputFile path> <Output path>!");
            System.exit(-1);
        }

        try {
            JobConf conf = new JobConf();

            conf.setMapperClass(DSAMapper.class);
            conf.setReducerClass(DSAReducer.class);

            conf.setMapOutputKeyClass(Text.class);
            conf.setMapOutputValueClass(LongWritable.class);

            conf.setOutputKeyClass(Text.class);
            conf.setOutputValueClass(LongWritable.class);

            conf.setJarByClass(DSALatency.class);
            conf.setJobName(args[0]);

            FileInputFormat.addInputPath(conf, new Path(args[1]));
            FileOutputFormat.setOutputPath(conf, new Path(args[2]));

            RunningJob job = JobClient.runJob(conf);
            job.waitForCompletion();

        }
        catch (Exception e){
            System.out.println(e.getMessage());
            System.exit(-1);
        }
    }
}
