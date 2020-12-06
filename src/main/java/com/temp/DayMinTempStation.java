package com.temp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;

public class DayMinTempStation {
    public static class DayMinTempStationMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            String temp = split[8].trim();
            String date = split[1];
            String station = split[0];

            if (!"-".equals(temp) & !"Dry Bulb Temp".equals(temp)) {
                //气象站和时间作为key，以温度作为value输出到reduce端
                context.write(new Text(date), new Text(station + "," + temp));
            }

        }
    }


    public static class DayMinTempStationReduce extends Reducer<Text, Text, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> stations = new ArrayList<>();

            long minTemp =1000000000L;

            for (Text value : values) {
                String[] split = value.toString().split(",");

                String station = split[0];
                long temp = Long.parseLong(split[1]);

                if (temp <= minTemp) {

                    if (temp == minTemp) {
                        stations.add(station);
                    }

                    if (temp < minTemp) {
                        stations.clear();
                        stations.add(station);
                    }

                    minTemp = temp;
                }

            }

            for (String station : stations) {
                String line = key.toString() + "," + station + "," + minTemp;
                //保存数据到hdfs
                context.write(new Text(line), NullWritable.get());
            }

        }
    }


    public static void main(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJobName("query by station");
        job.setJarByClass(DayMinTempStation.class);

        job.setMapperClass(DayMinTempStationMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(DayMinTempStationReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //输入路径
        FileInputFormat.addInputPath(job, new Path("/user/yiyu001/data/in/"));

        Path outPath = new Path("/user/yiyu001/data/min_temp_station");

        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        //输出路径
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);

    }
}
