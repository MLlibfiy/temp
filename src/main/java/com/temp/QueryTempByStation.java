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

public class QueryTempByStation {
    public static class QueryTempByStationMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split(",");
            String temp = split[8].trim();
            String date = split[1];
            String station = split[0];

            String outKey = station + "," + date;

            if (!"-".equals(temp) & !"Dry Bulb Temp".equals(temp)) {

                //气象站和时间作为key，以温度作为value输出到reduce端
                context.write(new Text(outKey), new LongWritable(Long.parseLong(temp)));
            }


        }
    }

    public static class QueryTempByStationReduce extends Reducer<Text, LongWritable, Text, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {

            //获取输入的气象站编号
            String inStation = context.getConfiguration().get("station");

            String station = key.toString().split(",")[0];

            if (inStation.equals(station)) {
                long maxTemp = 0L;
                long minTemp = 100000000L;

                //计算每个气象站每天的最高文档和最低温度
                for (LongWritable value : values) {
                    long temp = value.get();
                    if (temp > maxTemp) {
                        maxTemp = temp;
                    }
                    if (temp < minTemp) {
                        minTemp = temp;
                    }
                }

                String line = key.toString() + "," + maxTemp + "," + minTemp;

                //保存数据到hdfs
                context.write(new Text(line), NullWritable.get());
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();


        //获取输入的气象站编号
        String station = args[0];
        System.out.println("查询的气象站：" + station);
        //设置全局变量
        configuration.set("station", station);

        Job job = Job.getInstance(configuration);

        job.setJobName("Dry Bulb Temp");
        job.setJarByClass(QueryTempByStation.class);


        job.setMapperClass(QueryTempByStationMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        job.setReducerClass(QueryTempByStationReduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        //输入路径
        FileInputFormat.addInputPath(job, new Path("/user/ylyu001/data/in/"));

        Path outPath = new Path("/user/ylyu001//data/station_temp-" + station);

        //删除输出目录
        FileSystem fileSystem = FileSystem.get(configuration);
        if (fileSystem.exists(outPath)) {
            fileSystem.delete(outPath, true);
        }

        //输出路径
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);

        

    }
}