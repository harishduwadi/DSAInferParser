package com.DSALatency;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.base.Joiner;
import com.supplyframe.frameparser.model.Duration;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import com.supplyframe.frameparser.AccessLogParserFactory;
import com.supplyframe.frameparser.parser.Parser;
import com.supplyframe.frameparser.ParseException;
import com.supplyframe.frameparser.model.AccessLog;
import org.joda.time.DateTime;

public class DSAMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable> {

    private AccessLogParserFactory factory;

    private Joiner joiner;

    private String LocationExp = "^.*,([\\d{?}\\.{?}]*).*$";
    private String TopicExp = ".*\\s(.*).log$";

    private Pattern LocationPattern;
    private Pattern TopicPattern;
    private Matcher matcher;

    @Override
    public void configure(JobConf conf){
        try{
            factory = AccessLogParserFactory.withDefaultConfig();
            joiner = Joiner.on("\t").skipNulls();
            LocationPattern = Pattern.compile(LocationExp);
            TopicPattern = Pattern.compile(TopicExp);
        }
        catch(Exception e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, LongWritable> out, Reporter reporter)
        throws IOException {

        String line = value.toString();
        if (line.contains("dsa.log")){
            reporter.incrCounter(Audit.DSA_FOUND, 1);
        }
        if (line.contains("infer.log")){
            reporter.incrCounter(Audit.INFER_FOUND, 1);
        }

        Parser<AccessLog> parser = factory.autoDetectByTopic(line);


        if (parser == null){
            reporter.incrCounter(Audit.NO_PARSER, 1);
            return;
        }
        reporter.incrCounter(Audit.PARSER_FOUND, 1);

        AccessLog log;
        try {
            log = parser.parse(line);
        }
        catch(ParseException e){
            reporter.incrCounter(Audit.PARSE_EXCEPTION, 1);
            return;
        }
        reporter.incrCounter(Audit.PARSE_SUCCESS, 1);

        String path = log.getUri();
        if (path == null){
            reporter.incrCounter(Audit.NO_PATH, 1);
            return;
        }
        reporter.incrCounter(Audit.PATH_FOUND, 1);

        String logTopic = getTopic(line, reporter);

        Duration response = log.getResponseTime();
        if (response == null){
            reporter.incrCounter(Audit.NO_RESPONSE_TIME, 1);
            return;
        }
        reporter.incrCounter(Audit.RESPONSE_TIME_FOUND, 1);

        DateTime time = log.getTime();
        if (time == null) {
            reporter.incrCounter(Audit.NO_TIME, 1);
            return;
        }
        reporter.incrCounter(Audit.TIME_FOUND, 1);

        String location = parseLocation(line, reporter);

        if(StringUtils.equalsIgnoreCase(logTopic, DSALatency.logFiles[0])){
            reporter.incrCounter(Audit.DSA_STARTS_WITH, 1);
            dsaMap(logTopic, path, response, time, out, location);
        }
        else if(StringUtils.equalsIgnoreCase(logTopic, DSALatency.logFiles[1])) {
            reporter.incrCounter(Audit.INFER_STARTS_WITH, 1);
            inferMap(logTopic, path, response, time, out);
        }

    }

    private String getTopic(String line, Reporter reporter){
        matcher = TopicPattern.matcher(line);
        if (matcher.find()){
            reporter.incrCounter(Audit.TOPIC_FOUND, 1);
            return matcher.group(1);
        }
        reporter.incrCounter(Audit.NO_TOPIC, 1);
        return "";
    }

    private String parseLocation(String line, Reporter reporter){
        matcher = LocationPattern.matcher(line);
        if (matcher.find()){
            reporter.incrCounter(Audit.LOCATION_FOUND, 1);
            return matcher.group(1);
        }
        reporter.incrCounter(Audit.NO_LOCATION, 1);
        return "";
    }

    private void dsaMap(String logTopic, String path, Duration response, DateTime time, OutputCollector<Text, LongWritable> out, String location)
            throws IOException{

        DateTime thresholdTime = DSALatency.dsaTime();

        if (time.isBefore(thresholdTime.toInstant())){
            return;
        }

        for (String request : DSALatency.DSARequests){

            if (!StringUtils.startsWithIgnoreCase(path, request)) {
                continue;
            }

            for (String dsalocation : DSALatency.DSALocations){
                if (!StringUtils.startsWith(location, dsalocation)){
                    continue;
                }

                long resp = response.getMicros();

                time = time.minusMillis(time.getMillisOfSecond());
                time = time.minusSeconds(time.getSecondOfMinute());
                time = time.minusMinutes(time.getMinuteOfHour() % 5);

                String mapKey = joiner.join(logTopic, time, request, dsalocation);

                out.collect(new Text(mapKey), new LongWritable(resp));

            }

        }

    }

    private void inferMap(String logTopic, String path, Duration response, DateTime time, OutputCollector<Text, LongWritable> out)
        throws IOException{

        DateTime thresholdTime = DSALatency.inferTime();

        if (time.isBefore(thresholdTime.toInstant())){
            return;
        }

        for (String request : DSALatency.InferRequest){
            if (!StringUtils.startsWithIgnoreCase(path, request)){
                continue;
            }

            long resp = response.getMicros();

            time = time.minusMillis(time.getMillisOfSecond());
            time = time.minusSeconds(time.getSecondOfMinute());

            String mapKey = joiner.join(logTopic, time, request);

            out.collect(new Text(mapKey), new LongWritable(resp));
        }
    }
}
