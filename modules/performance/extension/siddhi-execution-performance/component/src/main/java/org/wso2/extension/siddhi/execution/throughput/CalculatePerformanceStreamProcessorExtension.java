/*
 * Copyright (c) 2017, WSO2 Inc. (http://www.wso2.org) All Rights Reserved.
 *
 * WSO2 Inc. licenses this file to you under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.wso2.extension.siddhi.execution.throughput;


import org.HdrHistogram.Histogram;
import org.apache.log4j.Logger;
import org.wso2.siddhi.annotation.Example;
import org.wso2.siddhi.annotation.Extension;
import org.wso2.siddhi.annotation.Parameter;
import org.wso2.siddhi.annotation.ReturnAttribute;
import org.wso2.siddhi.annotation.util.DataType;
import org.wso2.siddhi.core.config.SiddhiAppContext;
import org.wso2.siddhi.core.event.ComplexEventChunk;
import org.wso2.siddhi.core.event.stream.StreamEvent;
import org.wso2.siddhi.core.event.stream.StreamEventCloner;
import org.wso2.siddhi.core.event.stream.populater.ComplexEventPopulater;
import org.wso2.siddhi.core.executor.ConstantExpressionExecutor;
import org.wso2.siddhi.core.executor.ExpressionExecutor;
import org.wso2.siddhi.core.executor.VariableExpressionExecutor;
import org.wso2.siddhi.core.query.processor.Processor;
import org.wso2.siddhi.core.query.processor.stream.StreamProcessor;
import org.wso2.siddhi.core.util.config.ConfigReader;
import org.wso2.siddhi.query.api.definition.AbstractDefinition;
import org.wso2.siddhi.query.api.definition.Attribute;
import org.wso2.siddhi.query.api.exception.SiddhiAppValidationException;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;




/**
 * Input attributes to log is (iijTimeStamp (Long), value (Float)).
 */




@Extension(
        name = "throughput",
        namespace = "throughput",
        description = "Measuring performance of stream processor with simple passthrough",
        parameters = {
                @Parameter(name = "iijtimestamp",
                        description = "This value used to find the sending timestamp from client",
                        type = {DataType.LONG}),
        },

        returnAttributes = @ReturnAttribute(
                name = "return",
                description = "Returns the list of filtered email addresses as a comma separated list of email " +
                        "addresses.",
                type = {DataType.STRING}),
        examples = {
                @Example(
                        syntax = "@App:name(\"TCP_Benchmark\")\n" +
                                "@source(type = 'tcp', context='inputStream',@map(type='binary'))\n" +
                                "define stream outputStream (iijtimestamp long,value float);\n" +
                                "from inputStream\n" +
                                "select iijtimestamp,value\n" +
                                "insert into tempStream;" +
                                "from tempStream#throughput:throughput(iijtimestamp,value)\n" +
                                "select \"aa\" as tempTrrb\n" +
                                "insert into tempStream1;",
                        description = "This is a simple passthrough query that inserts iijtimestamp (long) and random "
                                + "number(float) into the temp stream  "
                ),
                @Example(
                        syntax = "@App:name(\"TCP_Benchmark\")\n"
                                + "@source(type = 'tcp', context='inputStream',@map(type='binary'))\n"
                                + "define stream inputStream (iijtimestamp long,value float);\n"
                                + "define stream outputStream (iijtimestamp long,value float,mode String);\n"
                                + "from inputStream[value<=0.25]\n"
                                + "select iijtimestamp,value\n"
                                + "insert into tempStream;\n"
                                + "from tempStream#throughput:throughput(iijtimestamp,value,\"both\")\n"
                                + "select \"aa\" as tempTrrb\n"
                                + "insert into tempStream1;",
                        description = "This is a filter query"
                )
        }
)

public class CalculatePerformanceStreamProcessorExtension extends StreamProcessor {
    private static final Logger log = Logger.getLogger(CalculatePerformanceStreamProcessorExtension.class);

    private static  ConcurrentHashMap<String, Histogram> histogramMap;
    private static  ConcurrentHashMap<String, Histogram> histogramMap2;

    private static  ConcurrentHashMap<String,Long> firstTupleTimeMap;

    private static  ConcurrentHashMap<String, Long> eventCountMap;
    private static  ConcurrentHashMap<String, Long> eventCountTotalMap;

    private static  ConcurrentHashMap<String, Long> timeSpentMap;
    private static  ConcurrentHashMap<String, Long> totalTimeSpentMap;

    private static ConcurrentHashMap<String, Long> startTimeMap;

     // throughput related parameters

    private static  ConcurrentHashMap<String,Long> firstTupleTimeMapDataRate;

    private static  ConcurrentHashMap<String, Long> eventCountMapDataRate;
    private static  ConcurrentHashMap<String, Long> eventCountTotalMapDataRate;

    private static  ConcurrentHashMap<String, Long> timeSpentMapDataRate;
    private static  ConcurrentHashMap<String, Long> totalTimeSpentMapDataRate;

    private static ConcurrentHashMap<String, Long> startTimeMapDataRate;



    private String executionType;
    private ExecutorService executorService;
    BufferedWriter bw = null;
    FileWriter fw = null;
    static final String DB_URL = "jdbc:mysql://localhost:3306/test3?autoReconnect=true&useSSL=false";
    static final String USER = "root";
    static final String PASS = "87654321";
    String siddhiAppContextName = "";
    Connection conn;


    /**
     * The init method of the StreamFunction.
     *
     * @param inputDefinition              the incoming stream definition
     * @param attributeExpressionExecutors the executors for the function parameters
     * @param siddhiAppContext             siddhi app context
     * @param configReader                 this hold the {@link} configuration reader.
     * @return the additional output attributes introduced by the function
     */
    @Override
    protected List<Attribute> init(AbstractDefinition inputDefinition, ExpressionExecutor[]
            attributeExpressionExecutors, ConfigReader configReader, SiddhiAppContext siddhiAppContext) {
        executorService = siddhiAppContext.getExecutorService();

        siddhiAppContextName = siddhiAppContext.getName();

        if(histogramMap == null){
            histogramMap = new ConcurrentHashMap<String, Histogram>();
        }

        if(histogramMap2==null){
             histogramMap2 = new ConcurrentHashMap<String, Histogram>();
        }

        if(eventCountTotalMap == null){
             eventCountTotalMap = new ConcurrentHashMap<String, Long>();
        }

        if(eventCountMap==null){
            eventCountMap = new ConcurrentHashMap<String, Long>();
        }

        if(timeSpentMap == null){
            timeSpentMap= new ConcurrentHashMap<String, Long>();
        }

        if(totalTimeSpentMap == null){
            totalTimeSpentMap= new ConcurrentHashMap<String, Long>();
        }

        if(firstTupleTimeMap == null){
            firstTupleTimeMap = new ConcurrentHashMap<String,Long>();
        }

        if(startTimeMap == null){
            startTimeMap  = new ConcurrentHashMap<String, Long>();
        }

// data rate related
        if(eventCountTotalMapDataRate == null){
            eventCountTotalMapDataRate = new ConcurrentHashMap<String, Long>();
        }

        if(eventCountMapDataRate==null){
            eventCountMapDataRate = new ConcurrentHashMap<String, Long>();
        }

        if(timeSpentMapDataRate == null){
            timeSpentMapDataRate = new ConcurrentHashMap<String, Long>();
        }

        if(totalTimeSpentMapDataRate == null){
            totalTimeSpentMapDataRate = new ConcurrentHashMap<String, Long>();
        }

        if(firstTupleTimeMapDataRate == null){
            firstTupleTimeMapDataRate = new ConcurrentHashMap<String,Long>();
        }

        if(startTimeMapDataRate == null){
            startTimeMapDataRate  = new ConcurrentHashMap<String, Long>();
        }

        // data rate related
        if (!histogramMap.containsKey(siddhiAppContextName)) {
            histogramMap.put(siddhiAppContextName, new Histogram(2));
        }

        if (!histogramMap2.containsKey(siddhiAppContextName)) {
            histogramMap2.put(siddhiAppContextName, new Histogram(2));
        }

        if (!eventCountTotalMap.containsKey(siddhiAppContextName)) {
            eventCountTotalMap.put(siddhiAppContextName, 0L);
            System.out.println("Total Event Count Initialized For " + siddhiAppContextName);
        }

        if (!eventCountMap.containsKey(siddhiAppContextName)) {
            eventCountMap.put(siddhiAppContextName, 0L);
        }

        if (!timeSpentMap.containsKey(siddhiAppContextName)) {
            timeSpentMap.put(siddhiAppContextName, 0L);
        }

        if (!totalTimeSpentMap.containsKey(siddhiAppContextName)) {
            totalTimeSpentMap.put(siddhiAppContextName, 0L);
        }

        if (!startTimeMap.containsKey(siddhiAppContextName)) {
            startTimeMap.put(siddhiAppContextName, -1L);
        }

        if (!firstTupleTimeMap.containsKey(siddhiAppContextName)) {
            firstTupleTimeMap.put(siddhiAppContextName, -1L);
        }

        // data rate related

        if (!eventCountTotalMapDataRate.containsKey(siddhiAppContextName)) {
            eventCountTotalMapDataRate.put(siddhiAppContextName, 0L);
            System.out.println("Total Event Count Initialized For " + siddhiAppContextName);
        }

        if (!eventCountMapDataRate.containsKey(siddhiAppContextName)) {
            eventCountMapDataRate.put(siddhiAppContextName, 0L);
        }

        if (!timeSpentMapDataRate.containsKey(siddhiAppContextName)) {
            timeSpentMapDataRate.put(siddhiAppContextName, 0L);
        }

        if (!totalTimeSpentMapDataRate.containsKey(siddhiAppContextName)) {
            totalTimeSpentMapDataRate.put(siddhiAppContextName, 0L);
        }

        if (!startTimeMapDataRate.containsKey(siddhiAppContextName)) {
            startTimeMapDataRate.put(siddhiAppContextName, -1L);
        }

        if (!firstTupleTimeMapDataRate.containsKey(siddhiAppContextName)) {
            firstTupleTimeMapDataRate.put(siddhiAppContextName, -1L);
        }

        try {

            if (conn == null) {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection(DB_URL, USER, PASS);
            }
        } catch(Exception e) {
            //handle
        }

        log.info("init-@@@@@@");

        if (attributeExpressionLength == 6) {
            if (!(attributeExpressionExecutors[0] instanceof VariableExpressionExecutor)) {
                throw new SiddhiAppValidationException("iijTimeStamp has to be a variable but found " +
                        this.attributeExpressionExecutors[0].getClass()
                                .getCanonicalName());
            }

            if (attributeExpressionExecutors[0].getReturnType() == Attribute.Type.LONG) {

            } else {
                throw new SiddhiAppValidationException("iijTimestamp is expected to be long but "
                        + "found" + attributeExpressionExecutors[0]
                        .getReturnType());

            }

            if (!(attributeExpressionExecutors[1] instanceof ConstantExpressionExecutor)) {
                throw new SiddhiAppValidationException("second parameter has to be constant but found" + this
                        .attributeExpressionExecutors[1].getClass().getCanonicalName());
            }

            if (attributeExpressionExecutors[1].getReturnType() == Attribute.Type.STRING) {
                executionType = (String) ((ConstantExpressionExecutor) attributeExpressionExecutors[1]).getValue();

            } else {
                throw new SiddhiAppValidationException("Second parameter expected to be String but "
                        + "found" + attributeExpressionExecutors[1]
                        .getReturnType());
            }

        } else {
            throw new SiddhiAppValidationException("Input parameters for Log can be iijTimeStamp (Long), " +
                    "type (String), but there are " +
                    attributeExpressionExecutors
                            .length + " in the input!");
        }
        //createFile();

        return new ArrayList<Attribute>();
    }

    @Override
    protected void process(ComplexEventChunk<StreamEvent> streamEventChunk, Processor nextProcessor,
                           StreamEventCloner streamEventCloner, ComplexEventPopulater complexEventPopulater) {

        switch (executionType.toLowerCase()) {

            case "throughput":
                calculateThroughput(streamEventChunk, conn);
                break;
            case "datarate" :
                calculateDataRate(streamEventChunk, conn);
            default:
                log.error("executionType should be either throughput or latency or both "
                        + "but found" + " " + executionType);
        }

        nextProcessor.process(streamEventChunk);
    }



    int k = 1;
    /**
     * This method is to calculate throughput.
     *
     * @param streamEventChunk
     */

    private String calculateThroughput(ComplexEventChunk<StreamEvent> streamEventChunk, Connection conn) {
        System.out.println("Inside throughput %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");



        synchronized (this) {
//            Connection conn = null;

            if (firstTupleTimeMap != null && firstTupleTimeMap.get(siddhiAppContextName) == -1) {
                firstTupleTimeMap.put(siddhiAppContextName,System.currentTimeMillis());
            }
            try {

//                Class.forName("com.mysql.jdbc.Driver");
//                conn = DriverManager.getConnection(DB_URL, USER, PASS);
                Statement st = conn.createStatement();
                System.out.println("Conn--------");
                System.out.println(conn);



                while (streamEventChunk.hasNext()) {
                    if(firstTupleTimeMap != null && startTimeMap!= null && timeSpentMap != null && totalTimeSpentMap != null && eventCountMap != null && eventCountTotalMap != null
                            && histogramMap2 != null && histogramMap!= null) {

                    if (firstTupleTimeMap.get(siddhiAppContextName) == -1) {
                        firstTupleTimeMap.put(siddhiAppContextName, System.currentTimeMillis());
                    }
                    int temp = 0;
                    try {
                        log.info("The current app name is " + siddhiAppContextName);


                        StreamEvent streamEvent = streamEventChunk.next();
                        int execgroup = (Integer) (attributeExpressionExecutors[2].execute(streamEvent));
                        int parallel = (Integer) (attributeExpressionExecutors[3].execute(streamEvent));
                        String outputLog = (String) (attributeExpressionExecutors[4].execute(streamEvent));
                        int recordWindow = (Integer) (attributeExpressionExecutors[5].execute(streamEvent));

                        //Getting the currentInstance and Execution Group like below is only valid when the numbers are of 1 digit.
                        int len = siddhiAppContextName.length();
//                        String currentInstance = siddhiAppContextName.
//                                substring((len - 1) , len);
//
//
//
//                        String currentExecutionGroup  = siddhiAppContextName.
//                                substring((len - 3) , len - 2);


                        String[] SplitArray = siddhiAppContextName.split("-");

                        String currentExecutionGroup = SplitArray[SplitArray.length-2].substring(5);

                        //executionGroup = Integer.valueOf(appHolder.getAppName().substring(appHolder.getAppName().length()-3,
                        //appHolder.getAppName().length()-2));

                        String currentInstance = SplitArray[SplitArray.length-1];


                        //Initiating the next window with new start time
                        if (startTimeMap.get(siddhiAppContextName) == -1) {
                            startTimeMap.put(siddhiAppContextName,System.currentTimeMillis());
                            log.info("Start time updated ");
                        }


                        eventCountTotalMap.put(siddhiAppContextName, eventCountTotalMap.get(siddhiAppContextName)+1);
                        eventCountMap.put(siddhiAppContextName,eventCountMap.get(siddhiAppContextName)+1);


                        long currentTime = System.currentTimeMillis();

                        long iijTimestamp = (Long) (attributeExpressionExecutors[0].execute(streamEvent));
                        //timeSpent += (currentTime - iijTimestamp);
                        timeSpentMap.put(siddhiAppContextName,timeSpentMap.get(siddhiAppContextName)+(currentTime - iijTimestamp));


                        if (eventCountMap.get(siddhiAppContextName) >= recordWindow) {
                            log.info("Inside throughput extension of " + outputLog);


                            //totalTimeSpent += timeSpent;
                            totalTimeSpentMap.put(siddhiAppContextName, timeSpentMap.get(siddhiAppContextName)+1);
                            log.info("Total time added");
                            //histogram2.recordValue((timeSpent));
                            histogramMap2.get(siddhiAppContextName).recordValue((timeSpentMap.get(siddhiAppContextName)));

                            //histogram.recordValue(totalTimeSpent);
                            histogramMap.get(siddhiAppContextName).recordValue(timeSpentMap.get(siddhiAppContextName));
                            long value = currentTime - startTimeMap.get(siddhiAppContextName);
                            System.out.println("value "+currentTime+" "+startTimeMap.get(siddhiAppContextName));
                            long totalPhysicalMemorySize = ((com.sun.management.OperatingSystemMXBean)
                                    ManagementFactory
                                            .getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
                            long freememorySize = ((com.sun.management.OperatingSystemMXBean) ManagementFactory
                                    .getOperatingSystemMXBean()).getFreePhysicalMemorySize();
                            double cpuUsage = ((com.sun.management.OperatingSystemMXBean) ManagementFactory
                                    .getOperatingSystemMXBean()).getProcessCpuLoad();


                            String s = Long.toString(currentTime);
                            //s = s.substring(0, 10);
                            long currentTime2 = Long.valueOf(s);
                            System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2");
                            log.info("Inserting in to database");
                            System.out.println("current Time 2");
                            System.out.println(currentTime2);
                            System.out.println("------------------");
                            System.out.println("freememory-size");
                            System.out.println(freememorySize);
                            String sql = "INSERT INTO metricstable (iijtimestamp,exec,parallel," +
                                    "m1,m2,m3,m4,m5,m6,m7," +
                                    "m8,m9,m10," +
                                    "m11,m12,m13,m14,m15,m16" +
                                    ")" + "VALUES (" +
                                    currentTime2 + "," +
                                    execgroup + "," +
                                    "'"+currentInstance+"'" + "," +
                                    (eventCountTotalMap.get(siddhiAppContextName) / recordWindow) + "," +
                                    ((eventCountMap.get(siddhiAppContextName) * 1000f) / value) + "," +
                                    (eventCountTotalMap.get(siddhiAppContextName) * 1000f / (currentTime - firstTupleTimeMap.get(siddhiAppContextName))) + "," +
                                    ((currentTime - firstTupleTimeMap.get(siddhiAppContextName)) / 1000f) + "," +
                                    eventCountTotalMap.get(siddhiAppContextName) + "," +
                                    ((timeSpentMap.get(siddhiAppContextName) * 1.0) / eventCountMap.get(siddhiAppContextName)) + "," +
                                    ((totalTimeSpentMap.get(siddhiAppContextName) * 1.0) / eventCountTotalMap.get(siddhiAppContextName)) + "," +
                                    histogramMap.get(siddhiAppContextName).getValueAtPercentile(90.0) + "," +
                                    histogramMap.get(siddhiAppContextName).getValueAtPercentile(95.0) + "," +
                                    histogramMap.get(siddhiAppContextName).getValueAtPercentile(99.0) + "," +
                                    histogramMap2.get(siddhiAppContextName).getValueAtPercentile(90.0) + "," +
                                    histogramMap2.get(siddhiAppContextName).getValueAtPercentile(95.0) + "," +
                                    histogramMap2.get(siddhiAppContextName).getValueAtPercentile(99.0) + "," +
                                    totalPhysicalMemorySize + "," +
                                    freememorySize +"," +
                                    cpuUsage +
                                    ")";
                            System.out.println(sql);


                            st.executeUpdate(sql);


                            log.info("Done inserting values to SQL DB ****************");
                            System.out.println("Done inserting values to SQL DB");

                            // executorService.submit(file);
                            startTimeMap.put(siddhiAppContextName, -1L);
                            eventCountMap.put(siddhiAppContextName, 0L);
                            histogramMap2.get(siddhiAppContextName).reset();
                            timeSpentMap.put(siddhiAppContextName, 0L);

                            log.info("Exiting the throughput extension ");

                            return ("");
                        }
                    } catch (Exception ex) {
                        log.error("Error while consuming event. " + ex.getStackTrace(), ex.getCause());
                        System.out.println(ex.getMessage());
                        System.out.println(ex.getCause());
                        System.out.println(ex);
                        ex.printStackTrace();
                        System.out.println("mysql error");

                    }
                }
                }






            } catch (Exception e) {
                log.error(e.getMessage());
            }
      //  try {
           // conn.close();} catch (SQLException e){log.error(e.getMessage());}

        }

        return ("");
    }


    private String calculateDataRate (ComplexEventChunk<StreamEvent> streamEventChunk, Connection conn) {
        System.out.println("Inside DataRate %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");
        System.out.println("siddhi app "+siddhiAppContextName);
        System.out.println("tuple "+ firstTupleTimeMapDataRate.get(siddhiAppContextName));



        synchronized (this) {
           // Connection conn = null;


            if (firstTupleTimeMapDataRate != null && firstTupleTimeMapDataRate.get(siddhiAppContextName) == -1) {
                firstTupleTimeMapDataRate.put(siddhiAppContextName,System.currentTimeMillis());
            }
            try {

//                Class.forName("com.mysql.jdbc.Driver");
//                conn = DriverManager.getConnection(DB_URL, USER, PASS);
                Statement st = conn.createStatement();
                System.out.println("Conn--------");
                System.out.println(conn);



                while (streamEventChunk.hasNext()) {
                    if(firstTupleTimeMapDataRate != null && startTimeMapDataRate!= null && timeSpentMapDataRate != null && totalTimeSpentMapDataRate != null &&
                            eventCountMapDataRate != null && eventCountTotalMapDataRate != null) {

                        if (firstTupleTimeMapDataRate.get(siddhiAppContextName) == -1) {
                            firstTupleTimeMapDataRate.put(siddhiAppContextName, System.currentTimeMillis());
                        }
                        int temp = 0;
                        try {
                            log.info("The current app name is " + siddhiAppContextName);


                            StreamEvent streamEvent = streamEventChunk.next();
                            int execgroup = (Integer) (attributeExpressionExecutors[2].execute(streamEvent));
                            int parallel = (Integer) (attributeExpressionExecutors[3].execute(streamEvent));
                            String outputLog = (String) (attributeExpressionExecutors[4].execute(streamEvent));
                            int recordWindow = (Integer) (attributeExpressionExecutors[5].execute(streamEvent));

                            //Getting the currentInstance and Execution Group like below is only valid when the numbers are of 1 digit.
                            int len = siddhiAppContextName.length();
//
                            String[] SplitArray = siddhiAppContextName.split("-");

                            String currentExecutionGroup = SplitArray[SplitArray.length-2].substring(5);

                            String currentInstance = SplitArray[SplitArray.length-1];

                            if (startTimeMapDataRate.get(siddhiAppContextName) == -1) {
                                startTimeMapDataRate.put(siddhiAppContextName,System.currentTimeMillis());
                                log.info("Start time updated ");
                            }


                            eventCountTotalMapDataRate.put(siddhiAppContextName, eventCountTotalMap.get(siddhiAppContextName)+1);
                            eventCountMapDataRate.put(siddhiAppContextName,eventCountMap.get(siddhiAppContextName)+1);


                            long currentTime = System.currentTimeMillis();

                            long iijTimestamp = (Long) (attributeExpressionExecutors[0].execute(streamEvent));
                            //timeSpent += (currentTime - iijTimestamp);
                            timeSpentMapDataRate.put(siddhiAppContextName,timeSpentMapDataRate.get(siddhiAppContextName)+(currentTime - iijTimestamp));


                            if (eventCountMapDataRate.get(siddhiAppContextName) >= recordWindow) {
                                log.info("Inside throughput extension of " + outputLog);


                                //totalTimeSpent += timeSpent;
                                totalTimeSpentMapDataRate.put(siddhiAppContextName, timeSpentMapDataRate.get(siddhiAppContextName)+1);
                                log.info("Total time added");

                                long value = currentTime - startTimeMapDataRate.get(siddhiAppContextName);
                                System.out.println("value "+currentTime+" "+startTimeMapDataRate.get(siddhiAppContextName));


                                String s = Long.toString(currentTime);
                                //s = s.substring(0, 10);
                                long currentTime2 = Long.valueOf(s);
                                System.out.println("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@2");
                                log.info("Inserting in to database");
                                System.out.println("current Time 2");
                                System.out.println(currentTime2);
                                System.out.println("------------------");

                                String sql = "INSERT INTO dataratetable (iijtimestamp,exec,parallel," +
                                        "m1,m2,m3" +
                                        ")" + "VALUES (" +
                                        currentTime2 + "," +
                                        execgroup + "," +
                                        "'"+currentInstance+"'" + "," +
                                        (eventCountTotalMapDataRate.get(siddhiAppContextName) / recordWindow) + "," +
                                        ((eventCountMapDataRate.get(siddhiAppContextName) * 1000f) / value) + "," +
                                        (eventCountTotalMapDataRate.get(siddhiAppContextName) * 1000f / (currentTime - firstTupleTimeMapDataRate.get(siddhiAppContextName))) + ")";
                                System.out.println(sql);


                                st.executeUpdate(sql);


                                log.info("Done inserting values to SQL DB ****************");


                                // executorService.submit(file);
                                startTimeMapDataRate.put(siddhiAppContextName, -1L);
                                eventCountMapDataRate.put(siddhiAppContextName, 0L);
                                timeSpentMapDataRate.put(siddhiAppContextName, 0L);

                                log.info("Exiting the throughput extension ");

                                return ("");
                            }
                        } catch (Exception ex) {
                            log.error("Error while consuming event. " + ex.getStackTrace(), ex.getCause());
                            System.out.println(ex.getMessage());
                            System.out.println(ex.getCause());
                            System.out.println(ex);
                            ex.printStackTrace();
                            System.out.println("mysql error");

                        }
                    }
                }






            } catch (Exception e) {
                log.error(e.getMessage());
            }
          //  try {
            //    conn.close();} catch (SQLException e){log.error(e.getMessage());}

        }

        return ("");
    }




    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
        histogramMap = null;
        histogramMap2 = null;
        eventCountMap = null;
        eventCountTotalMap = null;
        timeSpentMap = null;
        totalTimeSpentMap = null;
        firstTupleTimeMap = null;
        startTimeMap = null;


        //Do nothing
    }

    @Override
    public Map<String, Object> currentState() {
        //No state
        return null;
    }

    @Override
    public void restoreState(Map<String, Object> state) {
        //Nothing to be done
    }


}
