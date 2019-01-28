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
    private static final int RECORDWINDOW = 2;
    private static final Histogram histogram = new Histogram(2);
    private static final Histogram histogram2 = new Histogram(2);
    private static long firstTupleTime = -1;
    private static long eventCountTotal = 0;
    private static long eventCount = 0;
    private static long timeSpent = 0;
    private static long totalTimeSpent = 0;
    private static long startTime = -1;
    private String executionType;
    private ExecutorService executorService;
    BufferedWriter bw = null;
    FileWriter fw = null;
    static final String DB_URL = "jdbc:mysql://localhost:3306/test3?autoReconnect=true&useSSL=false";
    static final String USER = "root";
    static final String PASS = "87654321";
    String siddhiAppContextName = "";


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
                calculateThroughput(streamEventChunk);
                break;

            default:
                log.error("executionType should be either throughput or latency or both "
                        + "but found" + " " + executionType);
        }

        nextProcessor.process(streamEventChunk);
    }






    public void filewritecreator(File file2) {
        try {
            if (!file2.exists()) {
                try {
                    file2.createNewFile();

                    fw = new FileWriter(file2.getAbsoluteFile(), true);
                    bw = new BufferedWriter(fw);

                    bw.write("Timestamp, " + "Execution Group, " + ", ParallelInstance" +
                            "Number of Windows Executed" +
                            "Throughput in this window (thousands events/second), " +
                            "Entire throughput for the run (thousands events/second), " +
                            "Total elapsed time(s)," +
                            "Total Events," +
                            "Average latency per event in this window(ms)," +
                            "Entire Average latency per event for the run(ms), " +
                            "AVG latency from start (90), " +
                            "AVG latency from start(95), " +
                            "AVG latency from start (99), " +
                            "AVG latency in this window(90), " +
                            "AVG latency in this window(95), " +
                            "AVG latency in this window(99), " +
                            "Total memory with the Oracle JVM, " +
                            "Free memory with the Oracle JVM " +
                            "InputStream");
                    bw.write("\n");
                    bw.flush();


                } catch (IOException e) { }
            }  else {
                fw = new FileWriter(file2.getAbsoluteFile(), true);
                bw = new BufferedWriter(fw);
            }
        } catch (IOException e) {
        }

    }




    int k = 1;
    /**
     * This method is to calculate throughput.
     *
     * @param streamEventChunk
     */

    private String calculateThroughput(ComplexEventChunk<StreamEvent> streamEventChunk) {
        System.out.println("Inside throughput %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%");

        Connection conn = null;     //Initiating the connection Variable



        synchronized (this) {

            if (firstTupleTime == -1) {
                firstTupleTime = System.currentTimeMillis();
            }
            try {

                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection(DB_URL, USER, PASS);
                Statement st = conn.createStatement();
                System.out.println("Conn--------");
                System.out.println(conn);



                while (streamEventChunk.hasNext()) {
                    if (firstTupleTime == -1) {
                        firstTupleTime = System.currentTimeMillis();
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
                        String currentInstance = siddhiAppContextName.
                                substring((len - 1) , len);


                        String currentExecutionGroup  = siddhiAppContextName.
                                substring((len - 3) , len - 2);

                        File file = new File("/home/winma/Documents/Performance-Files/"
                                + execgroup + "_" + currentInstance + ".csv");
                        filewritecreator(file);




                        //Initiating the next window with new start time
                        if (startTime == -1) {
                            startTime = System.currentTimeMillis();
                            log.info("Start time updated ");
                        }


                        eventCountTotal++;
                        eventCount++;



                        long currentTime = System.currentTimeMillis();

                        long iijTimestamp = (Long) (attributeExpressionExecutors[0].execute(streamEvent));
                        timeSpent += (currentTime - iijTimestamp);



                        if (eventCount >= recordWindow) {
                            log.info("Inside throughput extension of " + outputLog);


                            totalTimeSpent += timeSpent;
                            log.info("Total time added");
                            histogram2.recordValue((timeSpent));
                            histogram.recordValue(totalTimeSpent);
                            long value = currentTime - startTime;
                            long totalPhysicalMemorySize = ((com.sun.management.OperatingSystemMXBean)
                                    ManagementFactory
                                            .getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
                            long freememorySize = ((com.sun.management.OperatingSystemMXBean) ManagementFactory
                                    .getOperatingSystemMXBean()).getTotalPhysicalMemorySize();
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
                                    (eventCountTotal / recordWindow) + "," +
                                    ((eventCount * 1000f) / value) + "," +
                                    (eventCountTotal * 1000f / (currentTime - firstTupleTime)) + "," +
                                    ((currentTime - firstTupleTime) / 1000f) + "," +
                                    eventCountTotal + "," +
                                    ((timeSpent * 1.0) / eventCount) + "," +
                                    ((totalTimeSpent * 1.0) / eventCountTotal) + "," +
                                    histogram.getValueAtPercentile(90.0) + "," +
                                    histogram.getValueAtPercentile(95.0) + "," +
                                    histogram.getValueAtPercentile(99.0) + "," +
                                    histogram2.getValueAtPercentile(90.0) + "," +
                                    histogram2.getValueAtPercentile(95.0) + "," +
                                    histogram2.getValueAtPercentile(99.0) + "," +
                                    totalPhysicalMemorySize + "," +
                                    freememorySize +"," +
                                    cpuUsage +
                                    ")";
                            System.out.println(sql);


                            st.executeUpdate(sql);


                            log.info("Done inserting values to SQL DB ****************");
                            System.out.println("Done inserting values to SQL DB");



                            log.info("Writing files");

                            bw.write(currentTime2 + "," +
                                    execgroup + "," +
                                    currentInstance + "," +
                                    (eventCountTotal / recordWindow) + "," +
                                    ((eventCount * 1000) / value) + "," +
                                    (eventCountTotal * 1000 / (currentTime - firstTupleTime)) + "," +
                                    ((currentTime - firstTupleTime) / 1000f) + "," +
                                    eventCountTotal + "," +
                                    ((timeSpent * 1.0) / eventCount) + "," +
                                    ((totalTimeSpent * 1.0) / eventCountTotal) + "," +
                                    histogram.getValueAtPercentile(90.0) + "," +
                                    histogram.getValueAtPercentile(95.0) + "," +
                                    histogram.getValueAtPercentile(99.0) + "," +
                                    histogram2.getValueAtPercentile(90.0) + "," +
                                    histogram2.getValueAtPercentile(95.0) + "," +
                                    histogram2.getValueAtPercentile(99.0) + "," +
                                    totalPhysicalMemorySize + "," +
                                    freememorySize + "," +
                                    outputLog);
                            bw.write("\n");
                            bw.flush();
                            log.info("File wrtiting completed in throughput" + execgroup +
                                    "_" + currentInstance + ".csv");

                            // executorService.submit(file);
                            startTime = -1;
                            eventCount = 0;
                            histogram2.reset();
                            timeSpent = 0;

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


                conn.close();


            } catch (ClassNotFoundException e) {
            } catch (SQLException e) {
                log.error(e.getMessage());
            }



        }

        return ("");
    }







    @Override
    public void start() {
        //Do nothing
    }

    @Override
    public void stop() {
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
