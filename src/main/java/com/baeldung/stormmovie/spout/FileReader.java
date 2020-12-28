package com.baeldung.stormmovie.spout;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.*;

import java.net.Socket;
import java.util.Map;

public class FileReader extends BaseRichSpout {

    private SpoutOutputCollector collector;
    private BufferedReader reader,in;
    private boolean completed = false;
    private CsvParser parser;
    private Socket echoSocket;
    private PrintWriter out;
    // Called when Storm detects a tuple emitted successfully
    public void ack(Object msgId) {
        System.out.println("SUCCESS: " + msgId);
    }

    // Called when a tuple fails to be emitted
    public void fail(Object msgId) {
        System.out.println("ERROR: " + msgId);
    }

    public void close() {
    }

    // Called when a task for this component is initialized within a worker on the cluster.
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {




        try {
            echoSocket = new Socket("localhost", 31000);
            out = new PrintWriter(echoSocket.getOutputStream(), true);
            in = new BufferedReader(
                    new InputStreamReader(echoSocket.getInputStream()));

        } catch (IOException e) {
            e.printStackTrace();
        }






        /*CsvParserSettings settings = new CsvParserSettings();
        parser = new CsvParser(settings);

        String filePath = "/home/nabil/eclipse-workspace/tutorials/libraries-data/src/main/resources/ratings1m.csv";

        try {
            parser.beginParsing(new InputStreamReader(new FileInputStream(filePath)));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }*/

        this.collector = collector;
    }

    public void nextTuple() {
        /**
         * NextTuple either emits a new tuple into the topology or simply returns if there are no new tuples to emit
         */
        if (completed) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                System.out.println("Error: " + e.getMessage());
            }
            return;
        }

        out.println( 0 + ":persons");
        String line = null;
        try {
            while ((line = in.readLine()) != null) {
                line += "," + System.currentTimeMillis();
                this.collector.emit(new Values(line));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }


     /*   String[] row;
        while ((row = parser.parseNext()) != null) {
            // println(out, Arrays.toString(row));
            String str = String.join(",", row);
            str += "," + System.nanoTime();

            this.collector.emit(new Values(str));
        }*/

   /*     String line;

        try {
            //Read all lines
            while ((line = reader.readLine()) != null) {


                line += "," + System.nanoTime();

                this.collector.emit(new Values(line));
            }
        } catch (Exception e) {
            throw new RuntimeException("Error reading tuple", e);
        }*/

    }

    // The declareOutputFields function declares the output fields ("line") for the component.
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }


}


