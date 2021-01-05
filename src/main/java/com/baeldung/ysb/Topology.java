package com.baeldung.ysb;


import com.baeldung.ysb.bolt.CurrencyConversionBolt;
import com.baeldung.ysb.bolt.EventFilterBolt;
import com.baeldung.ysb.bolt.FIleWriteBolt;
import com.baeldung.ysb.bolt.HighestBidBolt;
import com.baeldung.ysb.spout.FileReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
//import org.apache.storm.starter.bolt.SlidingWindowSumBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;


public class Topology {
    public static void main(String[] args) throws InterruptedException, IOException {

        // Topology definition
        String filePath = "./src/main/resources/calculateYsb.csv";
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader", new FileReader());
        //The spout and the bolts are connected using shuffleGroupings. This type of grouping
        //tells Storm to send messages from the source node to target nodes in randomly distributed
        //fashion.
        /*builder.setBolt("slidingsum", new SlidingWindowSumBolt().withWindow(BaseWindowedBolt.Count.of(40), BaseWindowedBolt.Count.of(20)))
                .shuffleGrouping("word-reader");
*/
       /* builder.setBolt("event_filter", new EventFilterBolt().withWindow(BaseWindowedBolt.Duration.of(1000), BaseWindowedBolt.Duration.of(500)))
                .shuffleGrouping("word-reader");*/

  /*      builder.setBolt("event_filter", new HighestBidBolt().withTumblingWindow(BaseWindowedBolt.Duration.of(1000)))
                .shuffleGrouping("word-reader");*/

        builder.setBolt("event_filter", new CurrencyConversionBolt()).shuffleGrouping("word-reader");




        builder.setBolt("fileBolt", new FIleWriteBolt(filePath)).shuffleGrouping("event_filter");


        // Send the same word to the same instance of the word-counter using fieldsGrouping instead of shuffleGrouping


        // Configuration
        Config config = new Config();

        config.setDebug(true);

        long startTime = System.nanoTime();
        // Run topology
        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("my-first-topology", config, builder.createTopology());
        Thread.sleep(50000);
       // System.out.println("Throughput: " + (System.nanoTime() - startTime));
        BufferedWriter wr = new BufferedWriter(new FileWriter("./src/main/resources/calculaterthroughput1m.csv"));
        wr.write((System.nanoTime() - startTime)+"");
        wr.close();
        localCluster.shutdown();
    }

}