package com.baeldung.stormmovie.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;


public class PrintBolt extends BaseBasicBolt {


    // The execute method receives a tuple from one of the bolt's inputs
    // It performs an operation on the input and can also emit a new value.
    // In this example we take the line of text, split it into words and trim the words.
    // Then we emit each word.
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {


        String line = input.getString(0);
        line += "," + System.nanoTime();
        collector.emit(new Values(line));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

}