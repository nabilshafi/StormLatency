package com.baeldung.ysb.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class CurrencyConversionBolt extends BaseBasicBolt {

    Map<String, Integer> counterMap;
    OutputCollector _collector;
    double usdEur = 1.24;



    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {



            String key = input.getString(1);
            int price = Integer.parseInt(input.getString(4));
            System.out.println(key + ": " + price );


            collector.emit(new Values(key, price * usdEur));

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));
        // declarer.declare(new Fields("user_id", "page_id", "ad_id", "ad_type", "event_type", "event_time", "ip_address", "time"));
    }


}