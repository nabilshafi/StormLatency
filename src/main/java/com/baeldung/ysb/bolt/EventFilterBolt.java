package com.baeldung.ysb.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

public class EventFilterBolt extends BaseRichBolt {


    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        _collector = collector;
    }


    @Override
    public void execute(Tuple tuple) {
        if(tuple.getStringByField("event_type").equals("view")) {
            _collector.emit(tuple, tuple.getValues());
        }
       // _collector.ack(tuple);
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_id", "page_id", "ad_id", "ad_type", "event_type", "event_time", "ip_address", "time"));
    }


}
