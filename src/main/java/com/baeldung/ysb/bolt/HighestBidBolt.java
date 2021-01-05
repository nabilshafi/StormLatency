package com.baeldung.ysb.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.util.*;

public class HighestBidBolt extends BaseWindowedBolt {

    Map<String, Integer> counterMap;
    OutputCollector _collector;

    int i = 0;
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {

        counterMap = new HashMap();

        _collector = collector;
    }

    @Override
    public void execute(TupleWindow tupleWindow) {

        System.out.println(tupleWindow.get().size());
        System.out.println("------------------------------------------------------");

        for(Tuple tuple: tupleWindow.get()) {
            // do the windowing computation


            String key = tuple.getString(1);
            int price = Integer.parseInt(tuple.getString(4));
            //System.out.println(key + ": " + price );
            if(!counterMap.containsKey(key)){
                counterMap.put(key, price);
            }else{
                if(counterMap.get(key) < price){
                    counterMap.put(key, price);
                }

            }
        }
        String key = Collections.max(counterMap.entrySet(), Map.Entry.comparingByValue()).getKey();

        int value = Collections.max(counterMap.values());

        _collector.emit( new Values(key,value));

    }


    /*    @Override
        public void execute(Tuple tuple) {

            System.out.println("-----------------------------[");
            System.out.println(tuple.getValues().size());
            System.out.println(i++);
            if(tuple.getStringByField("event_type").equals("view")) {
                _collector.emit(tuple, tuple.getValues());
            }
           // _collector.ack(tuple);
        }*/
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key","value"));
        // declarer.declare(new Fields("user_id", "page_id", "ad_id", "ad_type", "event_type", "event_time", "ip_address", "time"));
    }

    @Override
    public void cleanup() {

         Set<String> keys = counterMap.keySet();

         for(String k : keys){

             System.out.println(k + ": " + counterMap.get(k));
         }
        String key = Collections.max(counterMap.entrySet(), Map.Entry.comparingByValue()).getKey();

        int value = Collections.max(counterMap.values());

        System.out.println("Key: " + key + ": Value: " + value);

        //writer.close();
    }

}