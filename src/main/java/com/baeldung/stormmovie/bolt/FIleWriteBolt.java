package com.baeldung.stormmovie.bolt;


import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Map;

public class FIleWriteBolt extends BaseRichBolt {
    public static Logger logger = LoggerFactory.getLogger(FIleWriteBolt.class);
    private BufferedWriter writer;
    private String filePath;
    private ObjectMapper objectMapper;
    Writer outputWriter;
    CsvWriter csvWriter;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        objectMapper = new ObjectMapper();
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);
        try {
           // writer = new BufferedWriter(new FileWriter(filePath));

            OutputStream outputStream = new FileOutputStream(filePath);
            outputWriter = new OutputStreamWriter(outputStream);
            csvWriter = new CsvWriter(outputWriter, new CsvWriterSettings());

        } catch (IOException e) {
            logger.error("Failed to open a file for writing.", e);
        }
    }

    @Override
    public void execute(Tuple tuple) {

        String line = tuple.getString(0);
       // System.out.println(line);
        csvWriter.writeRow(line);
              /*  writer.write(line);
                writer.write("\n");*/
        //writer.flush();
    }


    public FIleWriteBolt(String filePath) {
        this.filePath = filePath;
    }

    @Override
    public void cleanup() {
        //writer.close();
        csvWriter.close();
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
