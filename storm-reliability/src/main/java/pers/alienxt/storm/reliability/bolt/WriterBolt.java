/**
 * Copyright (c) 20013-2099 https://github.com/alienxt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package pers.alienxt.storm.reliability.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

/**
 * <p>File Description：WriterBolt</p>
 * <p>Content Description：Writer with storm</p>
 * <p>Others： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/9/15 上午10:09
 */
public class WriterBolt implements IRichBolt {

    private static final long serialVersionUID = -5638227697629664774L;

    private FileWriter writer;

    private OutputCollector collector;

    private int count = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        try {
            // 将结果写入文件
            writer = new FileWriter("reliability_out.log");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple input) {
        String word = input.getString(0);
        // 当计数器为5时，fail
        if (count == 5) {
            collector.fail(input);
        } else {
            // 写入文件
            try {
                writer.write(word);
                writer.write("\r\n");
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
            collector.emit(input, new Values(word));
            collector.ack(input);
        }
        count++;
    }

    @Override
    public void cleanup() {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
