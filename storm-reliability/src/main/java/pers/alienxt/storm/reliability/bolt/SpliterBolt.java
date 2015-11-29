/**
 * Copyright (c) 20013-2099 https://github.com/alienxt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package pers.alienxt.storm.reliability.bolt;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * <p>File Description：SpliterBolt</p>
 * <p>Content Description：Word split with storm</p>
 * <p>Others： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/9/15 上午10:09
 */
public class SpliterBolt implements IRichBolt {

    private static final long serialVersionUID = -7330275106662326260L;

    private OutputCollector collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word"));
    }

    @Override
    public void execute(Tuple input) {
        String line = input.getString(0);
        // 分词，将一个tuple分割，emit 2个新的tuple
        String[] words = line.split(",");
        for (String word : words) {
            collector.emit(input, new Values(word));
        }
        collector.ack(input);
    }

    @Override
    public void cleanup() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
