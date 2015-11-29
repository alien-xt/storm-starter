/**
 * Copyright (c) 20013-2099 https://github.com/alienxt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package pers.alienxt.storm.wordcount.bolt;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Map;

/**
 * <p>File Description：WordSplitterBolt</p>
 * <p>Content Description：Split word from spout</p>
 * <p>Others： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/9/14 下午4:00
 */
public class WordSplitterBolt extends BaseRichBolt {

    private static final Log LOG = LogFactory.getLog(WordSplitterBolt.class);

    private static final long serialVersionUID = -1195826410252006751L;

    private OutputCollector outputCollector;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    @Override
    public void execute(Tuple input) {
        final String line = input.getString(0);
        LOG.info("[kafka -> line] " + line);
        final String[] words = line.split("\\s+");
        for (String word : words) {
            LOG.info("[splitter -> counter] " + word);
            outputCollector.emit(input, new Values(word, 1));
        }
        outputCollector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
