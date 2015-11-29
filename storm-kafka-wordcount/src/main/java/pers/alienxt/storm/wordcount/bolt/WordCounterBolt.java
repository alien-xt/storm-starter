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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * <p>File Description：WordCounterBolt</p>
 * <p>Content Description：Word count and print</p>
 * <p>Others： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/9/14 下午4:04
 */
public class WordCounterBolt extends BaseRichBolt {

    private static final Log LOG = LogFactory.getLog(WordCounterBolt.class);

    private static final long serialVersionUID = 5443220684083087988L;

    private OutputCollector outputCollector;

    private Map<String, AtomicInteger> counterMap;

    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        this.counterMap = new HashMap<String, AtomicInteger>();
    }

    @Override
    public void execute(Tuple input) {
        final String word = input.getString(0);
        final int count = input.getInteger(1);
        LOG.info("[splitter -> counter] " + word + " : " + count);
        AtomicInteger ai = this.counterMap.get(word);
        if (ai == null) {
            ai = new AtomicInteger();
            this.counterMap.put(word, ai);
        }
        ai.addAndGet(count);
        outputCollector.ack(input);
    }

    @Override
    public void cleanup() {
        // shutdown的时候调用
        LOG.info("========== The final result ==========");
        final Iterator<Map.Entry<String, AtomicInteger>> iter = this.counterMap.entrySet().iterator();
        while (iter.hasNext()) {
            final Map.Entry<String, AtomicInteger> entry = iter.next();
            LOG.info(entry.getKey() + "\t:\t" + entry.getValue().get());
        }
        LOG.info("========== End ==========");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
