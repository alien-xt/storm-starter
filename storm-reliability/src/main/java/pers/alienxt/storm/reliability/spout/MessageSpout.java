/**
 * Copyright (c) 20013-2099 https://github.com/alienxt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package pers.alienxt.storm.reliability.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * <p>File Description：MessageSpout</p>
 * <p>Content Description：Message Spout with storm</p>
 * <p>Others： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/9/15 上午10:04
 */
public class MessageSpout implements IRichSpout {

    private static final Logger LOG = LoggerFactory.getLogger(MessageSpout.class);

    private static final long serialVersionUID = -3633713119345486752L;

    private int index = 0;

    // 消息数组
    private String[] lines;

    private SpoutOutputCollector collector;

    public MessageSpout() {
        lines = new String[] {
                "0,zero",
                "1,one",
                "2,two",
                "3,three",
                "4,four",
                "5,five",
                "6,six",
                "7,seven",
                "8,eight",
                "9,nine"
        };
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void nextTuple() {
        // 读取整个消息数组，一个元素作一个tuple
        if (index < lines.length) {
            String l = lines[index];
            // tuple messageId
            collector.emit(new Values(l), index);
            index++;
        }
    }

    @Override
    public void ack(Object msgId) {
        LOG.warn("message sends successfully (msgId = " + msgId + ")");
    }

    @Override
    public void fail(Object msgId) {
        // 重新发送消息
        LOG.error("error : message sends unsuccessfully (msgId = " + msgId + ")");
        LOG.error("resending...");
        collector.emit(new Values(lines[(Integer) msgId]), msgId);
        LOG.error("resend successfully");
    }

    @Override
    public void close() {

    }

    @Override
    public void activate() {

    }

    @Override
    public void deactivate() {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
