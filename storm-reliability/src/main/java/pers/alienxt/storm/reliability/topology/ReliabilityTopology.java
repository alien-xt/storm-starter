/**
 * Copyright (c) 20013-2099 https://github.com/alienxt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package pers.alienxt.storm.reliability.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import pers.alienxt.storm.reliability.bolt.SpliterBolt;
import pers.alienxt.storm.reliability.bolt.WriterBolt;
import pers.alienxt.storm.reliability.spout.MessageSpout;

/**
 * <p>File Description：ReliabilityTopology</p>
 * <p>Content Description：Build reliability topology with storm</p>
 * <p>Others： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/9/15 上午10:14
 */
public class ReliabilityTopology {

    private static final String TOPOLOGY_NAME = "topology_reliability";

    public static void main(String[] args) {
        TopologyBuilder builder = new TopologyBuilder();
        // 消息源
        builder.setSpout("spout", new MessageSpout());
        // 分词器
        builder.setBolt("bolt_1", new SpliterBolt()).shuffleGrouping("spout");
        // 持久化bolt
        builder.setBolt("bolt_2", new WriterBolt()).shuffleGrouping("bolt_1");
        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, conf, builder.createTopology());
        try {
            Thread.sleep(6000l);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        cluster.shutdown();
    }
}
