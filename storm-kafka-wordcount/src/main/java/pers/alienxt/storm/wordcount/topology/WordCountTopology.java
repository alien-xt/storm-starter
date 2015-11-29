/**
 * Copyright (c) 20013-2099 https://github.com/alienxt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package pers.alienxt.storm.wordcount.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

import pers.alienxt.storm.wordcount.bolt.WordCounterBolt;
import pers.alienxt.storm.wordcount.bolt.WordSplitterBolt;
import storm.kafka.*;

import java.util.Arrays;

/**
 * <p>File Description：WordCountTopology</p>
 * <p>Content Description：Build topology with storm</p>
 * <p>Others： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/9/14 下午3:54
 */
public class WordCountTopology {

    private static final String TOPOLOGY_NAME = "topology_kafka_wordcount";

    /**
     * 构建kafka的配置
     *
     * @return kafka spout 配置
     */
    private static SpoutConfig createSpoutConfig() {
        final String zks = "localhost:2181";
        final String topic = "t_storm_wordcount";            // 必须事先创建
        final String zkRoot = "/kafka/storm/wordcount";   // zookeeper存储Offset名称必须以/开头
        final String id = "topology_wordcount_spout";         // spout唯一标识
        final BrokerHosts brokerHosts = new ZkHosts(zks);
        final SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.zkServers = Arrays.asList(new String[] {"localhost"});
        spoutConf.zkPort = 2181;
        spoutConf.forceFromStart = false;    // from beginning
        return spoutConf;
    }

    public static void main(final String[] args) throws AlreadyAliveException,
            InvalidTopologyException {
        final TopologyBuilder builder = new TopologyBuilder();
        final SpoutConfig spoutConf = createSpoutConfig();
        // spout
        builder.setSpout("wordcount_kafka_source", new KafkaSpout(spoutConf), 1);
        // 分词
        builder.setBolt("wordcount_splitter", new WordSplitterBolt(), 2)
                .shuffleGrouping("wordcount_kafka_source");  // 混乱随机分发
        // 聚合统计
        builder.setBolt("wordcount_counter", new WordCounterBolt())
                .fieldsGrouping("wordcount_splitter", new Fields("word"));  // 根据field的值分发

        final Config conf = new Config();
        boolean debug = false;
        if (args != null && args.length > 0) {
            // 集群提交
            String nimbus = args[0];
            conf.setDebug(debug);
            conf.put(Config.NIMBUS_HOST, nimbus);
            conf.setNumWorkers(2);
            StormSubmitter.submitTopologyWithProgressBar(TOPOLOGY_NAME, conf, builder.createTopology());
        } else {
            debug = true;
            conf.setDebug(debug);
            // 本地模式
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_NAME, conf,
                    builder.createTopology());
            try {
                // 1分钟后shutdown
                Thread.sleep(60000l);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            cluster.shutdown();
        }
    }

}
