/**
 * Copyright (c) 20013-2099 https://github.com/alienxt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 */
package pers.alienxt.storm.transactional.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * <p>File Description：WordCountTopology</p>
 * <p>Content Description：Build topology with storm</p>
 * <p>Others： </p>
 *
 * @version 1.0
 * @author <a> href="mailto:alien.xt.xm@gmail.com">alien.guo</a>
 * @since 15/9/14 下午3:54
 * @see <a href="http://storm.apache.org/documentation/Transactional-topologies.html">Transactional topologies</a>
 */
public class TransactionalGlobalCountTopology {

    private static final Log LOG = LogFactory.getLog(TransactionalGlobalCountTopology.class);

    // 最大的bath个数
    public static final int PARTITION_TAKE_PER_BATCH = 3;
    // 内存消息队列
    public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {{
        put(0, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("chicken"));
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("apple"));
        }});
        put(1, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("apple"));
            add(new Values("banana"));
        }});
        put(2, new ArrayList<List<Object>>() {{
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("cat"));
            add(new Values("dog"));
            add(new Values("dog"));
            add(new Values("dog"));
            add(new Values("dog"));
        }});
    }};

    public static class Value {
        int count = 0;
        BigInteger txid;
    }

    // 模拟的数据库
    public static Map<String, Value> DATABASE = new HashMap<String, Value>();
    // key
    public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";

    /**
     * Transaction process bolt
     */
    public static class BatchCount extends BaseBatchBolt {
        Object _id;
        BatchOutputCollector _collector;

        int _count = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
            _collector = collector;
            _id = id;
        }

        @Override
        public void execute(Tuple tuple) {
            _count++;
        }

        /**
         * 当batch结束的时候调用
         */
        @Override
        public void finishBatch() {
            _collector.emit(new Values(_id, _count));
            LOG.info("[finishBatch_BatchCount] " + "id: " + _id.toString() + " count: " + _count);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "count"));
        }
    }

    /**
     * Transaction committer bolt
     */
    public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
        TransactionAttempt _attempt;
        BatchOutputCollector _collector;

        int _sum = 0;

        @Override
        public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt attempt) {
            _collector = collector;
            _attempt = attempt;
        }

        @Override
        public void execute(Tuple tuple) {
            _sum += tuple.getInteger(1);
        }

        /**
         * 当一个事务结束时提交，必须是顺序性的
         */
        @Override
        public void finishBatch() {
            Value val = DATABASE.get(GLOBAL_COUNT_KEY);
            Value newVal = null;
            if (val == null || !val.txid.equals(_attempt.getTransactionId())) {
                newVal = new Value();
                newVal.txid = _attempt.getTransactionId();
                if (val == null) {
                    newVal.count = _sum;
                } else {
                    newVal.count = _sum + val.count;
                }
                DATABASE.put(GLOBAL_COUNT_KEY, newVal);
                LOG.info("[finishBatch_UpdateGlobalCount] diff trancation" + " trancationId: " + newVal.txid + " count: " + newVal.count);
            } else {
                newVal = val;
            }
            LOG.info("[finishBatch_UpdateGlobalCount] " + " trancationId: " + newVal.txid + " count: " + newVal.count);
            _collector.emit(new Values(_attempt, newVal.count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("id", "sum"));
        }
    }

    public static void main(String[] args) throws Exception {
        // 自带的内存事务spout，消息源，分发规则，bath的个数
        MemoryTransactionalSpout spout = new MemoryTransactionalSpout(DATA, new Fields("word"), PARTITION_TAKE_PER_BATCH);
        TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global_count", "spout", spout, 3);
        builder.setBolt("partial_count", new BatchCount(), 5).noneGrouping("spout");
        builder.setBolt("sum", new UpdateGlobalCount()).globalGrouping("partial_count");
        LocalCluster cluster = new LocalCluster();
        Config config = new Config();
        config.setDebug(true);
        config.setMaxSpoutPending(3);
        cluster.submitTopology("topology_transactional", config, builder.buildTopology());
        Thread.sleep(3000);
        cluster.shutdown();
    }
}
