package com.dianping.cosmos;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.TopologyBuilder;
import com.dianping.cosmos.bolt.PrinterBolt;
import com.dianping.cosmos.swallow.SwallowSpout;

/**
 * Created with IntelliJ IDEA.
 * User: haoyu.zhao
 * Date: 15-5-8
 * Time: 下午2:39
 * To change this template use File | Settings | File Templates.
 */
public class SwallowSpoutTest {
    private static final String TOPIC = "paycenter.paymentNotify.order.order";
    private static final String CONSUMER_ID = "new_user_test";
    public static void main(String[] args){
        TopologyBuilder builder = new TopologyBuilder();


        builder.setSpout("swallow-spout",
                new SwallowSpout(TOPIC, CONSUMER_ID), 1);
        builder.setBolt("Print", new PrinterBolt(), 1).shuffleGrouping("swallow-spout", TOPIC);

        Config conf = new Config();
        conf.setDebug(false);
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("TestSwallow", conf, builder.createTopology());
    }
}
