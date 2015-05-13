package com.dianping.cosmos.bolt;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

public class PrinterBolt extends BaseBasicBolt{

    private static final long serialVersionUID = 1L;

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        System.out.println(tuple.getValue(0));
        System.out.println(tuple.getValue(1));
        System.out.println(tuple.getValue(2));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer ofd) {
    }

}
