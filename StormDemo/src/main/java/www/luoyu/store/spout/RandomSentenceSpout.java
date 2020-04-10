package www.luoyu.store.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;

import java.util.Map;
import java.util.Random;

public class RandomSentenceSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;
    private Random random;


    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.spoutOutputCollector = spoutOutputCollector;
        random = new Random();
    }

    public void nextTuple() {
        Utils.sleep(2000);
        String[] word = new String[]{
                "ZooKeeper is a centralized service for maintaining configuration information, naming",
                "and race conditions that are inevitable. Because of the difficulty of implementing these kinds of services",
                "which make them brittle in the presence of change and difficult to manage.",
                "Start by installing ZooKeeper on a single machine or a very small cluster.",
                "Apache ZooKeeper is an open source volunteer project under the Apache Software Foundation.",
                "Apache ZooKeeper, ZooKeeper, Apache, the Apache feather logo, and the Apache ZooKeeper project logo are ",
                "Apache Storm is a free and open source distributed realtime computation system. Apache Storm makes it easy to reliably process unbounded streams of data, doing for realtime processing",
                "a benchmark clocked it at over a million tuples processed per second per node",
                "data and processes those streams in arbitrarily complex ways, repartitioning the streams between ",
                "The Apache Hadoop software library is a framework that allows for the distributed processing of large data sets across ",
                " It is designed to scale up from single servers to thousands of machines, each offering local computation and storage",
                "and monitoring Apache Hadoop clusters which includes support for Hadoop HDFS, Hadoop MapReduce"
        };
        String temp = word[random.nextInt(word.length)];
        spoutOutputCollector.emit(new Values(temp.trim().toLowerCase()));
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
