package www.luoyu.store;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import www.luoyu.store.bolt.PrintBolt;
import www.luoyu.store.bolt.WordCountBolt;
import www.luoyu.store.bolt.WordNormalizerBolt;
import www.luoyu.store.spout.RandomSentenceSpout;

public class WordCountTopology {
    private static TopologyBuilder builder = new TopologyBuilder();

    public static void main(String[] args) {
        Config config = new Config();
        builder.setSpout("RandomSentenceSpout",new RandomSentenceSpout(),2);
        builder.setBolt("WordNormalizerBolt",new WordNormalizerBolt(),2).shuffleGrouping("RandomSentenceSpout");
        builder.setBolt("WordCountBolt",new WordCountBolt(),2).fieldsGrouping("WordNormalizerBolt",new Fields("word"));
        builder.setBolt("PrintBolt",new PrintBolt(),1).shuffleGrouping("WordCountBolt");
        config.setDebug(false);
        if (args != null && args.length>0){
            try {
                config.setNumAckers(1);
                StormSubmitter.submitTopology(args[0],config,builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }else {
            try {
                config.setMaxTaskParallelism(1);
                LocalCluster cluster = new LocalCluster();
                cluster.submitTopology("wordcount",config,builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }

        }
    }
}
