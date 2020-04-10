package www.luoyu.store.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class PrintBolt extends BaseBasicBolt {

    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            String s = tuple.getString(0);
            if (s != null) {
                System.out.println(s);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }
}
