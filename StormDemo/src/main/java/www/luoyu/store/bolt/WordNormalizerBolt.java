package www.luoyu.store.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.Map;

public class WordNormalizerBolt implements IRichBolt {

    private OutputCollector outputCollector;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
    }

    public void execute(Tuple tuple) {
        String temp = tuple.getString(0);
        String[] word = temp.split(" ");
        for (String s :word){
            outputCollector.emit(new Values(s));
        }
    }

    public void cleanup() {

    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("word"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
