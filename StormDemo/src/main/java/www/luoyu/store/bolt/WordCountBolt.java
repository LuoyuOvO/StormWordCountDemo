package www.luoyu.store.bolt;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import www.luoyu.store.util.MapSortUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class WordCountBolt implements IRichBolt {

    private OutputCollector outputCollector;
    Map<String,Integer> counter;

    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.outputCollector = outputCollector;
        counter = new HashMap<String, Integer>();
    }

    public void execute(Tuple tuple) {
        String getword = tuple.getString(0);
        if (counter.containsKey(getword)){
            Integer count = counter.get(getword);
            counter.put(getword,++count);
        }else {
            counter.put(getword,1);
        }

        int num = 8;
        int length = 0;
        counter = MapSortUtil.sort(counter);
        if (num<counter.keySet().size()){
            length = num;
        }else {
            length = counter.keySet().size();
        }
        String word= "";
        int count = 0;
        for (String key:counter.keySet()){
            if (count>=length){
                break;
            }
            if (count == 0){
                word = "["+key+":"+counter.get(key)+"]";
            }else {
                word = word+",["+key+":"+counter.get(key)+"]";
            }
            count++;
        }
        word = "The first "+num+" : "+word;
        outputCollector.emit(new Values(word));
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
