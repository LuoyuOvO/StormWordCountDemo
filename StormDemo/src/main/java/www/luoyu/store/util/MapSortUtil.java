package www.luoyu.store.util;

import java.util.*;

public class MapSortUtil {

    public static Map<String,Integer> sort(Map<String,Integer> map ){
        if (map == null) return null;
        List list = new LinkedList(map.entrySet());
        Collections.sort(list, new Comparator() {
            public int compare(Object o1, Object o2) {
                Comparable s1 = (Comparable)((Map.Entry)o1).getValue();
                Comparable s2 = (Comparable)((Map.Entry)o2).getValue();
                return s2.compareTo(s1);
            }
        });
        Map result = new LinkedHashMap();
        for (Iterator it = list.iterator();it.hasNext();){
            Map.Entry entry = (Map.Entry)it.next();
            result.put(entry.getKey(),entry.getValue());
        }
        return result;
    }
}
