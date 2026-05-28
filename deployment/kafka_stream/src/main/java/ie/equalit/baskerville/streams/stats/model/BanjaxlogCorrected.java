package ie.equalit.baskerville.streams.stats.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import ie.equalit.baskerville.streams.stats.model.Banjaxlog;

public class BanjaxlogCorrected {
    String hostname;
    ArrayList<String> client_ip;
    Long uniquebots;
    String window_end;

    ArrayList<Object> target_url;
    ArrayList<Object> country_codes;

    private ArrayList<Object> correct_map(HashMap<String, String> original){
        ArrayList<Object> result = new ArrayList<Object>();
        for (Map.Entry<String, String> entry : original.entrySet()) {
            HashMap<String, Object> item = new HashMap<String, Object>();
            item.put("key", entry.getKey());
            item.put("doc_count", Integer.parseInt(entry.getValue()));
            result.add(item);
        }
        return result;
    }


    public BanjaxlogCorrected(Banjaxlog original) {
        this.hostname = original.host;
        this.client_ip = original.client_ip;
        this.uniquebots = original.uniquebots;
        this.window_end = original.window_end;

        this.target_url = this.correct_map(original.target_url);
        this.country_codes = this.correct_map(original.country_codes);
    }

}