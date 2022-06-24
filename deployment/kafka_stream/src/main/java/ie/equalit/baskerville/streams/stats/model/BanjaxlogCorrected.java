package ie.equalit.baskerville.streams.stats.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import ie.equalit.baskerville.streams.stats.model.Banjaxlog;

public class BanjaxlogCorrected {
    String HOST;
    ArrayList<String> CLIENT_IP;
    Long UNIQUEBOTS;
    String WINDOW_END;

    ArrayList<Object> TARGET_URL;
    ArrayList<Object> COUNTRY_CODES;

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
        this.HOST = original.HOST;
        this.CLIENT_IP = original.CLIENT_IP;
        this.UNIQUEBOTS = original.UNIQUEBOTS;
        this.WINDOW_END = original.WINDOW_END;

        this.TARGET_URL = this.correct_map(original.TARGET_URL);
        this.COUNTRY_CODES = this.correct_map(original.COUNTRY_CODES);
    }

}