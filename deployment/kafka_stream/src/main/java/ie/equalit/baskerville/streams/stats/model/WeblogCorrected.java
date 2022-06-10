package ie.equalit.baskerville.streams.stats.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import ie.equalit.baskerville.streams.stats.model.Weblog;

public class WeblogCorrected {
    String HOST;
    int ALLBYTES;
    int CACHEDBYTES;
    int ALLHITS;
    int CACHEDHITS;
    ArrayList<String> CLIENT_IP;
    int VIEWED_PAGE_COUNT;
    String WINDOW_END;

    ArrayList<Object> UA;
    ArrayList<Object> COUNTRY_CODES;
    ArrayList<Object> CLIENT_URL;
    ArrayList<Object> VIEWED_PAGES;
    ArrayList<Object> HTTP_CODE;

    private ArrayList<Object> correct_map(HashMap<String, String> original){
        ArrayList<Object> result = new ArrayList<Object>();
        for (Map.Entry<String, String> entry : original.entrySet()) {
            if(entry.getKey().length() == 0)
                continue;
            HashMap<String, Object> item = new HashMap<String, Object>();
            item.put("key", entry.getKey());
            item.put("doc_count", Integer.parseInt(entry.getValue()));
            result.add(item);
        }
        return result;
    }


    public WeblogCorrected(Weblog original) {
        this.HOST = original.HOST;
        this.ALLBYTES = original.ALLBYTES;
        this.CACHEDBYTES = original.CACHEDBYTES;
        this.ALLHITS = original.ALLHITS;
        this.CACHEDHITS= original.CACHEDHITS;
        this.CLIENT_IP = original.CLIENT_IP;
        this.VIEWED_PAGE_COUNT = original.VIEWED_PAGE_COUNT;
        this.WINDOW_END = original.WINDOW_END;

        this.UA = this.correct_map(original.UA);
        this.COUNTRY_CODES = this.correct_map(original.COUNTRY_CODES);
        this.CLIENT_URL = this.correct_map(original.CLIENT_URL);
        this.VIEWED_PAGES = this.correct_map(original.VIEWED_PAGES);
        this.HTTP_CODE = this.correct_map(original.HTTP_CODE);
    }

}