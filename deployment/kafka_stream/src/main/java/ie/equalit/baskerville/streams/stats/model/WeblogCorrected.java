package ie.equalit.baskerville.streams.stats.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import ie.equalit.baskerville.streams.stats.model.Weblog;

public class WeblogCorrected {
    String hostname;
    Long allbytes;
    Long cachedbytes;
    Long allhits;
    Long cachedhits;
    ArrayList<String> client_ip;
    Long viewed_page_count;
    String window_end;

    ArrayList<Object> ua;
    ArrayList<Object> country_codes;
    ArrayList<Object> client_url;
    ArrayList<Object> viewed_pages;
    ArrayList<Object> http_code;
    ArrayList<Object> content_type;
    ArrayList<Object> utm_source;
    ArrayList<Object> utm_medium;
    ArrayList<Object> utm_campaign;

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
        this.hostname = original.host;
        this.allbytes = original.allbytes;
        this.cachedbytes = original.cachedbytes;
        this.allhits = original.allhits;
        this.cachedhits= original.cachedhits;
        this.client_ip = original.client_ip;
        this.viewed_page_count = original.viewed_page_count;
        this.window_end = original.window_end;

        this.ua = this.correct_map(original.ua);
        this.country_codes = this.correct_map(original.country_codes);
        this.client_url = this.correct_map(original.client_url);
        this.viewed_pages = this.correct_map(original.viewed_pages);
        this.http_code = this.correct_map(original.http_code);

        this.content_type = this.correct_map(original.content_type);
        this.utm_source = this.correct_map(original.utm_source);
        this.utm_medium = this.correct_map(original.utm_medium);
        this.utm_campaign = this.correct_map(original.utm_campaign);
    }

}