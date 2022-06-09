package ie.equalit.baskerville.streams.stats.model;

import java.util.ArrayList;
import java.util.HashMap;

public class Weblog {

    String host;
    int allbytes;
    int cachedbytes;
    int allhits;
    int cachedhits;
    ArrayList<String> client_ip;
    HashMap<String, String> country_codes;
    HashMap<String, String> client_url;
    HashMap<String, String> viewed_pages;
    int viewed_page_count;
    HashMap<String, String> ua;
    HashMap<String, String> http_code;
    String window_end;

    public Weblog(
        String host,
        int allbytes,
        int cachedbytes,
        int allhits,
        int cachedhits,
        ArrayList<String> client_ip,
        HashMap<String, String> country_codes,
        HashMap<String, String> client_url,
        HashMap<String, String> viewed_pages,
        int viewed_page_count,
        HashMap<String, String> ua,
        HashMap<String, String> http_code,
        String window_end
    ) {
        this.host = host;
        this.allbytes = allbytes;
        this.cachedbytes = cachedbytes;
        this.allhits = allhits;
        this.cachedhits= cachedhits;
        this.client_ip = client_ip;
        this.country_codes = country_codes;
        this.client_url = client_url;
        this.viewed_pages = viewed_pages;
        this.viewed_page_count = viewed_page_count;
        this.ua = ua;
        this.http_code = http_code;
    }

    @Override
    public String toString() {
        return "WeblogStat {" +
        "host='" + host  +
        ", allbytes='" + allbytes  +
        ", cachedbytes='" + cachedbytes  +
        ", client_ip='" + client_ip  +
        ", country_codes='" + country_codes  +
        ", client_url='" + client_url  +
        ", viewed_pages='" + viewed_pages  +
        ", viewed_page_count='" + viewed_page_count  +
        ", ua='" + ua  +
        ", http_code='" + http_code;
    }

    public Weblog correct(){
        return this;
    }
}
