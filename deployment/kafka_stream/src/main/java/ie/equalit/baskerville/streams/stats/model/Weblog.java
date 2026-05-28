package ie.equalit.baskerville.streams.stats.model;

import java.util.ArrayList;
import java.util.HashMap;


public class Weblog {

    String host;
    Long allbytes;
    Long cachedbytes;
    Long allhits;
    Long cachedhits;
    ArrayList<String> client_ip;
    HashMap<String, String> country_codes;
    HashMap<String, String> client_url;
    HashMap<String, String> viewed_pages;
    Long viewed_page_count;
    HashMap<String, String> ua;
    HashMap<String, String> http_code;
    String window_end;
    HashMap<String, String> content_type;
    HashMap<String, String> utm_source;
    HashMap<String, String> utm_medium;
    HashMap<String, String> utm_campaign;

    public Weblog(
        String host,
        Long allbytes,
        Long cachedbytes,
        Long allhits,
        Long cachedhits,
        ArrayList<String> client_ip,
        HashMap<String, String> country_codes,
        HashMap<String, String> client_url,
        HashMap<String, String> viewed_pages,
        Long viewed_page_count,
        HashMap<String, String> ua,
        HashMap<String, String> http_code,
        HashMap<String, String> content_type,
        HashMap<String, String> utm_source,
        HashMap<String, String> utm_medium,
        HashMap<String, String> utm_campaign,
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
        this.window_end = window_end;
        this.content_type = content_type;
        this.utm_source = utm_source;
        this.utm_medium = utm_medium;
        this.utm_campaign = utm_campaign;
    }

    @Override
    public String toString() {
        return "WeblogStat {" +
        "host='" + this.host  +
        ", allbytes='" + this.allbytes  +
        ", cachedbytes='" + this.cachedbytes  +
        ", client_ip='" + this.client_ip  +
        ", country_codes='" + this.country_codes  +
        ", client_url='" + this.client_url  +
        ", viewed_pages='" + this.viewed_pages  +
        ", viewed_page_count='" + this.viewed_page_count  +
        ", ua='" + this.ua  +
        ", http_code='" + this.http_code;
    }
}