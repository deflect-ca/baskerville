package ie.equalit.baskerville.streams.stats.model;

import java.util.ArrayList;
import java.util.HashMap;


public class Weblog {

    String HOST;
    Long ALLBYTES;
    Long CACHEDBYTES;
    Long ALLHITS;
    Long CACHEDHITS;
    ArrayList<String> CLIENT_IP;
    HashMap<String, String> COUNTRY_CODES;
    HashMap<String, String> CLIENT_URL;
    HashMap<String, String> VIEWED_PAGES;
    Long VIEWED_PAGE_COUNT;
    HashMap<String, String> UA;
    HashMap<String, String> HTTP_CODE;
    String WINDOW_END;

    public Weblog(
        String HOST,
        Long ALLBYTES,
        Long CACHEDBYTES,
        Long ALLHITS,
        Long CACHEDHITS,
        ArrayList<String> CLIENT_IP,
        HashMap<String, String> COUNTRY_CODES,
        HashMap<String, String> CLIENT_URL,
        HashMap<String, String> VIEWED_PAGES,
        Long VIEWED_PAGE_COUNT,
        HashMap<String, String> UA,
        HashMap<String, String> HTTP_CODE,
        String WINDOW_END
    ) {
        this.HOST = HOST;
        this.ALLBYTES = ALLBYTES;
        this.CACHEDBYTES = CACHEDBYTES;
        this.ALLHITS = ALLHITS;
        this.CACHEDHITS= CACHEDHITS;
        this.CLIENT_IP = CLIENT_IP;
        this.COUNTRY_CODES = COUNTRY_CODES;
        this.CLIENT_URL = CLIENT_URL;
        this.VIEWED_PAGES = VIEWED_PAGES;
        this.VIEWED_PAGE_COUNT = VIEWED_PAGE_COUNT;
        this.UA = UA;
        this.HTTP_CODE = HTTP_CODE;
        this.WINDOW_END = WINDOW_END;
    }

    @Override
    public String toString() {
        return "WeblogStat {" +
        "host='" + this.HOST  +
        ", allbytes='" + this.ALLBYTES  +
        ", cachedbytes='" + this.CACHEDBYTES  +
        ", client_ip='" + this.CLIENT_IP  +
        ", country_codes='" + this.COUNTRY_CODES  +
        ", client_url='" + this.CLIENT_URL  +
        ", viewed_pages='" + this.VIEWED_PAGES  +
        ", viewed_page_count='" + this.VIEWED_PAGE_COUNT  +
        ", ua='" + this.UA  +
        ", http_code='" + this.HTTP_CODE;
    }
}