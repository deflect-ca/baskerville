package ie.equalit.baskerville.streams.stats.model;

import java.util.ArrayList;
import java.util.HashMap;


public class Banjaxlog {

    String HOST;
    ArrayList<String> CLIENT_IP;
    HashMap<String, String> COUNTRY_CODES;
    HashMap<String, String> TARGET_URL;
    int UNIQUEBOTS;
    String WINDOW_END;

    public Weblog(
    String HOST,
    ArrayList<String> CLIENT_IP,
    HashMap<String, String> COUNTRY_CODES,
    HashMap<String, String> TARGET_URL,
    int UNIQUEBOTS,
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
    }

    @Override
    public String toString() {
        return "BanjaxlogStat {" +
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