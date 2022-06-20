package ie.equalit.baskerville.streams.stats.model;

import java.util.ArrayList;
import java.util.HashMap;


public class Banjaxlog {

    String HOST;
    ArrayList<String> CLIENT_IP;
    HashMap<String, String> COUNTRY_CODES;
    HashMap<String, String> TARGET_URL;
    Long UNIQUEBOTS;
    String WINDOW_END;

    public Banjaxlog(
        String HOST,
        ArrayList<String> CLIENT_IP,
        HashMap<String, String> COUNTRY_CODES,
        HashMap<String, String> TARGET_URL,
        Long UNIQUEBOTS,
        String WINDOW_END
    ) {
        this.HOST = HOST;
        this.CLIENT_IP = CLIENT_IP;
        this.UNIQUEBOTS = UNIQUEBOTS;
        this.COUNTRY_CODES = COUNTRY_CODES;
        this.TARGET_URL = TARGET_URL;
        this.WINDOW_END = WINDOW_END;
    }

    @Override
    public String toString() {
        return "BanjaxlogStat {" +
        "HOST='" + this.HOST  +
        ", CLIENT_IP='" + this.CLIENT_IP  +
        ", UNIQUEBOTS='" + this.UNIQUEBOTS  +
        ", country_codes='" + this.COUNTRY_CODES  +
        ", TARGET_URL='" + this.TARGET_URL  +
        ", WINDOW_END='" + this.WINDOW_END;
    }
}