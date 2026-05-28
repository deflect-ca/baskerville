package ie.equalit.baskerville.streams.stats.model;

import java.util.ArrayList;
import java.util.HashMap;


public class Banjaxlog {

    String host;
    ArrayList<String> client_ip;
    HashMap<String, String> country_codes;
    HashMap<String, String> target_url;
    Long uniquebots;
    String window_end;

    public Banjaxlog(
        String host,
        ArrayList<String> client_ip,
        HashMap<String, String> country_codes,
        HashMap<String, String> target_url,
        Long uniquebots,
        String window_end
    ) {
        this.host = host;
        this.client_ip = client_ip;
        this.uniquebots = uniquebots;
        this.country_codes = country_codes;
        this.target_url = target_url;
        this.window_end = window_end;
    }

    @Override
    public String toString() {
        return "BanjaxlogStat {" +
        "HOST='" + this.host  +
        ", CLIENT_IP='" + this.client_ip  +
        ", UNIQUEBOTS='" + this.uniquebots  +
        ", country_codes='" + this.country_codes  +
        ", TARGET_URL='" + this.target_url  +
        ", WINDOW_END='" + this.window_end;
    }
}