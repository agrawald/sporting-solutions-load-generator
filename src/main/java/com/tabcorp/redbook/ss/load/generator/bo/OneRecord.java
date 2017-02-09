package com.tabcorp.redbook.ss.load.generator.bo;

import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by agrawald on 31/01/17.
 */
@Data
@NoArgsConstructor
public class OneRecord {
    private String publishTimestamp;
    private String token;
    private String payload;
    private byte[] gzippedPayload;
    private String type;

    public OneRecord(String publishTimestamp, String token, String payload) {
        this.publishTimestamp = publishTimestamp;
        this.token = token;
        this.payload = payload;
    }
}
