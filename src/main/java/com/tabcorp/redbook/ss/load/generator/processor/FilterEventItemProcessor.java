package com.tabcorp.redbook.ss.load.generator.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tabcorp.redbook.ss.load.generator.bo.OneRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

/**
 * Created by agrawald on 7/02/17.
 */
@Slf4j
public class FilterEventItemProcessor implements ItemProcessor<OneRecord, OneRecord>{
    @Value("${process.this.event:}")
    private String eventExternalId;
    @Autowired
    ObjectMapper mapper;

    @Override
    public OneRecord process(OneRecord item) throws Exception {
        if(eventExternalId!=null || !eventExternalId.isEmpty() || eventExternalId.equals(mapper.readTree(item.getPayload()).findValue("Id").asText())) {
            return item;
        }
        return null;
    }
}
