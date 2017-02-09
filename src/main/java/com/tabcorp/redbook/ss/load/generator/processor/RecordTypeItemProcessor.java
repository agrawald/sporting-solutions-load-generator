package com.tabcorp.redbook.ss.load.generator.processor;

import com.tabcorp.redbook.ss.load.generator.bo.OneRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;

/**
 * Created by agrawald on 7/02/17.
 */
@Slf4j
public class RecordTypeItemProcessor implements ItemProcessor<OneRecord, OneRecord>{
    private final String type;

    public RecordTypeItemProcessor(String type) {
        this.type = type;
    }

    @Override
    public OneRecord process(OneRecord item) throws Exception {
        item.setType(type);
        return item;
    }
}
