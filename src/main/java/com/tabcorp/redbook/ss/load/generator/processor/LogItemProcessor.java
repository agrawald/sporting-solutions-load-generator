package com.tabcorp.redbook.ss.load.generator.processor;

import com.tabcorp.redbook.ss.load.generator.bo.OneRecord;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

/**
 * Created by agrawald on 7/02/17.
 */
@Slf4j
public class LogItemProcessor implements ItemProcessor<OneRecord, OneRecord>{

    @Override
    public OneRecord process(OneRecord item) throws Exception {
        log.debug("Item: {}", item);
        return item;
    }
}
