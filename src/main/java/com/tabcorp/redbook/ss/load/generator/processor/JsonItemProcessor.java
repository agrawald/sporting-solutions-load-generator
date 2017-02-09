package com.tabcorp.redbook.ss.load.generator.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tabcorp.redbook.ss.load.generator.bo.Workload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Created by agrawald on 31/01/17.
 */
@Slf4j
public class JsonItemProcessor implements ItemProcessor<Workload, byte[]> {

    @Autowired
    ObjectMapper objectMapper;

    @Override
    public byte[] process(Workload item) throws Exception {
        return objectMapper.writeValueAsBytes(item);
    }
}
