package com.tabcorp.redbook.ss.load.generator.writer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tabcorp.redbook.ss.load.generator.bo.Workload;
import lombok.extern.slf4j.Slf4j;
import org.gearman.GearmanClient;
import org.gearman.GearmanJobEvent;
import org.gearman.GearmanJobEventType;
import org.gearman.GearmanJobReturn;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * Created by agrawald on 29/09/16.
 */
@Slf4j
@Component
public class GearmanItemWriter implements ItemWriter<Workload> {
    @Autowired
    GearmanClient gearmanClient;
    @Autowired
    ObjectMapper objectMapper;

    @Override
    public void write(List<? extends Workload> items) throws Exception {
        if (items != null || !items.isEmpty()) {
            items.parallelStream()
                    .forEach(item -> {
                        log.info("Submitting the job to the worker: {} ", item.getType());
                        GearmanJobReturn gearmanJobReturn = null;
                        try {
                            gearmanJobReturn = gearmanClient.submitJob(item.getType(), objectMapper.writeValueAsBytes(item));
                            GearmanJobEvent gearmanJobEvent = gearmanJobReturn.poll();
                            while (gearmanJobEvent.getEventType() != GearmanJobEventType.GEARMAN_EOF) {
                                gearmanJobEvent = gearmanJobReturn.poll();
                            }
                            log.info("Job Taken by Gearman: {}", gearmanJobEvent);
                        } catch (Exception e) {
                            log.error("Error while submitting the job", e);
                        }
                    });
        }
    }
}
