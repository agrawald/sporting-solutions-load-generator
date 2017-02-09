package com.tabcorp.redbook.ss.load.generator.processor;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.tabcorp.redbook.ss.load.generator.bo.Delta;
import com.tabcorp.redbook.ss.load.generator.bo.OneRecord;
import com.tabcorp.redbook.ss.load.generator.bo.Snapshot;
import com.tabcorp.redbook.ss.load.generator.bo.Workload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Created by agrawald on 31/01/17.
 */
@Slf4j
public class WorkloadItemProcessor implements ItemProcessor<OneRecord, Workload> {
    @Autowired
    ObjectMapper objectMapper;
    @Value("${gearman.worker.snapshot:ss_snapshot}")
    String snapshotWorker;
    @Value("${gearman.worker.delta:ss_delta}")
    String deltaWorker;

    @Override
    public Workload process(OneRecord item) throws Exception {
        byte[] gzippedPayload = compressAndEncode(item.getPayload());
        item.setGzippedPayload(gzippedPayload);
        log.info("Finished compressing: {}", item.getToken());
        try {
            if("snapshot".equalsIgnoreCase(item.getType())) {
                return new Snapshot(gzippedPayload,
                        objectMapper.readTree(item.getPayload()).findValue("FixtureName").asText(),
                        "ss_burrito_market_update", snapshotWorker);
            } else {
                return new Delta(gzippedPayload,
                        objectMapper.readTree(item.getPayload()).findValue("FixtureName").asText(),
                        "ss_burrito_market_update", deltaWorker);
            }
        } catch (Exception ex) {
            log.error("Error while processing the record: {}", item);
        }
        return null;
    }

    public static byte[] compressAndEncode(final String str) throws IOException {
        if ((str == null) || (str.length() == 0)) {
            return null;
        }
        return str.getBytes(StandardCharsets.UTF_8);
    }
}
