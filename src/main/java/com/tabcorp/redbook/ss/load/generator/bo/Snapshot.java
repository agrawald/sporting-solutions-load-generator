package com.tabcorp.redbook.ss.load.generator.bo;

import lombok.Data;

/**
 * Created by agrawald on 7/02/17.
 */
@Data
public class Snapshot extends Workload {
    private byte[] snapshot;

    public Snapshot(byte[] snapshot, String resourceName, String marketWorkerName, String type) {
        super(resourceName, marketWorkerName, type);
        this.snapshot = snapshot;
    }
}
