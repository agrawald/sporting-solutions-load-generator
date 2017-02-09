package com.tabcorp.redbook.ss.load.generator.bo;

import lombok.Data;

/**
 * Created by agrawald on 7/02/17.
 */
@Data
public class Delta extends Workload {
    private byte[] delta;

    public Delta(byte[] delta, String resourceName, String marketWorkerName, String type) {
        super(resourceName, marketWorkerName, type);
        this.delta = delta;
    }
}
