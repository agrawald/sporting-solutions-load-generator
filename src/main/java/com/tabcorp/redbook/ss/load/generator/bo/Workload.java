package com.tabcorp.redbook.ss.load.generator.bo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

/**
 * Created by agrawald on 7/02/17.
 */
@Data
public abstract class Workload {
    private String resourceName;
    private String marketWorkerName;
    private boolean isTest = true;
    @JsonIgnore
    private String type;

    public Workload(String resourceName, String marketWorkerName, String type) {
        this.resourceName = resourceName;
        this.marketWorkerName = marketWorkerName;
        this.type = type;
    }
}
