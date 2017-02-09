package com.tabcorp.redbook.ss.load.generator.config;

import lombok.extern.slf4j.Slf4j;
import org.gearman.*;
import org.gearman.impl.GearmanImpl;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Created by agrawald on 26/09/16.
 */
@Slf4j
@Configuration
public class GearmanConfig {
    /**
     * Exposing a bean for gearman
     *
     * @param coreThreads number of core thread for this gearman instance
     * @return Gearman
     * @throws IOException
     */
    @Bean
    Gearman gearman(@Value("${gearman.core.threads:10}") int coreThreads) throws IOException {
        Gearman gearman = new GearmanImpl(coreThreads);
        return gearman;
    }

    /**
     * Function to get the gearman sever for the ip and port passed
     * @param gearman {@link Gearman} instance
     * @param ip IP address for the host where gearman is running
     * @param port Port on which the gearman server is running
     * @return GearmanServer
     *
     * @throws IOException
     */
    @Bean
    GearmanServer gearmanServer(Gearman gearman,
                                @Value("${gearman.server.ip:localhost}") String ip,
                                @Value("${gearman.server.port:4730}") int port) throws IOException {
        GearmanServer server = gearman.createGearmanServer(ip, port);
        return server;
    }

    /**
     * Function to create a gearman client to submit gearman jobs
     * @param gearman {@link Gearman}
     * @param server {@link GearmanServer}
     * @return GearmanClient
     */
    @Bean
    GearmanClient gearmanClient(Gearman gearman, GearmanServer server) {
        final GearmanClient client = gearman.createGearmanClient();
        client.addServer(server);
        return client;
    }
}
