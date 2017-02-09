package com.tabcorp.redbook.ss.load.generator;

import com.tabcorp.redbook.ss.load.generator.listener.JobCompletionNotificationListener;
import lombok.extern.slf4j.Slf4j;
import net.lingala.zip4j.core.ZipFile;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.core.io.PathResource;

import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 * Created by agrawald on 31/01/17.
 */
@Slf4j
@SpringBootApplication
public class LoadGenerator implements CommandLineRunner {
    public static void main(String[] args) throws Exception {
        SpringApplication.run(LoadGenerator.class, args);
    }


    public void unzipAll() throws IOException {
        Files.walk(Paths.get("data/zip"), FileVisitOption.FOLLOW_LINKS)
                .map(path -> new PathResource(path.toAbsolutePath()))
                .filter(pathResource -> {
                    boolean isFile;
                    try {
                        isFile = pathResource.getFile().isFile();
                        log.info("Identified: {}", pathResource.getFilename());
                    } catch (IOException e) {
                        log.error("Error while reading the file", e);
                        isFile = false;
                    }
                    return isFile;
                })
                .forEach(pathResource -> {
                    if (pathResource.getFilename().contains("snapshot")) {
                        unzip(pathResource.getPath(), "data/snapshot");
                    } else {
                        unzip(pathResource.getPath(), "data/delta");
                    }
                });
    }

    public void unzip(String file, String destination) {
        try {
            ZipFile zipFile = new ZipFile(file);
            zipFile.extractAll(destination);
        } catch (Exception e) {
            throw new RuntimeException("Failed to unzip file: " + file, e);
        }
    }

    @Override
    public void run(String... strings) throws Exception {
//        unzipAll();
//        processSportingSolutionLoad();
    }
}
