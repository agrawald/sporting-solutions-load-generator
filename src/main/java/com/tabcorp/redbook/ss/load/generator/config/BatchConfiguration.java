package com.tabcorp.redbook.ss.load.generator.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.tabcorp.redbook.ss.load.generator.bo.OneRecord;
import com.tabcorp.redbook.ss.load.generator.bo.Workload;
import com.tabcorp.redbook.ss.load.generator.listener.JobCompletionNotificationListener;
import com.tabcorp.redbook.ss.load.generator.processor.FilterEventItemProcessor;
import com.tabcorp.redbook.ss.load.generator.processor.LogItemProcessor;
import com.tabcorp.redbook.ss.load.generator.processor.RecordTypeItemProcessor;
import com.tabcorp.redbook.ss.load.generator.processor.WorkloadItemProcessor;
import com.tabcorp.redbook.ss.load.generator.writer.GearmanItemWriter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.separator.SimpleRecordSeparatorPolicy;
import org.springframework.batch.item.file.transform.RegexLineTokenizer;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.PathResource;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Created by agrawald on 31/01/17.
 */
@Slf4j
@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    @Autowired
    public JobBuilderFactory jobBuilderFactory;
    @Autowired
    public StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job processSportingSolutionLoad() throws IOException {
        return jobBuilderFactory.get("importSSSnapshotRecordsJob")
                .incrementer(new RunIdIncrementer())
                .listener(jobCompletionNotificationListener())
                .flow(stepReadSSSnapshotFiles())
                .next(stepReadSSDeltaFiles())
                .next(stepPublishToGearman())
                .end()
                .build();
    }

    @Bean
    public DataSource dataSource() {
        // no need shutdown, EmbeddedDatabaseFactoryBean will take care of this
        EmbeddedDatabaseBuilder builder = new EmbeddedDatabaseBuilder();
        EmbeddedDatabase db = builder
                .setType(EmbeddedDatabaseType.HSQL) //.H2 or .DERBY
                .addScript("schema-all.sql")
                .build();
        return db;
    }

    public MultiResourceItemReader<OneRecord> multiFileReader(String folderPath) throws IOException {
        MultiResourceItemReader<OneRecord> multiResourceItemReader = new MultiResourceItemReader<>();
        multiResourceItemReader.setDelegate(flatFileReader());

        List<PathResource> pathResourceList = Files.walk(Paths.get(folderPath), FileVisitOption.FOLLOW_LINKS)
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
                .collect(Collectors.toList());
        log.info("Processing following files");
        multiResourceItemReader.setResources(pathResourceList.toArray(new PathResource[0]));
        return multiResourceItemReader;
    }

    @Bean
    public FlatFileItemReader<OneRecord> flatFileReader() {
        FlatFileItemReader<OneRecord> reader = new FlatFileItemReader<>();
        reader.setRecordSeparatorPolicy(new SimpleRecordSeparatorPolicy() {
            Pattern pattern = Pattern.compile("^(?<publishTimestamp>\\S*\\s\\S*)\\s:\\s\\((?<token>\\w*)\\)\\s(?<payload>[\\S\\s]*)");
            @Override
            public boolean isEndOfRecord(String record) {
                return record.trim().length() != 0 && super.isEndOfRecord(record);
            }

            @Override
            public String postProcess(String record) {
                if (record == null || record.trim().length() == 0 || record.contains("(The above message has repeated 2 times)")) {
                    return null;
                }
                Matcher matcher = pattern.matcher(record);
                if(!matcher.matches()) {
                    return null;
                }
                return super.postProcess(record);
            }
        });
        reader.setLineMapper(new DefaultLineMapper<OneRecord>() {{
            setLineTokenizer(new RegexLineTokenizer() {{
                setRegex("^(?<publishTimestamp>\\S*\\s\\S*)\\s:\\s\\((?<token>\\w*)\\)\\s(?<payload>[\\S\\s]*)");
                setNames(new String[]{"publishTimestamp", "token", "payload"});
            }});
            setFieldSetMapper(new BeanWrapperFieldSetMapper<OneRecord>() {{
                setTargetType(OneRecord.class);
            }});
        }});
        return reader;
    }

    @Bean
    public JdbcBatchItemWriter<OneRecord> writer() {
        JdbcBatchItemWriter<OneRecord> writer = new JdbcBatchItemWriter<>();
        writer.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());
        //2017-01-30 19:44:15.243511 YYYY-MM-DD HH24:MI:SS.FF
        writer.setSql("INSERT INTO records (token, publishTimestamp, payload, type) VALUES (:token, to_timestamp(:publishTimestamp, 'YYYY-MM-DD HH24:MI:SS.FF'), :payload, :type)");
        writer.setDataSource(dataSource());
        return writer;
    }

    @Bean
    public JdbcCursorItemReader<OneRecord> reader() {
        JdbcCursorItemReader<OneRecord> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource());
        reader.setSql("SELECT publishTimestamp, token, payload, type FROM records ORDER BY publishTimestamp ASC");
        reader.setRowMapper((resultSet, i) -> {
            final OneRecord oneRecord = new OneRecord();
            oneRecord.setPublishTimestamp(resultSet.getString(1));
            oneRecord.setToken(resultSet.getString(2));
            oneRecord.setPayload(resultSet.getString(3));
            oneRecord.setType(resultSet.getString(4));
            return oneRecord;
        });
        return reader;
    }

    @Bean
    JobCompletionNotificationListener jobCompletionNotificationListener() {
        return new JobCompletionNotificationListener();
    }

    @Bean
    LogItemProcessor logItemProcessor() {
        return new LogItemProcessor();
    }

    @Bean
    FilterEventItemProcessor filterEventItemProcessor() {
        return new FilterEventItemProcessor();
    }

    RecordTypeItemProcessor recordTypeItemProcessor(String type) {
        return new RecordTypeItemProcessor(type);
    }

    CompositeItemProcessor<OneRecord, OneRecord> stepFileCompositeItemProcessor(String type) {
        CompositeItemProcessor<OneRecord, OneRecord> processor = new CompositeItemProcessor<>();
        processor.setDelegates(Arrays.asList(filterEventItemProcessor(), logItemProcessor(), recordTypeItemProcessor(type)));
        return processor;
    }

    @Bean
    GearmanItemWriter gearmanItemWriter() {
        return new GearmanItemWriter();
    }


    @Bean
    WorkloadItemProcessor workloadItemProcessor() {
        return new WorkloadItemProcessor();
    }


    @Bean("stepGearmanCompositeItemProcessor")
    CompositeItemProcessor<OneRecord, Workload> stepGearmanCompositeItemProcessor() {
        CompositeItemProcessor<OneRecord, Workload> processor = new CompositeItemProcessor<>();
        processor.setDelegates(Arrays.asList(workloadItemProcessor()));
        return processor;
    }

    @Bean("stepReadSSSnapshotFiles")
    public Step stepReadSSSnapshotFiles() throws IOException {
        return stepBuilderFactory.get("stepReadSSSnapshotFiles")
                .<OneRecord, OneRecord>chunk(10)
                .reader(multiFileReader("data/snapshot"))
                .processor(stepFileCompositeItemProcessor("snapshot"))
                .writer(writer())
                .build();
    }

    @Bean("stepReadSSDeltaFiles")
    public Step stepReadSSDeltaFiles() throws IOException {
        return stepBuilderFactory.get("stepReadSSDeltaFiles")
                .<OneRecord, OneRecord>chunk(10)
                .reader(multiFileReader("data/delta"))
                .processor(stepFileCompositeItemProcessor("delta"))
                .writer(writer())
                .build();
    }

    @Bean("stepPublishToGearman")
    public Step stepPublishToGearman() throws IOException {
        return stepBuilderFactory.get("stepPublishToGearman")
                .<OneRecord, Workload>chunk(10)
                .reader(reader())
                .processor(stepGearmanCompositeItemProcessor())
                .writer(gearmanItemWriter())
                .build();
    }

    @Bean
    ObjectMapper objectMapper() {
        ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
        return mapper;
    }
}
