package com.github.behrangsa;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesis.AmazonKinesis;
import com.amazonaws.services.kinesis.AmazonKinesisClientBuilder;
import com.amazonaws.services.kinesis.model.PutRecordsRequest;
import com.amazonaws.services.kinesis.model.PutRecordsRequestEntry;
import com.amazonaws.services.kinesis.model.PutRecordsResult;
import com.amazonaws.services.kinesis.model.ResourceNotFoundException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertThat;

public class KinesisPutRecordsLimitTest {

    private static final Logger LOG = LogManager.getLogger(KinesisPutRecordsLimitTest.class);

    private static final String STREAM_NAME = KinesisPutRecordsLimitTest.class.getName().replaceAll("[\\W]", "_");

    private static final int SHARD_COUNT = 1;

    private static AmazonKinesis amazonKinesis;

    @BeforeClass
    public static void setup() throws Exception {
        amazonKinesis = AmazonKinesisClientBuilder.standard()
                                                  .withCredentials(new DefaultAWSCredentialsProviderChain())
                                                  .build();

//        deleteStream();
//        createStream();
    }

    @AfterClass
    public static void destroy() throws Exception {
//        deleteStream();
    }


    @Test
    public void shouldNotBeMoreThan1000RecordsPerShardPerSecond() {
        LOG.info("Creating 20 producers");

        final List<Producer> producers = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            producers.add(new Producer());
        }

        final List<Thread> threads = producers.stream().map(Thread::new).collect(Collectors.toList());

        final Instant startTime = Instant.now();
        LOG.info("Started at: {}", startTime);

        threads.forEach(Thread::start);

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        final Instant endTime = Instant.now();
        final Duration testDuration = Duration.between(startTime, endTime);
        final int successCount = producers.stream().mapToInt(Producer::getSuccess).sum();
        final int failCount = producers.stream().mapToInt(Producer::getFailed).sum();
        final double successPerSecond = successCount / (testDuration.toMillis() / 1000d);

        LOG.info("Number of records successfully put: {}", successCount);
        LOG.info("Number of failures: {}", failCount);
        LOG.info("Test duration: {}", testDuration);
        LOG.info("Success per second: {}", successPerSecond);

        assertThat(successPerSecond, lessThanOrEqualTo(1000d));
    }

    private static void createStream() throws InterruptedException {
        LOG.info("Creating stream: {}", STREAM_NAME);

        amazonKinesis.createStream(STREAM_NAME, SHARD_COUNT);

        String streamStatus;

        do {
            streamStatus = amazonKinesis.describeStream(STREAM_NAME)
                                        .getStreamDescription()
                                        .getStreamStatus();

            LOG.info("Stream status: {}", streamStatus);

            Thread.sleep(TimeUnit.SECONDS.toMillis(5));
        } while (!"ACTIVE".equals(streamStatus));
    }

    private static void deleteStream() throws InterruptedException {
        try {
            LOG.info("Deleting stream: {}", STREAM_NAME);

            amazonKinesis.deleteStream(STREAM_NAME);

            while (true) {
                final String streamStatus = amazonKinesis.describeStream(STREAM_NAME)
                                                         .getStreamDescription()
                                                         .getStreamStatus();

                LOG.info("Stream status: {}", streamStatus);

                Thread.sleep(TimeUnit.SECONDS.toMillis(5));
            }
        } catch (ResourceNotFoundException e) {
            LOG.info("Stream is deleted or no longer exists");
        }
    }

    public static class Producer implements Runnable {

        private AmazonKinesis amazonKinesis;

        private PutRecordsRequest request;

        private volatile Integer success;

        private volatile Integer failed;

        public Producer() {
            amazonKinesis = AmazonKinesisClientBuilder.standard()
                                                      .withCredentials(new DefaultAWSCredentialsProviderChain())
                                                      .build();

            request = new PutRecordsRequest().withStreamName(STREAM_NAME);

            final List<PutRecordsRequestEntry> entries = new ArrayList<>();
            for (int i = 0; i < 250; i++) {
                final byte[] payload = String.format("%s-%2d", Thread.currentThread().getName(), i).getBytes(UTF_8);
                entries.add(new PutRecordsRequestEntry().withData(ByteBuffer.wrap(payload)).withPartitionKey(String.valueOf(i)));
            }

            request.setRecords(entries);
        }

        @Override
        public void run() {
            LOG.info("Sending a request with {} entries", request.getRecords().size());

            final PutRecordsResult result = amazonKinesis.putRecords(request);

            LOG.info("Request sent");

            success = result.getRecords().size() - result.getFailedRecordCount();
            failed = result.getFailedRecordCount();
        }

        public Integer getSuccess() {
            return success;
        }

        public Integer getFailed() {
            return failed;
        }
    }
}
