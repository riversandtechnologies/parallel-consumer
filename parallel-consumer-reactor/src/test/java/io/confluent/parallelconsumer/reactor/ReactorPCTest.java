package io.confluent.parallelconsumer.reactor;

/*-
 * Copyright (C) 2020-2021 Confluent, Inc.
 */

import io.confluent.csid.utils.ProgressBarUtils;
import io.confluent.csid.utils.StringUtils;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelEoSStreamProcessorTestBase;
import io.confluent.parallelconsumer.ParentParallelEoSStreamProcessor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import me.tongfei.progressbar.ProgressBar;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.BaseStream;

import static com.google.common.truth.Truth.assertThat;
import static com.google.common.truth.Truth.assertWithMessage;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.CommitMode.PERIODIC_CONSUMER_SYNC;
import static io.confluent.parallelconsumer.truth.LongPollingMockConsumerSubject.assertThat;
import static org.awaitility.Awaitility.await;

@Slf4j
class ReactorPCTest extends ParallelEoSStreamProcessorTestBase {

    public static final int MAX_CONCURRENCY = 1000;
    ReactorProcessor<String, String> rp;

    @Override
    protected ParentParallelEoSStreamProcessor initAsyncConsumer(ParallelConsumerOptions parallelConsumerOptions) {
        var build = parallelConsumerOptions.toBuilder()
                .commitMode(PERIODIC_CONSUMER_SYNC)
                .maxConcurrency(MAX_CONCURRENCY)
//                .ordering(ParallelConsumerOptions.ProcessingOrder.KEY)
                .build();

        rp = new ReactorProcessor<>(build);

        return rp;
    }

    @BeforeEach
    public void setupData() {
        super.primeFirstRecord();
    }

    @Test
    void kickTires() {
        primeFirstRecord();
        primeFirstRecord();
        primeFirstRecord();

        ConcurrentLinkedQueue<Object> msgs = new ConcurrentLinkedQueue<>();

        rp.react((rec) -> {
            log.info("Reactor user poll function: {}", rec);
            msgs.add(rec);
            Mono<String> result = Mono.just(StringUtils.msg("result: {}:{}", rec.offset(), rec.value()));
//            Flux<String> stringFlux = fromPath("/tmp/out.html");
            return result;
        });

        await()
                .atMost(Duration.ofSeconds(1))
                .untilAsserted(() -> {
                    assertWithMessage("Processed records collection so far")
                            .that(msgs.size())
                            .isEqualTo(4);

                    assertThat(consumerSpy).hasCommittedToPartition(topicPartition).atLeastOffset(4);
                });
    }

    private static Flux<String> fromPath(Path path) {
        return Flux.using(() -> Files.lines(path),
                Flux::fromStream,
                BaseStream::close
        );
    }

    @SneakyThrows
    @Test
    void concurrencyTest() {
        //
        var quantity = 100000;
        var expectedQuantity = quantity + 1;
        var consumerRecords = ktu.generateRecords(quantity);
        ktu.send(consumerSpy, consumerRecords);
        log.info("Finished priming records");

        //
        ProgressBar bar = ProgressBarUtils.getNewMessagesBar(log, quantity);

        //
        ConcurrentLinkedQueue<Object> msgs = new ConcurrentLinkedQueue<>();
        var finishedCount = new AtomicInteger(0);
        var completeOrProblem = new CountDownLatch(1);
        var maxConcurrency = MAX_CONCURRENCY;

        rp.react((rec) -> {
            Mono<String> result = Mono.just(StringUtils.msg("result: {}:{}", rec.offset(), rec.value()))
                    .doOnNext(ignore -> {
                        // add that our mono processing has started
                        log.trace("Reactor user function executing: {}", rec);
                        msgs.add(rec);
                        if (msgs.size() > maxConcurrency) {
                            log.error("More records out for processing than max concurrency settings");
                            completeOrProblem.countDown();
                        }
                    })
                    // delay the Mono to simulate a slow async processing time, to cause our concurrency to be reached for sure
                    .delayElement(Duration.ofMillis((int) (100 * Math.random())))
                    .doOnNext(s -> {
                        log.trace("User function after delay. Records pending: {}, removing from out for processing: {}", msgs.size(), rec);
                        boolean removed = msgs.remove(rec);
                        assertWithMessage("record was present and removed")
                                .that(removed).isTrue();

                        int finished = finishedCount.incrementAndGet();
                        if (finished > quantity - 1)
                            completeOrProblem.countDown();

                        bar.step();
                    });
            return result;
        });

        completeOrProblem.await();
        assertThat(msgs.size()).isLessThan(maxConcurrency);

        await()
                // perform testing for at least some time - see fail fast
                .atMost(Duration.ofSeconds(2))
                // make sure out for processing recs never exceeds max concurrency
                .failFast("max concurrency exceeded", () -> {
                    return msgs.size() > maxConcurrency;
                })
                .untilAsserted(() -> {
                    assertWithMessage("Number of completed messages")
                            .that(finishedCount.get()).isEqualTo(expectedQuantity);

                    assertThat(consumerSpy).hasCommittedToPartition(topicPartition).offset(expectedQuantity);
                });

        bar.close();
    }
}
