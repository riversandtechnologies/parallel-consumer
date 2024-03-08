package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ParallelConsumerOptions;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import pl.tlinkowski.unij.api.UniMaps;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Delegate for {@link KafkaConsumer}
 */
@Slf4j
public class ConsumerManager<K, V> {
    private final Consumer<K, V> consumer;
    private final ParallelConsumerOptions<K, V> consumerOptions;
    private final ActionListeners<K, V> actionListeners;
    private final AtomicBoolean pollingBroker = new AtomicBoolean(false);

    /**
     * Since Kakfa 2.7, multi-threaded access to consumer group metadata was blocked, so before and after polling, save
     * a copy of the metadata.
     *
     * @since 2.7.0
     */
    private ConsumerGroupMetadata metaCache;

    private volatile int pausedPartitionSizeCache = 0;

    private int erroneousWakups = 0;
    private int correctPollWakeups = 0;
    private int noWakeups = 0;
    private boolean commitRequested;

    public ConsumerManager(AbstractParallelEoSStreamProcessor<K, V> apc) {
        this.consumerOptions = apc.getOptions();
        this.consumer = apc.getOptions().getConsumer();
        this.actionListeners = apc.getActionListeners();
    }

    ConsumerRecords<K, V> poll(Duration requestedLongPollTimeout) {
        Duration timeoutToUse = requestedLongPollTimeout;
        ConsumerRecords<K, V> records;
        try {
            if (commitRequested) {
                log.debug("Commit requested, so will not long poll as need to perform the commit");
                timeoutToUse = Duration.ofMillis(1);// disable long poll, as commit needs performing
                commitRequested = false;
            }
            pollingBroker.set(true);
            updateCache();
            log.debug("Poll starting with timeout: {}", timeoutToUse);
            records = pollWithActionListener(timeoutToUse);
            log.debug("Poll completed normally (after timeout of {}) and returned {}...", timeoutToUse, records.count());
            updateCache();
        } catch (WakeupException w) {
            correctPollWakeups++;
            log.debug("Awoken from broker poll");
            log.trace("Wakeup caller is:", w);
            records = new ConsumerRecords<>(UniMaps.of());
        } catch (IllegalStateException ex) {
            log.error("Failed to poll from broker", ex);
            records = new ConsumerRecords<>(UniMaps.of());
        } finally {
            pollingBroker.set(false);
        }

        if (consumerOptions.getWaitPollingStrategy() != null) {
            consumerOptions.getWaitPollingStrategy().execute(records.count());
        }

        return records;
    }

    private ConsumerRecords<K, V> pollWithActionListener(final Duration timeoutToUse) {
        ConsumerRecords<K, V> consumerRecords = null;
        Set<TopicPartition> assignment = consumer.assignment();
        actionListeners.refresh();
        if (actionListeners.shouldPoll()) {
            Set<TopicPartition> refreshedAssignment = consumer.assignment();
            if (!refreshedAssignment.isEmpty() && assignment.equals(refreshedAssignment)) {
                Set<TopicPartition> pausedPartitions = actionListeners.pausePartitions();
                ConsumerRecords<K, V> partitionRecords = consumer.poll(timeoutToUse);
                Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();
                for (final TopicPartition pollTopicPartition : partitionRecords.partitions()) {
                    records.put(pollTopicPartition, new ArrayList<>(partitionRecords.records(pollTopicPartition)));
                }
                consumerRecords = actionListeners.afterPoll(records);
                consumer.resume(pausedPartitions);
            }

            if (consumerRecords != null) {
                return consumerRecords;
            }
        }
        return new ConsumerRecords<>(UniMaps.of());
    }

    protected void updateCache() {
        metaCache = consumer.groupMetadata();
        pausedPartitionSizeCache = consumer.paused().size();
    }

    /**
     * Wakes up the consumer, but only if it's polling.
     * <p>
     * Otherwise, we can interrupt other operations like {@link KafkaConsumer#commitSync()}.
     */
    public void wakeup() {
        // boolean reduces the chances of a mis-timed call to wakeup, but doesn't prevent all spurious wake up calls to other methods like #commit
        // if the call to wakeup happens /after/ the check for a wake up state inside #poll, then the next call will through the wake up exception (i.e. #commit)
        if (pollingBroker.get()) {
            log.debug("Waking up consumer");
            consumer.wakeup();
        }
    }

    public void commitSync(final Map<TopicPartition, OffsetAndMetadata> offsetsToSend) {
        // we dont' want to be woken up during a commit, only polls
        boolean inProgress = true;
        noWakeups++;
        while (inProgress) {
            try {
                consumer.commitSync(offsetsToSend);
                inProgress = false;
            } catch (WakeupException w) {
                log.debug("Got woken up, retry. errors: " + erroneousWakups + " none: " + noWakeups + " correct:" + correctPollWakeups, w);
                erroneousWakups++;
            }
        }
    }

    public void commitAsync(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetCommitCallback callback) {
        // we dont' want to be woken up during a commit, only polls
        boolean inProgress = true;
        noWakeups++;
        while (inProgress) {
            try {
                consumer.commitAsync(offsets, callback);
                inProgress = false;
            } catch (WakeupException w) {
                log.debug("Got woken up, retry. errors: " + erroneousWakups + " none: " + noWakeups + " correct:" + correctPollWakeups, w);
                erroneousWakups++;
            }
        }
    }

    public ConsumerGroupMetadata groupMetadata() {
        return metaCache;
    }

    public void close(final Duration defaultTimeout) {
        consumer.close(defaultTimeout);
    }

    public Set<TopicPartition> assignment() {
        return consumer.assignment();
    }

    public void pause(final Set<TopicPartition> assignment) {
        consumer.pause(assignment);
    }

    public Set<TopicPartition> paused() {
        return consumer.paused();
    }

    public int getPausedPartitionSize() {
        return pausedPartitionSizeCache;
    }

    public void resume(final Set<TopicPartition> pausedTopics) {
        consumer.resume(pausedTopics);
    }

    public void onCommitRequested() {
        this.commitRequested = true;
    }
}
