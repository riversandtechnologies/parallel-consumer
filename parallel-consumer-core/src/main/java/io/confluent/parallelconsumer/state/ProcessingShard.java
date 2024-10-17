package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import com.google.common.collect.LinkedListMultimap;
import com.google.common.collect.ListMultimap;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import io.confluent.parallelconsumer.internal.ActionListeners;
import io.confluent.parallelconsumer.internal.RateLimiter;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static io.confluent.csid.utils.BackportUtils.toSeconds;
import static io.confluent.csid.utils.JavaUtils.isGreaterThan;
import static io.confluent.csid.utils.StringUtils.msg;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.KEY_BATCH_EXCLUSIVE;
import static io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder.UNORDERED;
import static lombok.AccessLevel.PRIVATE;

/**
 * Models the queue of work to be processed, based on the {@link ProcessingOrder} modes.
 *
 * @author Antony Stubbs
 * @see ShardManager
 */
@RequiredArgsConstructor
public class ProcessingShard<K, V> {
    private static final Logger log = LogManager.getLogger(ProcessingShard.class);

    /**
     * Map of offset to WorkUnits.
     * <p>
     * Uses a ConcurrentSkipListMap instead of a TreeMap as under high pressure there appears to be some concurrency
     * errors (missing WorkContainers). This is addressed in PR#270.
     * <p>
     * Is a Map because need random access into collection, as records don't always complete in order (i.e. UNORDERED
     * mode).
     */
    @Getter
    private final NavigableMap<String, WorkContainer<K, V>> entries = new ConcurrentSkipListMap<>();


    @Getter(PRIVATE)
    private final ShardKey key;

    private final ParallelConsumerOptions<?, ?> options;

    private final PartitionStateManager<K, V> pm;

    private final RateLimiter slowWarningRateLimit = new RateLimiter(5);

    private final AtomicLong availableWorkContainerCnt = new AtomicLong(0);

    public boolean workIsWaitingToBeProcessed() {
        return entries.values().parallelStream()
                .anyMatch(kvWorkContainer -> kvWorkContainer.isAvailableToTakeAsWork());
    }

    public void addWorkContainer(WorkContainer<K, V> wc) {
        String key = getWcKey(wc);
        if (entries.containsKey(key)) {
            log.debug("Entry for {} already exists in shard queue, dropping record", wc);
        } else {
            entries.put(key, wc);
            availableWorkContainerCnt.incrementAndGet();
        }
    }

    private String getWcKey(WorkContainer<K, V> wc) {
        return wc.getTopicPartition() + "-" + wc.offset();
    }

    private String getWcKey(ConsumerRecord<K, V> cr) {
        return new TopicPartition(cr.topic(), cr.partition()) + "-" + cr.offset();
    }

    public void onSuccess(WorkContainer<K, V> wc) {
        // remove work from shard's queue
        entries.remove(getWcKey(wc));
    }

    public void onFailure() {
        // increase available cnt first to let retry expired calculated later
        availableWorkContainerCnt.incrementAndGet();
    }


    public boolean isEmpty() {
        return entries.isEmpty();
    }

    public long getCountOfWorkAwaitingSelection() {
        return availableWorkContainerCnt.get();
    }

    public long getCountOfWorkTracked() {
        return entries.size();
    }

    public long getCountWorkInFlight() {
        return entries.values().stream()
                .filter(WorkContainer::isInFlight)
                .count();
    }

    public WorkContainer<K, V> remove(ConsumerRecord<K, V> consumerRecord) {
        // from onPartitionsRemoved callback, need to deduce the available worker count for the revoked partition
        String key = getWcKey(consumerRecord);
        WorkContainer<K, V> toRemovedWorker = entries.get(key);
        if (toRemovedWorker != null && toRemovedWorker.isAvailableToTakeAsWork()) {
            dcrAvailableWorkContainerCntByDelta(1);
        }
        return entries.remove(key);
    }


    // remove staled WorkContainer otherwise when the partition is reassigned, the staled messages will:
    // 1. block the new work containers to be picked and processed
    // 2. will cause the consumer to paused consuming new messages indefinitely
    public List<WorkContainer<K, V>> removeStaleWorkContainersFromShard() {
        List<WorkContainer<K, V>> staleContainers = new ArrayList<>();
        this.entries.entrySet()
                .removeIf(entry -> {
                    WorkContainer<K, V> workContainer = entry.getValue();
                    boolean isStale = isWorkContainerStale(workContainer);
                    if (isStale) {
                        // decrease the AvailableWorkContainerCnt and collect stale containers
                        dcrAvailableWorkContainerCntByDelta(1);
                        staleContainers.add(workContainer);
                    }
                    return isStale;
                });
        return staleContainers;
    }

    ListMultimap<ShardKey, WorkContainer<K, V>> getWorkIfAvailable(int workToGetDelta, ActionListeners<K, V> actionListeners) {
        log.trace("Looking for work on shardQueueEntry: {}", getKey());

        var slowWork = new HashSet<WorkContainer<?, ?>>();
        ListMultimap<ShardKey, WorkContainer<K, V>> workTaken = LinkedListMultimap.create();

        var iterator = entries.entrySet().iterator();
        int keyBatchSize = 0;
        while (workTaken.size() < workToGetDelta && iterator.hasNext()) {
            var workContainer = iterator.next().getValue();

            if (actionListeners.couldBeTakenAsWork(workContainer.getCr())) {
                if (pm.couldBeTakenAsWork(workContainer)) {
                    if (workContainer.isAvailableToTakeAsWork()) {
                        if (!options.getOrdering().equals(KEY_BATCH_EXCLUSIVE) && (keyBatchSize < 1 && getCountWorkInFlight() < 1)) {
                            log.trace("Taking {} as work", workContainer);
                            workContainer.onQueueingForExecution();
                            workTaken.put(key, workContainer);
                            keyBatchSize++;
                        } else if (options.getOrdering().equals(KEY_BATCH_EXCLUSIVE) &&
                                (keyBatchSize < options.getBatchSize() && getCountWorkInFlight() < options.getBatchSize())) {
                            log.trace("Taking {} as work", workContainer);

                            workContainer.onQueueingForExecution();
                            workTaken.put(key, workContainer);
                            keyBatchSize++;
                        } else {
                            break;
                        }
                    } else {
                        log.trace("Skipping {} as work, not available to take as work", workContainer);
                        addToSlowWorkMaybe(slowWork, workContainer);
                    }

                    if (isOrderRestricted()) {
                        // can't take any more work from this shard, due to ordering restrictions
                        // processing blocked on this shard, continue to next shard
                        log.trace("Processing by {}, so have cannot get more messages on this ({}) shardEntry.", this.options.getOrdering(), getKey());
                        break;
                    } else if (options.getOrdering().equals(KEY_BATCH_EXCLUSIVE) && (keyBatchSize >= options.getBatchSize())) {
                        // can't take any more work from this shard, due to ordering restrictions
                        // processing blocked on this shard, continue to next shard
                        log.trace("Processing by {}, so have cannot get more messages on this ({}) shardEntry.", this.options.getOrdering(), getKey());
                        break;
                    }
                } else {
                    // break, assuming all work in this shard, is for the same ShardKey, which is always on the same
                    //  partition (regardless of ordering mode - KEY, PARTITION or UNORDERED (which is parallel PARTITIONs)),
                    //  so no point continuing shard scanning. This only isn't true if a non standard partitioner produced the
                    //  recrods of the same key to different partitions. In which case, there's no way PC can make sure all
                    //  records of that belong to the shard are able to even be processed by the same PC instance, so it doesn't
                    //  matter.
                    log.trace("Partition for shard {} is blocked for work taking, stopping shard scan", this);
                    break;
                }
            } else {
                break;
            }
        }

        if (workTaken.size() == workToGetDelta) {
            log.trace("Work taken ({}) exceeds max ({})", workTaken.size(), workToGetDelta);
        }

        logSlowWork(slowWork);

        dcrAvailableWorkContainerCntByDelta(workTaken.size());

        return workTaken;
    }

    private void logSlowWork(Set<WorkContainer<?, ?>> slowWork) {
        // log
        if (!slowWork.isEmpty()) {
            List<String> slowTopics = slowWork.parallelStream()
                    .map(x -> x.getTopicPartition().toString()).distinct()
                    .collect(Collectors.toList());
            slowWarningRateLimit.performIfNotLimited(() ->
                    log.warn("Warning: {} records in the queue have been waiting longer than {}s for following topics {}.",
                            slowWork.size(), toSeconds(options.getThresholdForTimeSpendInQueueWarning()), slowTopics));
        }
    }

    private void addToSlowWorkMaybe(Set<WorkContainer<?, ?>> slowWork, WorkContainer<?, ?> workContainer) {
        Duration timeInFlight = workContainer.getTimeInFlight();
        Duration slowThreshold = options.getThresholdForTimeSpendInQueueWarning();
        if (isGreaterThan(timeInFlight, slowThreshold)) {
            if (!slowWork.contains(workContainer)) {
                pm.incrementSlowWorkCounter(workContainer.getTopicPartition());
            }
            slowWork.add(workContainer);
            if (log.isTraceEnabled()) {
                log.trace("Work has spent over " + slowThreshold + " in queue! " + cantTakeAsWorkMsg(workContainer, timeInFlight));
            }
        } else {
            if (log.isTraceEnabled()) {
                log.trace(cantTakeAsWorkMsg(workContainer, timeInFlight));
            }
        }
    }

    private static String cantTakeAsWorkMsg(WorkContainer<?, ?> workContainer, Duration timeInFlight) {
        var msgTemplate = "Can't take as work: Work ({}). Must all be true: Delay passed= {}. Is not in flight= {}. Has not succeeded already= {}. Time spent in execution queue: {}.";
        return msg(msgTemplate, workContainer, workContainer.isDelayPassed(), workContainer.isNotInFlight(), !workContainer.isUserFunctionSucceeded(), timeInFlight);
    }

    private boolean isOrderRestricted() {
        return !(options.getOrdering().equals(UNORDERED) || options.getOrdering().equals(KEY_BATCH_EXCLUSIVE));
    }

    // check if the work container is stale
    private boolean isWorkContainerStale(WorkContainer<K, V> workContainer) {
        return pm.getPartitionState(workContainer).checkIfWorkIsStale(workContainer);
    }

    private void dcrAvailableWorkContainerCntByDelta(int ByNum) {
        availableWorkContainerCnt.getAndAdd(-1 * ByNum);
        // in case of possible race condition
        if (availableWorkContainerCnt.get() < 0L) {
            availableWorkContainerCnt.set(0L);
        }
    }
}
