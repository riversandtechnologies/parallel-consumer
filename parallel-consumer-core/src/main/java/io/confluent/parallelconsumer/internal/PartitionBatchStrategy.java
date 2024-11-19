package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import com.google.common.collect.ListMultimap;
import io.confluent.parallelconsumer.ParallelConsumerOptions;
import io.confluent.parallelconsumer.state.ShardKey;
import io.confluent.parallelconsumer.state.WorkContainer;

import java.util.List;

public abstract class PartitionBatchStrategy<K, V> {
    private ParallelConsumerOptions<K, V> options;

    public ParallelConsumerOptions<K, V> getOptions() {
        return options;
    }

    void setOptions(final ParallelConsumerOptions<K, V> options) {
        this.options = options;
    }

    public abstract <K, V> List<List<WorkContainer<K, V>>> partitionBatch(ListMultimap<ShardKey, WorkContainer<K, V>> sourceCollection);
}
