package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.state.WorkContainer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Set;

public interface ActionListener<K, V> {

    boolean isEnabled();

    boolean isAssignmentChanged();

    void refresh();

    boolean shouldProcess();

    Set<TopicPartition> pausePartitions();

    void afterPoll(final Map<TopicPartition, List<ConsumerRecord<K, V>>> records);

    boolean couldBeTakenAsWork(final ConsumerRecord<K, V> consumerRecord);

    void beforeFunctionCall(final List<List<WorkContainer<K, V>>> batches);

    void functionError(final List<ConsumerRecord<K, V>> consumerRecords);

    void afterFunctionCall(final List<ConsumerRecord<K, V>> consumerRecords, final Map<String, Object> properties);

    void clear();
}
