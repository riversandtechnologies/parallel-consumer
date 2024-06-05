package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

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

    void beforeFunctionCall(final ConsumerRecord<K, V> consumerRecord);

    void functionError(final ConsumerRecord<K, V> consumerRecord);

    void afterFunctionCall(final ConsumerRecord<K, V> consumerRecord);

    void clear();
}
