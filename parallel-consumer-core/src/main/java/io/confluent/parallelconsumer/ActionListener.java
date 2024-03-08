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

    void refresh();

    boolean shouldPoll();

    Set<TopicPartition> pausePartitions();

    void afterPoll(final Map<TopicPartition, List<ConsumerRecord<K, V>>> records);

    void beforeFunctionCall(final TopicPartition pollTopicPartition);

    void afterFunctionCall(final TopicPartition pollTopicPartition);

    boolean isEnabled();
}
