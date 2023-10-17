package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public interface ActionListener<K, V> {

    void refresh();

    public boolean shouldPoll(final TopicPartition pollTopicPartition);

    public void afterPoll(final Map<TopicPartition, List<ConsumerRecord<K, V>>> records);

    boolean isEnabled();
}
