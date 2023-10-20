package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public class AbstractActionListener<K, V> implements ActionListener<K, V> {

    @Override
    public void refresh() {

    }

    @Override
    public boolean shouldPoll(final TopicPartition pollTopicPartition) {
        return true;
    }

    @Override
    public void afterPoll(final Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {

    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
