package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.internal.AbstractParallelEoSStreamProcessor;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Map;

public class AbstractActionListener<K, V> implements ActionListener<K, V> {

    @Override
    public void refresh(final AbstractParallelEoSStreamProcessor<K, V> apc, final Consumer<K, V> consumer) {

    }

    @Override
    public boolean shouldPoll(final Consumer<K, V> consumer, final TopicPartition pollTopicPartition) {
        return true;
    }

    @Override
    public void afterPoll(final Consumer<K, V> consumer, final Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {

    }

    @Override
    public void action() {

    }

    @Override
    public boolean isEnabled() {
        return true;
    }
}
