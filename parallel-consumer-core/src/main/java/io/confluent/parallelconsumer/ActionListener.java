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

public interface ActionListener<K, V> {

    void refresh(AbstractParallelEoSStreamProcessor<K, V> apc, Consumer<K, V> consumer);

    public boolean shouldPoll(final Consumer<K, V> consumer, final TopicPartition pollTopicPartition);

    public void afterPoll(final Consumer<K, V> consumer, final Map<TopicPartition, List<ConsumerRecord<K, V>>> records);

    public void action();

    boolean isEnabled();
}
