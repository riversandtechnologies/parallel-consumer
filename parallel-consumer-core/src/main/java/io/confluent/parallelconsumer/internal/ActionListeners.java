package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2023 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ActionListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActionListeners<K, V> {
    private final List<ActionListener<K, V>> actionListeners = new ArrayList<>();
    private final Map<TopicPartition, List<ConsumerRecord<K, V>>> bufferRecords = new HashMap<>();

    public void refresh() {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.refresh();
        }
    }

    public boolean shouldPoll(final TopicPartition pollTopicPartition) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            if (!actionListener.shouldPoll(pollTopicPartition)) {
                return false;
            }
        }
        return true;
    }

    public ConsumerRecords<K, V> afterPoll(final Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.afterPoll(records);
        }
        return new ConsumerRecords<K, V>(records);
    }

    public void registerListener(ActionListener<K, V> actionListener) {
        if (actionListener != null && actionListener.isEnabled()) {
            actionListeners.add(actionListener);
        }
    }
}
