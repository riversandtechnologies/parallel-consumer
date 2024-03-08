package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ActionListener;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class ActionListeners<K, V> {
    private final List<ActionListener<K, V>> actionListeners = new ArrayList<>();
    private final Consumer<K, V> consumer;

    public ActionListeners(Consumer<K, V> consumer) {
        this.consumer = consumer;
    }

    public void refresh() {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.refresh();
        }
    }

    public boolean shouldPoll() {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            if (!actionListener.shouldPoll()) {
                return false;
            }
        }
        return true;
    }

    public Set<TopicPartition> pausePartitions() {
        Set<TopicPartition> pausedPartitions = new HashSet<>();
        for (final ActionListener<K, V> actionListener : actionListeners) {
            pausedPartitions.addAll(actionListener.pausePartitions());
        }
        consumer.pause(pausedPartitions);
        return pausedPartitions;
    }

    public ConsumerRecords<K, V> afterPoll(final Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.afterPoll(records);
        }
        return new ConsumerRecords<K, V>(records);
    }

    public void beforeFunctionCall(final TopicPartition pollTopicPartition) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.beforeFunctionCall(pollTopicPartition);
        }
    }

    public void afterFunctionCall(final TopicPartition pollTopicPartition) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.afterFunctionCall(pollTopicPartition);
        }
    }

    public boolean isEmpty() {
        return actionListeners.isEmpty();
    }

    void registerListener(ActionListener<K, V> actionListener) {
        if (actionListener != null && actionListener.isEnabled()) {
            actionListeners.add(actionListener);
        }
    }
}
