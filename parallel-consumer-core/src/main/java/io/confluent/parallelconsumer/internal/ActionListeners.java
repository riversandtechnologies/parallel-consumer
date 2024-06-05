package io.confluent.parallelconsumer.internal;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import io.confluent.parallelconsumer.ActionListener;
import lombok.Getter;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;

import java.util.*;

public class ActionListeners<K, V> {
    private final List<ActionListener<K, V>> actionListeners = new ArrayList<>();
    private final Consumer<K, V> consumer;
    @Getter
    private boolean isPausing;

    public ActionListeners(Consumer<K, V> consumer) {
        this.consumer = consumer;
    }

    public boolean isAssignmentChanged() {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            if (actionListener.isAssignmentChanged()) {
                return true;
            }
        }
        return false;
    }

    public void refresh() {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.refresh();
        }
    }

    public boolean shouldProcess() {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            if (!actionListener.shouldProcess()) {
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
        if (pausedPartitions.isEmpty()) {
            isPausing = false;
        } else {
            consumer.pause(pausedPartitions);
            isPausing = true;
        }
        return pausedPartitions;
    }

    public ConsumerRecords<K, V> afterPoll(final Map<TopicPartition, List<ConsumerRecord<K, V>>> records) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.afterPoll(records);
        }
        return new ConsumerRecords<K, V>(records);
    }

    public boolean couldBeTakenAsWork(final ConsumerRecord<K, V> consumerRecord) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            if (!actionListener.couldBeTakenAsWork(consumerRecord)) {
                return false;
            }
        }
        return true;
    }

    public void beforeFunctionCall(final ConsumerRecord<K, V> consumerRecord) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.beforeFunctionCall(consumerRecord);
        }
    }

    public void functionError(final ConsumerRecord<K, V> consumerRecord) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.functionError(consumerRecord);
        }
    }

    public void afterFunctionCall(final ConsumerRecord<K, V> consumerRecord) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.afterFunctionCall(consumerRecord);
        }
    }

    public void clear() {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.clear();
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
