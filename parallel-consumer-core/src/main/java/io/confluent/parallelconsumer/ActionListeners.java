package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Syndigo
 */

import org.apache.kafka.clients.consumer.ConsumerRecords;

import java.util.ArrayList;
import java.util.List;

public class ActionListeners<K, V> {
    private List<ActionListener<K, V>> actionListeners = new ArrayList<>();

    public boolean shouldPoll() {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            if (!actionListener.shouldPoll()) {
                return false;
            }
        }
        return true;
    }

    public void afterPoll(final ConsumerRecords<K, V> records) {
        for (final ActionListener<K, V> actionListener : actionListeners) {
            actionListener.afterPoll(records);
        }
    }

    public void registerListener(ActionListener<K, V> actionListener) {
        if (actionListener.isEnabled()) {
            actionListeners.add(actionListener);
        }
    }
}
