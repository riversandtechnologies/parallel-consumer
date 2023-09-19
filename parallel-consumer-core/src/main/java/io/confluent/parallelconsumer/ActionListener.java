package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2023 Syndigo
 */

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface ActionListener<K, V> {

    public boolean shouldPoll();

    public void afterPoll(ConsumerRecords<K, V> records);

    public void action();

    boolean isEnabled();
}
