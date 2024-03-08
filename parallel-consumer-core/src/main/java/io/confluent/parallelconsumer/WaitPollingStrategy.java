package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

public interface WaitPollingStrategy {
    void execute(int polledSize);
}
