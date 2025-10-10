package io.confluent.parallelconsumer;

/*-
 * Copyright (C) 2020-2025 Confluent, Inc.
 */

public interface WaitPollingStrategy {
    void execute(int polledSize);
}
