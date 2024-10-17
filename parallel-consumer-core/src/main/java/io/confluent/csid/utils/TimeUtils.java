package io.confluent.csid.utils;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import lombok.Builder;
import lombok.SneakyThrows;
import lombok.Value;
import lombok.experimental.UtilityClass;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Callable;


@UtilityClass
public class TimeUtils {
    private static final Logger log = LogManager.getLogger(TimeUtils.class);

    public Clock getClock() {
        return Clock.systemUTC();
    }

    @SneakyThrows
    public static <RESULT> RESULT time(final Callable<RESULT> func) {
        return timeWithMeta(func).getResult();
    }

    @SneakyThrows
    public static <RESULT> TimeResult<RESULT> timeWithMeta(final Callable<? extends RESULT> func) {
        long start = System.currentTimeMillis();
        TimeResult.TimeResultBuilder<RESULT> timer = TimeResult.<RESULT>builder().startMs(start);
        RESULT call = func.call();
        timer.result(call);
        long end = System.currentTimeMillis();
        long elapsed = end - start;
        timer.endMs(end);
        log.trace("Function took {}", Duration.ofMillis(elapsed));
        return timer.build();
    }

    @Builder
    @Value
    public static class TimeResult<RESULT> {
        long startMs;
        long endMs;
        RESULT result;

        public Duration getElapsed() {
            return Duration.ofMillis(endMs - startMs);
        }
    }
}
