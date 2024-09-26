package io.confluent.parallelconsumer.state;

/*-
 * Copyright (C) 2020-2024 Confluent, Inc.
 */

import com.google.common.base.Strings;
import io.confluent.parallelconsumer.ParallelConsumerOptions.ProcessingOrder;
import lombok.*;
import lombok.experimental.FieldDefaults;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple value class for processing {@link ShardKey}s to make the various key systems type safe and extendable.
 *
 * @author Antony Stubbs
 */
@Getter
@FieldDefaults(makeFinal = true, level = AccessLevel.PRIVATE)
@ToString
@EqualsAndHashCode
public class ShardKey {
    private static final Map<String, String> TOPIC_GROUP_MAP = new ConcurrentHashMap();

    public static ShardKey of(WorkContainer<?, ?> wc, ProcessingOrder ordering) {
        return of(wc.getCr(), ordering);
    }

    public static ShardKey of(ConsumerRecord<?, ?> rec, ProcessingOrder ordering) {
        return switch (ordering) {
            case KEY_BATCH_EXCLUSIVE -> ofKeyBatchExclusive(rec);
            case KEY_EXCLUSIVE -> ofKeyExclusive(rec);
            case KEY_GROUP_EXCLUSIVE -> ofKeyGroupExclusive(rec);
            case KEY -> ofKey(rec);
            case PARTITION, UNORDERED -> ofTopicPartition(rec);
        };
    }

    public static KeyBatchExclusiveOrderedKey ofKeyBatchExclusive(ConsumerRecord<?, ?> rec) {
        return new KeyBatchExclusiveOrderedKey(rec);
    }

    public static KeyExclusiveOrderedKey ofKeyExclusive(ConsumerRecord<?, ?> rec) {
        return new KeyExclusiveOrderedKey(rec);
    }

    public static KeyGroupExclusiveOrderedKey ofKeyGroupExclusive(ConsumerRecord<?, ?> rec) {
        return new KeyGroupExclusiveOrderedKey(rec);
    }

    public static KeyOrderedKey ofKey(ConsumerRecord<?, ?> rec) {
        return new KeyOrderedKey(rec);
    }

    public static ShardKey ofTopicPartition(final ConsumerRecord<?, ?> rec) {
        return new TopicPartitionKey(new TopicPartition(rec.topic(), rec.partition()));
    }

    @Value
    @RequiredArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class KeyBatchExclusiveOrderedKey extends ShardKey {

        /**
         * The key of the record being referenced. Nullable if record is produced with a null key.
         */
        KeyWithEquals key;

        public KeyBatchExclusiveOrderedKey(final ConsumerRecord<?, ?> rec) {
            this(rec.key());
        }

        public KeyBatchExclusiveOrderedKey(final Object key) {
            if (key instanceof KeyWithEquals) {
                this.key = (KeyWithEquals) key;
            } else {
                this.key = new KeyWithEquals(key);
            }
        }
    }

    @Value
    @RequiredArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class KeyExclusiveOrderedKey extends ShardKey {

        /**
         * The key of the record being referenced. Nullable if record is produced with a null key.
         */
        KeyWithEquals key;

        public KeyExclusiveOrderedKey(final ConsumerRecord<?, ?> rec) {
            this(rec.key());
        }

        public KeyExclusiveOrderedKey(final Object key) {
            if (key instanceof KeyWithEquals) {
                this.key = (KeyWithEquals) key;
            } else {
                this.key = new KeyWithEquals(key);
            }
        }
    }

    @Value
    @RequiredArgsConstructor
    @EqualsAndHashCode(callSuper = true)
    public static class KeyGroupExclusiveOrderedKey extends ShardKey {

        String topicGroup;
        /**
         * The key of the record being referenced. Nullable if record is produced with a null key.
         */
        KeyWithEquals key;

        public KeyGroupExclusiveOrderedKey(final ConsumerRecord<?, ?> rec) {
            this(rec.topic(), rec.key());
        }

        public KeyGroupExclusiveOrderedKey(final String topic, final Object key) {
            String topicGroup = TOPIC_GROUP_MAP.get(topic);
            if (Strings.isNullOrEmpty(topicGroup)) {
                if (topic.contains("-")) {
                    String[] delimitedTopic = topic.split("-");
                    if (delimitedTopic.length >= 2) {
                        topicGroup = delimitedTopic[delimitedTopic.length - 1];
                        TOPIC_GROUP_MAP.put(topic, topicGroup);
                    }
                }

                if (Strings.isNullOrEmpty(topicGroup)) {
                    topicGroup = "default";
                }
            }

            this.topicGroup = topicGroup;
            if (key instanceof KeyWithEquals) {
                this.key = (KeyWithEquals) key;
            } else {
                this.key = new KeyWithEquals(key);
            }
        }
    }

    @Value
    @EqualsAndHashCode(callSuper = true)
    public static class KeyOrderedKey extends ShardKey {

        /**
         * Note: We use just the topic name here, and not the partition, so that if we were to receive records from the
         * same key from the partitions we're assigned, they will be put into the same queue.
         */
        TopicPartition topicName;

        /**
         * The key of the record being referenced. Nullable if record is produced with a null key.
         */
        KeyWithEquals key;

        public KeyOrderedKey(final ConsumerRecord<?, ?> rec) {
            this(new TopicPartition(rec.topic(), rec.partition()), rec.key());
        }

        public KeyOrderedKey(final TopicPartition topicPartition, final Object key) {
            if (key instanceof KeyWithEquals) {
                this.key = (KeyWithEquals) key;
            } else {
                this.key = new KeyWithEquals(key);
            }
            this.topicName = topicPartition;
        }
    }

    @Value
    @RequiredArgsConstructor
    public static class KeyWithEquals {
        Object key;

        @Override
        public boolean equals(Object o) {
            if (o == this) return true;
            if (!(o instanceof KeyWithEquals)) return false;
            KeyWithEquals other = (KeyWithEquals) o;
            if (other.key == null && this.key == null) return true;
            if (other.key == null || this.key == null) return false;
            return Objects.deepEquals(this.key, other.key);
        }

        @Override
        public int hashCode() {

            final int PRIME = 59;
            int result = 1;
            result = (result * PRIME);
            if (key == null) {
                result = result + 43;
                return result;
            }
            if (isArray(key)) {
                result = result + arrayHashCode(key);
            } else {
                result = result + key.hashCode();
            }
            return result;
        }


        private int arrayHashCode(Object t) {
            if (t instanceof Object[]) {
                return Arrays.deepHashCode((Object[]) t);
            } else {
                return primitiveArrayHashCode(t, t.getClass().getComponentType());
            }
        }

        /**
         * Copy of {@link Arrays#primitiveArrayHashCode} logic
         *
         * @param a
         * @param cl
         * @return
         */
        private int primitiveArrayHashCode(Object a, Class<?> cl) {
            return
                    (cl == byte.class) ? Arrays.hashCode((byte[]) a) :
                            (cl == int.class) ? Arrays.hashCode((int[]) a) :
                                    (cl == long.class) ? Arrays.hashCode((long[]) a) :
                                            (cl == char.class) ? Arrays.hashCode((char[]) a) :
                                                    (cl == short.class) ? Arrays.hashCode((short[]) a) :
                                                            (cl == boolean.class) ? Arrays.hashCode((boolean[]) a) :
                                                                    (cl == double.class) ? Arrays.hashCode((double[]) a) :
                                                                            // If new primitive types are ever added, this method must be
                                                                            // expanded or we will fail here with ClassCastException.
                                                                            Arrays.hashCode((float[]) a);
        }

        private boolean isArray(Object obj) {
            return obj instanceof Object[] || obj instanceof boolean[] ||
                    obj instanceof byte[] || obj instanceof short[] ||
                    obj instanceof char[] || obj instanceof int[] ||
                    obj instanceof long[] || obj instanceof float[] ||
                    obj instanceof double[];
        }
    }


    @Value
    @EqualsAndHashCode(callSuper = true)
    public static class TopicPartitionKey extends ShardKey {
        TopicPartition topicPartition;
    }

}
