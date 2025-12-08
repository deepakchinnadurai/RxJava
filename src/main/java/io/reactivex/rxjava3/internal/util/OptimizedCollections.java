/*
 * Copyright (c) 2016-present, RxJava Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package io.reactivex.rxjava3.internal.util;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Optimized collection utilities for RxJava internal operations.
 * Provides memory-efficient and performance-optimized collections
 * for common RxJava use cases.
 * 
 * @since 3.0.0
 */
public final class OptimizedCollections {
    
    private OptimizedCollections() {
        throw new IllegalStateException("No instances!");
    }
    
    /**
     * Creates a memory-optimized ArrayList with initial capacity
     * based on expected usage patterns in RxJava operators.
     * 
     * @param <T> the element type
     * @return optimized ArrayList instance
     */
    public static <T> ArrayList<T> createOptimizedList() {
        return new ArrayList<>(16);
    }
    
    /**
     * Creates a memory-optimized ArrayList with specified initial capacity.
     * 
     * @param <T> the element type
     * @param expectedSize expected number of elements
     * @return optimized ArrayList instance
     */
    public static <T> ArrayList<T> createOptimizedList(int expectedSize) {
        int capacity = Integer.highestOneBit(expectedSize - 1) << 1;
        return new ArrayList<>(Math.max(capacity, 4));
    }
    
    /**
     * Creates a memory-optimized HashMap with initial capacity
     * based on expected usage patterns in RxJava operators.
     * 
     * @param <K> the key type
     * @param <V> the value type
     * @return optimized HashMap instance
     */
    public static <K, V> HashMap<K, V> createOptimizedMap() {
        return new HashMap<>(16, 0.75f);
    }
    
    /**
     * Creates a memory-optimized HashMap with specified expected size.
     * 
     * @param <K> the key type
     * @param <V> the value type
     * @param expectedSize expected number of entries
     * @return optimized HashMap instance
     */
    public static <K, V> HashMap<K, V> createOptimizedMap(int expectedSize) {
        int capacity = (int) Math.ceil(expectedSize / 0.75f);
        return new HashMap<>(capacity, 0.75f);
    }
    
    /**
     * Creates a thread-safe optimized ConcurrentHashMap.
     * 
     * @param <K> the key type
     * @param <V> the value type
     * @return optimized ConcurrentHashMap instance
     */
    public static <K, V> ConcurrentHashMap<K, V> createOptimizedConcurrentMap() {
        return new ConcurrentHashMap<>(16, 0.75f, Runtime.getRuntime().availableProcessors());
    }
    
    /**
     * Creates a thread-safe optimized ConcurrentHashMap with specified expected size.
     * 
     * @param <K> the key type
     * @param <V> the value type
     * @param expectedSize expected number of entries
     * @return optimized ConcurrentHashMap instance
     */
    public static <K, V> ConcurrentHashMap<K, V> createOptimizedConcurrentMap(int expectedSize) {
        int capacity = (int) Math.ceil(expectedSize / 0.75f);
        return new ConcurrentHashMap<>(capacity, 0.75f, Runtime.getRuntime().availableProcessors());
    }
    
    /**
     * Creates an optimized Set implementation based on expected size.
     * 
     * @param <T> the element type
     * @param expectedSize expected number of elements
     * @return optimized Set instance
     */
    public static <T> Set<T> createOptimizedSet(int expectedSize) {
        if (expectedSize <= 4) {
            return new HashSet<>(4, 1.0f);
        } else {
            int capacity = (int) Math.ceil(expectedSize / 0.75f);
            return new HashSet<>(capacity, 0.75f);
        }
    }
    
    /**
     * Checks if a collection is null or empty in an optimized way.
     * 
     * @param collection the collection to check
     * @return true if null or empty
     */
    public static boolean isNullOrEmpty(Collection<?> collection) {
        return collection == null || collection.isEmpty();
    }
    
    /**
     * Gets the size of a collection safely (returns 0 for null).
     * 
     * @param collection the collection
     * @return size or 0 if null
     */
    public static int safeSize(Collection<?> collection) {
        return collection == null ? 0 : collection.size();
    }
}
