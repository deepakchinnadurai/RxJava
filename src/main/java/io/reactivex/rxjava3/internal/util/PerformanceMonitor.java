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

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;

/**
 * Performance monitoring utility for RxJava operations.
 * Provides lightweight metrics collection for debugging and optimization.
 * 
 * @since 3.0.0
 */
public final class PerformanceMonitor {
    
    private static final boolean ENABLED = Boolean.getBoolean("rxjava3.performance.monitor");
    private static final MemoryMXBean MEMORY_BEAN = ManagementFactory.getMemoryMXBean();
    
    private static final LockFreeCounter observableCreations = new LockFreeCounter();
    private static final LockFreeCounter subscriptions = new LockFreeCounter();
    private static final LockFreeCounter disposals = new LockFreeCounter();
    private static final LockFreeCounter errors = new LockFreeCounter();
    
    private PerformanceMonitor() {
        throw new IllegalStateException("No instances!");
    }
    
    /**
     * Records an Observable creation event.
     */
    public static void recordObservableCreation() {
        if (ENABLED) {
            observableCreations.increment();
        }
    }
    
    /**
     * Records a subscription event.
     */
    public static void recordSubscription() {
        if (ENABLED) {
            subscriptions.increment();
        }
    }
    
    /**
     * Records a disposal event.
     */
    public static void recordDisposal() {
        if (ENABLED) {
            disposals.increment();
        }
    }
    
    /**
     * Records an error event.
     */
    public static void recordError() {
        if (ENABLED) {
            errors.increment();
        }
    }
    
    /**
     * Gets current memory usage information.
     * 
     * @return MemoryUsage snapshot
     */
    public static MemoryUsage getHeapMemoryUsage() {
        return MEMORY_BEAN.getHeapMemoryUsage();
    }
    
    /**
     * Gets performance statistics as a formatted string.
     * 
     * @return performance statistics
     */
    public static String getStatistics() {
        if (!ENABLED) {
            return "Performance monitoring disabled. Enable with -Drxjava3.performance.monitor=true";
        }
        
        MemoryUsage heap = getHeapMemoryUsage();
        long usedMB = heap.getUsed() / (1024 * 1024);
        long maxMB = heap.getMax() / (1024 * 1024);
        
        if (!ENABLED) {
            return "Performance monitoring disabled. Enable with -Drxjava3.performance.monitor=true";
        }
        
        return String.format(
            "RxJava Performance Stats:\n" +
            "  Observable Creations: %d\n" +
            "  Subscriptions: %d\n" +
            "  Disposals: %d\n" +
            "  Errors: %d\n" +
            "  Memory Usage: %d/%d MB (%.1f%%)\n" +
            "  JVM: %s (%d cores)\n" +
            "  Contention Stats: %s",
            observableCreations.get(),
            subscriptions.get(),
            disposals.get(),
            errors.get(),
            usedMB,
            maxMB,
            (double) heap.getUsed() / heap.getMax() * 100,
            JvmOptimizations.isHotSpotJvm() ? "HotSpot" : "Other",
            JvmOptimizations.getCpuCores(),
            subscriptions.getContentionStats()
        );
    }
    
    /**
     * Resets all performance counters.
     */
    public static void reset() {
        if (ENABLED) {
            observableCreations.reset();
            subscriptions.reset();
            disposals.reset();
            errors.reset();
        }
    }
    
    /**
     * Checks if performance monitoring is enabled.
     * 
     * @return true if monitoring is enabled
     */
    public static boolean isEnabled() {
        return ENABLED;
    }
}
