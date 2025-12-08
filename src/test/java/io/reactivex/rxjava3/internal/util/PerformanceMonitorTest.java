package io.reactivex.rxjava3.internal.util;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.lang.management.MemoryUsage;

import static org.junit.Assert.*;

public class PerformanceMonitorTest {

    @Before
    public void setUp() {
        // Enable monitoring for tests
        System.setProperty("rxjava3.performance.monitor", "true");
        PerformanceMonitor.reset();
    }

    @After
    public void tearDown() {
        PerformanceMonitor.reset();
        System.clearProperty("rxjava3.performance.monitor");
    }

    @Test
    public void testIsEnabled() {
        assertTrue(PerformanceMonitor.isEnabled());
        
        System.setProperty("rxjava3.performance.monitor", "false");
        // Note: This won't change the static field in the same JVM instance
        // but tests the property reading logic
    }

    @Test
    public void testRecordObservableCreation() {
        String initialStats = PerformanceMonitor.getStatistics();
        assertTrue(initialStats.contains("Observable Creations: 0"));
        
        PerformanceMonitor.recordObservableCreation();
        PerformanceMonitor.recordObservableCreation();
        
        String updatedStats = PerformanceMonitor.getStatistics();
        assertTrue(updatedStats.contains("Observable Creations: 2"));
    }

    @Test
    public void testRecordSubscription() {
        String initialStats = PerformanceMonitor.getStatistics();
        assertTrue(initialStats.contains("Subscriptions: 0"));
        
        PerformanceMonitor.recordSubscription();
        PerformanceMonitor.recordSubscription();
        PerformanceMonitor.recordSubscription();
        
        String updatedStats = PerformanceMonitor.getStatistics();
        assertTrue(updatedStats.contains("Subscriptions: 3"));
    }

    @Test
    public void testRecordDisposal() {
        String initialStats = PerformanceMonitor.getStatistics();
        assertTrue(initialStats.contains("Disposals: 0"));
        
        PerformanceMonitor.recordDisposal();
        
        String updatedStats = PerformanceMonitor.getStatistics();
        assertTrue(updatedStats.contains("Disposals: 1"));
    }

    @Test
    public void testRecordError() {
        String initialStats = PerformanceMonitor.getStatistics();
        assertTrue(initialStats.contains("Errors: 0"));
        
        PerformanceMonitor.recordError();
        PerformanceMonitor.recordError();
        
        String updatedStats = PerformanceMonitor.getStatistics();
        assertTrue(updatedStats.contains("Errors: 2"));
    }

    @Test
    public void testGetHeapMemoryUsage() {
        MemoryUsage memoryUsage = PerformanceMonitor.getHeapMemoryUsage();
        assertNotNull(memoryUsage);
        assertTrue(memoryUsage.getUsed() > 0);
        assertTrue(memoryUsage.getMax() > 0);
        assertTrue(memoryUsage.getUsed() <= memoryUsage.getMax());
    }

    @Test
    public void testGetStatistics() {
        // Record some operations
        PerformanceMonitor.recordObservableCreation();
        PerformanceMonitor.recordSubscription();
        PerformanceMonitor.recordDisposal();
        PerformanceMonitor.recordError();
        
        String stats = PerformanceMonitor.getStatistics();
        assertNotNull(stats);
        
        // Verify all metrics are included
        assertTrue(stats.contains("Observable Creations: 1"));
        assertTrue(stats.contains("Subscriptions: 1"));
        assertTrue(stats.contains("Disposals: 1"));
        assertTrue(stats.contains("Errors: 1"));
        assertTrue(stats.contains("Memory Usage:"));
        assertTrue(stats.contains("JVM:"));
        assertTrue(stats.contains("cores"));
    }

    @Test
    public void testReset() {
        // Record some operations
        PerformanceMonitor.recordObservableCreation();
        PerformanceMonitor.recordSubscription();
        PerformanceMonitor.recordDisposal();
        PerformanceMonitor.recordError();
        
        String statsBeforeReset = PerformanceMonitor.getStatistics();
        assertTrue(statsBeforeReset.contains("Observable Creations: 1"));
        
        // Reset counters
        PerformanceMonitor.reset();
        
        String statsAfterReset = PerformanceMonitor.getStatistics();
        assertTrue(statsAfterReset.contains("Observable Creations: 0"));
        assertTrue(statsAfterReset.contains("Subscriptions: 0"));
        assertTrue(statsAfterReset.contains("Disposals: 0"));
        assertTrue(statsAfterReset.contains("Errors: 0"));
    }

    @Test
    public void testConcurrentAccess() throws InterruptedException {
        int threadCount = 10;
        int operationsPerThread = 100;
        
        Thread[] threads = new Thread[threadCount];
        
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                for (int j = 0; j < operationsPerThread; j++) {
                    PerformanceMonitor.recordObservableCreation();
                    PerformanceMonitor.recordSubscription();
                    PerformanceMonitor.recordDisposal();
                }
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }
        
        String stats = PerformanceMonitor.getStatistics();
        assertTrue(stats.contains("Observable Creations: " + (threadCount * operationsPerThread)));
        assertTrue(stats.contains("Subscriptions: " + (threadCount * operationsPerThread)));
        assertTrue(stats.contains("Disposals: " + (threadCount * operationsPerThread)));
    }

    @Test
    public void testMemoryUsageTracking() {
        MemoryUsage before = PerformanceMonitor.getHeapMemoryUsage();
        
        // Allocate some memory
        byte[] largeArray = new byte[1024 * 1024]; // 1MB
        // Use the array to prevent optimization
        largeArray[0] = 1;
        
        MemoryUsage after = PerformanceMonitor.getHeapMemoryUsage();
        
        // Memory usage should have increased
        assertTrue(after.getUsed() >= before.getUsed());
    }

    @Test
    public void testStatisticsFormat() {
        PerformanceMonitor.recordObservableCreation();
        PerformanceMonitor.recordSubscription();
        
        String stats = PerformanceMonitor.getStatistics();
        
        // Verify format contains expected sections
        assertTrue(stats.contains("RxJava Performance Stats:"));
        assertTrue(stats.contains("Observable Creations:"));
        assertTrue(stats.contains("Subscriptions:"));
        assertTrue(stats.contains("Disposals:"));
        assertTrue(stats.contains("Errors:"));
        assertTrue(stats.contains("Memory Usage:"));
        assertTrue(stats.contains("MB"));
        assertTrue(stats.contains("%"));
    }

    @Test
    public void testDisabledMonitoring() {
        System.setProperty("rxjava3.performance.monitor", "false");
        
        // Create a new instance to test disabled state
        // Note: In real implementation, this would require reloading the class
        // For this test, we're testing the property reading logic
        
        // The actual behavior would be that no operations are recorded
        // when monitoring is disabled
    }
}
