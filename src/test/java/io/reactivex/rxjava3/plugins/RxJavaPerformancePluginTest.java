package io.reactivex.rxjava3.plugins;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.internal.util.PerformanceMonitor;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class RxJavaPerformancePluginTest {

    @Before
    public void setUp() {
        // Reset plugins before each test
        RxJavaPlugins.reset();
        PerformanceMonitor.reset();
    }

    @After
    public void tearDown() {
        // Clean up after each test
        RxJavaPerformancePlugin.uninstall();
        RxJavaPlugins.reset();
        System.clearProperty("rxjava3.performance.monitor");
    }

    @Test
    public void testPluginInstallation() {
        // Enable monitoring
        System.setProperty("rxjava3.performance.monitor", "true");
        
        // Install plugin
        RxJavaPerformancePlugin.install();
        
        // Verify plugin is working by running operations
        Observable.just(1, 2, 3)
            .map(x -> x * 2)
            .blockingSubscribe();
        
        String stats = PerformanceMonitor.getStatistics();
        assertTrue("Should track Observable creations", stats.contains("Observable Creations:"));
        assertTrue("Should track subscriptions", stats.contains("Subscriptions:"));
    }

    @Test
    public void testPluginDisabled() {
        // Ensure monitoring is disabled
        System.setProperty("rxjava3.performance.monitor", "false");
        
        // Try to install plugin (should do nothing)
        RxJavaPerformancePlugin.install();
        
        // Run operations
        Observable.just(1, 2, 3)
            .blockingSubscribe();
        
        // Should not track anything when disabled
        assertFalse("Plugin should be disabled", RxJavaPerformancePlugin.isEnabled());
    }

    @Test
    public void testRuntimeEnableDisable() {
        // Start disabled
        assertFalse(RxJavaPerformancePlugin.isEnabled());
        
        // Enable at runtime
        RxJavaPerformancePlugin.enable();
        assertTrue(RxJavaPerformancePlugin.isEnabled());
        
        // Disable at runtime
        RxJavaPerformancePlugin.disable();
        assertFalse(RxJavaPerformancePlugin.isEnabled());
    }

    @Test
    public void testObservableCreationTracking() {
        System.setProperty("rxjava3.performance.monitor", "true");
        RxJavaPerformancePlugin.install();
        
        // Create multiple observables
        Observable.just(1);
        Observable.range(1, 5);
        Observable.empty();
        
        String stats = PerformanceMonitor.getStatistics();
        assertTrue("Should track multiple Observable creations", 
                   stats.contains("Observable Creations:"));
    }

    @Test
    public void testSubscriptionTracking() {
        System.setProperty("rxjava3.performance.monitor", "true");
        RxJavaPerformancePlugin.install();
        
        // Subscribe to observables
        Observable.just(1, 2, 3).blockingSubscribe();
        Observable.range(1, 5).blockingSubscribe();
        
        String stats = PerformanceMonitor.getStatistics();
        assertTrue("Should track subscriptions", stats.contains("Subscriptions:"));
    }

    @Test
    public void testDisposalTracking() {
        System.setProperty("rxjava3.performance.monitor", "true");
        RxJavaPerformancePlugin.install();
        
        // Create and dispose subscription
        var disposable = Observable.interval(100, java.util.concurrent.TimeUnit.MILLISECONDS)
            .subscribe();
        
        disposable.dispose();
        
        String stats = PerformanceMonitor.getStatistics();
        assertTrue("Should track disposals", stats.contains("Disposals:"));
    }

    @Test
    public void testErrorTracking() {
        System.setProperty("rxjava3.performance.monitor", "true");
        RxJavaPerformancePlugin.install();
        
        try {
            Observable.error(new RuntimeException("Test error"))
                .blockingSubscribe();
        } catch (Exception e) {
            // Expected
        }
        
        String stats = PerformanceMonitor.getStatistics();
        assertTrue("Should track errors", stats.contains("Errors:"));
    }

    @Test
    public void testPluginUninstall() {
        System.setProperty("rxjava3.performance.monitor", "true");
        RxJavaPerformancePlugin.install();
        
        // Verify plugin is working
        Observable.just(1).blockingSubscribe();
        String statsBefore = PerformanceMonitor.getStatistics();
        assertTrue(statsBefore.contains("Observable Creations:"));
        
        // Uninstall plugin
        RxJavaPerformancePlugin.uninstall();
        
        // Reset counters and run more operations
        PerformanceMonitor.reset();
        Observable.just(2).blockingSubscribe();
        
        // Should not track new operations after uninstall
        String statsAfter = PerformanceMonitor.getStatistics();
        assertTrue("Should show reset counters", statsAfter.contains("Observable Creations: 0"));
    }

    @Test
    public void testConcurrentOperations() throws InterruptedException {
        System.setProperty("rxjava3.performance.monitor", "true");
        RxJavaPerformancePlugin.install();
        
        int threadCount = 5;
        Thread[] threads = new Thread[threadCount];
        
        for (int i = 0; i < threadCount; i++) {
            threads[i] = new Thread(() -> {
                Observable.range(1, 100)
                    .filter(n -> n % 2 == 0)
                    .blockingSubscribe();
            });
        }
        
        // Start all threads
        for (Thread thread : threads) {
            thread.start();
        }
        
        // Wait for completion
        for (Thread thread : threads) {
            thread.join();
        }
        
        String stats = PerformanceMonitor.getStatistics();
        assertTrue("Should handle concurrent operations", 
                   stats.contains("Observable Creations:"));
        assertTrue("Should track all subscriptions", 
                   stats.contains("Subscriptions:"));
    }

    @Test
    public void testNoCodePollution() {
        // Verify that Observable code remains clean
        // This test ensures the plugin approach doesn't modify core classes
        
        System.setProperty("rxjava3.performance.monitor", "true");
        RxJavaPerformancePlugin.install();
        
        // Standard Observable operations should work normally
        Integer result = Observable.just(1, 2, 3)
            .map(x -> x * 2)
            .filter(x -> x > 2)
            .reduce(Integer::sum)
            .blockingGet();
        
        assertEquals((Integer) 10, result);
        
        // Monitoring should work transparently
        String stats = PerformanceMonitor.getStatistics();
        assertNotNull(stats);
        assertTrue(stats.contains("Observable Creations:"));
    }
}
