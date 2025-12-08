package io.reactivex.rxjava3.internal.util;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class LockFreeCounterTest {

    @Test
    public void testBasicIncrement() {
        LockFreeCounter counter = new LockFreeCounter();
        assertEquals(0, counter.get());
        
        counter.increment();
        assertEquals(1, counter.get());
        
        counter.increment();
        assertEquals(2, counter.get());
    }

    @Test
    public void testBasicDecrement() {
        LockFreeCounter counter = new LockFreeCounter(10);
        assertEquals(10, counter.get());
        
        counter.decrement();
        assertEquals(9, counter.get());
        
        counter.decrement();
        assertEquals(8, counter.get());
    }

    @Test
    public void testAdd() {
        LockFreeCounter counter = new LockFreeCounter();
        
        counter.add(5);
        assertEquals(5, counter.get());
        
        counter.add(-2);
        assertEquals(3, counter.get());
        
        counter.add(0);
        assertEquals(3, counter.get());
    }

    @Test
    public void testReset() {
        LockFreeCounter counter = new LockFreeCounter(100);
        assertEquals(100, counter.get());
        
        counter.reset();
        assertEquals(0, counter.get());
    }

    @Test
    public void testSet() {
        LockFreeCounter counter = new LockFreeCounter();
        
        counter.set(42);
        assertEquals(42, counter.get());
        
        counter.set(-10);
        assertEquals(-10, counter.get());
    }

    @Test
    public void testGetAndReset() {
        LockFreeCounter counter = new LockFreeCounter(25);
        
        long value = counter.getAndReset();
        assertEquals(25, value);
        assertEquals(0, counter.get());
    }

    @Test
    public void testConcurrentIncrement() throws InterruptedException {
        LockFreeCounter counter = new LockFreeCounter();
        int threadCount = 10;
        int operationsPerThread = 1000;
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            new Thread(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < operationsPerThread; j++) {
                        counter.increment();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            }).start();
        }
        
        startLatch.countDown();
        assertTrue(endLatch.await(10, TimeUnit.SECONDS));
        
        assertEquals(threadCount * operationsPerThread, counter.get());
    }

    @Test
    public void testConcurrentMixedOperations() throws InterruptedException {
        LockFreeCounter counter = new LockFreeCounter(1000);
        int threadCount = 8;
        int operationsPerThread = 500;
        
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(threadCount);
        AtomicInteger incrementThreads = new AtomicInteger();
        AtomicInteger decrementThreads = new AtomicInteger();
        
        for (int i = 0; i < threadCount; i++) {
            final int threadId = i;
            new Thread(() -> {
                try {
                    startLatch.await();
                    if (threadId % 2 == 0) {
                        incrementThreads.incrementAndGet();
                        for (int j = 0; j < operationsPerThread; j++) {
                            counter.increment();
                        }
                    } else {
                        decrementThreads.incrementAndGet();
                        for (int j = 0; j < operationsPerThread; j++) {
                            counter.decrement();
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    endLatch.countDown();
                }
            }).start();
        }
        
        startLatch.countDown();
        assertTrue(endLatch.await(10, TimeUnit.SECONDS));
        
        // Expected: initial value + (increment threads * ops) - (decrement threads * ops)
        long expected = 1000 + (incrementThreads.get() * operationsPerThread) - (decrementThreads.get() * operationsPerThread);
        assertEquals(expected, counter.get());
    }

    @Test
    public void testHighContentionDetection() throws InterruptedException {
        LockFreeCounter counter = new LockFreeCounter();
        int threadCount = 20; // High contention
        int operationsPerThread = 1000;
        
        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch latch = new CountDownLatch(threadCount);
        
        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    for (int j = 0; j < operationsPerThread; j++) {
                        counter.increment();
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        
        assertTrue(latch.await(15, TimeUnit.SECONDS));
        executor.shutdown();
        
        assertEquals(threadCount * operationsPerThread, counter.get());
        
        // Under high contention, it might switch to adder mode
        LockFreeCounter.ContentionStats stats = counter.getContentionStats();
        assertNotNull(stats);
        assertTrue(stats.totalOperations >= 0);
    }

    @Test
    public void testContentionStats() {
        LockFreeCounter counter = new LockFreeCounter();
        
        // Initially should be in low contention mode
        assertFalse(counter.isHighContentionMode());
        
        LockFreeCounter.ContentionStats stats = counter.getContentionStats();
        assertNotNull(stats);
        assertEquals(0, stats.totalOperations);
        assertEquals(0, stats.failedOperations);
        assertFalse(stats.highContentionMode);
        assertEquals(0.0, stats.failureRate, 0.001);
    }

    @Test
    public void testLargeValues() {
        LockFreeCounter counter = new LockFreeCounter();
        
        counter.add(Long.MAX_VALUE / 2);
        assertTrue(counter.get() > 0);
        
        counter.add(Long.MAX_VALUE / 2);
        assertTrue(counter.get() > Long.MAX_VALUE / 2);
    }

    @Test
    public void testNegativeValues() {
        LockFreeCounter counter = new LockFreeCounter();
        
        counter.add(-100);
        assertEquals(-100, counter.get());
        
        counter.increment();
        assertEquals(-99, counter.get());
        
        counter.decrement();
        assertEquals(-100, counter.get());
    }

    @Test
    public void testInitialValue() {
        LockFreeCounter counter1 = new LockFreeCounter();
        assertEquals(0, counter1.get());
        
        LockFreeCounter counter2 = new LockFreeCounter(42);
        assertEquals(42, counter2.get());
        
        LockFreeCounter counter3 = new LockFreeCounter(-10);
        assertEquals(-10, counter3.get());
    }
}
