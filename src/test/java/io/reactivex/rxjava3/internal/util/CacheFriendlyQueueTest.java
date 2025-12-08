package io.reactivex.rxjava3.internal.util;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class CacheFriendlyQueueTest {

    @Test
    public void testBasicOfferPoll() {
        CacheFriendlyQueue<Integer> queue = new CacheFriendlyQueue<>(16);
        
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        
        assertTrue(queue.offer(1));
        assertFalse(queue.isEmpty());
        assertEquals(1, queue.size());
        
        Integer value = queue.poll();
        assertEquals((Integer) 1, value);
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
    }

    @Test
    public void testOfferPollSequence() {
        CacheFriendlyQueue<String> queue = new CacheFriendlyQueue<>(8);
        
        // Offer multiple items
        for (int i = 0; i < 5; i++) {
            assertTrue(queue.offer("item" + i));
        }
        
        assertEquals(5, queue.size());
        
        // Poll items in order
        for (int i = 0; i < 5; i++) {
            assertEquals("item" + i, queue.poll());
        }
        
        assertTrue(queue.isEmpty());
        assertNull(queue.poll());
    }

    @Test
    public void testOfferTwo() {
        CacheFriendlyQueue<Integer> queue = new CacheFriendlyQueue<>(16);
        
        assertTrue(queue.offer(1, 2));
        assertEquals(2, queue.size());
        
        assertEquals((Integer) 1, queue.poll());
        assertEquals((Integer) 2, queue.poll());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testOfferBatch() {
        CacheFriendlyQueue<Integer> queue = new CacheFriendlyQueue<>(32);
        Integer[] elements = {1, 2, 3, 4, 5};
        
        int offered = queue.offerBatch(elements, 5);
        assertEquals(5, offered);
        assertEquals(5, queue.size());
        
        for (int i = 1; i <= 5; i++) {
            assertEquals((Integer) i, queue.poll());
        }
    }

    @Test
    public void testPollBatch() {
        CacheFriendlyQueue<Integer> queue = new CacheFriendlyQueue<>(16);
        
        // Fill queue
        for (int i = 1; i <= 10; i++) {
            queue.offer(i);
        }
        
        Integer[] buffer = new Integer[5];
        int polled = queue.pollBatch(buffer, 5);
        
        assertEquals(5, polled);
        assertEquals(5, queue.size());
        
        for (int i = 0; i < 5; i++) {
            assertEquals((Integer) (i + 1), buffer[i]);
        }
    }

    @Test
    public void testCapacityLimits() {
        CacheFriendlyQueue<Integer> queue = new CacheFriendlyQueue<>(4);
        
        // Fill to capacity
        assertTrue(queue.offer(1));
        assertTrue(queue.offer(2));
        assertTrue(queue.offer(3));
        assertTrue(queue.offer(4));
        
        // Should reject when full
        assertFalse(queue.offer(5));
        
        // Poll one and try again
        assertEquals((Integer) 1, queue.poll());
        assertTrue(queue.offer(5));
    }

    @Test
    public void testClear() {
        CacheFriendlyQueue<String> queue = new CacheFriendlyQueue<>(8);
        
        for (int i = 0; i < 5; i++) {
            queue.offer("item" + i);
        }
        
        assertEquals(5, queue.size());
        assertFalse(queue.isEmpty());
        
        queue.clear();
        
        assertEquals(0, queue.size());
        assertTrue(queue.isEmpty());
        assertNull(queue.poll());
    }

    @Test
    public void testNullElements() {
        CacheFriendlyQueue<String> queue = new CacheFriendlyQueue<>(8);
        
        try {
            queue.offer(null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException expected) {
            // Expected
        }
        
        try {
            queue.offer("valid", null);
            fail("Should throw NullPointerException");
        } catch (NullPointerException expected) {
            // Expected
        }
    }

    @Test
    public void testConcurrentProducerConsumer() throws InterruptedException {
        CacheFriendlyQueue<Integer> queue = new CacheFriendlyQueue<>(1024);
        int itemCount = 10000;
        
        AtomicInteger produced = new AtomicInteger();
        AtomicInteger consumed = new AtomicInteger();
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch endLatch = new CountDownLatch(2);
        
        // Producer thread
        Thread producer = new Thread(() -> {
            try {
                startLatch.await();
                for (int i = 0; i < itemCount; i++) {
                    while (!queue.offer(i)) {
                        Thread.yield();
                    }
                    produced.incrementAndGet();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                endLatch.countDown();
            }
        });
        
        // Consumer thread
        Thread consumer = new Thread(() -> {
            try {
                startLatch.await();
                while (consumed.get() < itemCount) {
                    Integer item = queue.poll();
                    if (item != null) {
                        consumed.incrementAndGet();
                    } else {
                        Thread.yield();
                    }
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                endLatch.countDown();
            }
        });
        
        producer.start();
        consumer.start();
        
        startLatch.countDown();
        assertTrue(endLatch.await(10, TimeUnit.SECONDS));
        
        assertEquals(itemCount, produced.get());
        assertEquals(itemCount, consumed.get());
        assertTrue(queue.isEmpty());
    }

    @Test
    public void testBatchOperationsPerformance() {
        CacheFriendlyQueue<Integer> queue = new CacheFriendlyQueue<>(1024);
        Integer[] elements = new Integer[100];
        for (int i = 0; i < 100; i++) {
            elements[i] = i;
        }
        
        // Batch offer
        int offered = queue.offerBatch(elements, 100);
        assertEquals(100, offered);
        assertEquals(100, queue.size());
        
        // Batch poll
        Integer[] buffer = new Integer[50];
        int polled = queue.pollBatch(buffer, 50);
        assertEquals(50, polled);
        assertEquals(50, queue.size());
        
        // Verify order
        for (int i = 0; i < 50; i++) {
            assertEquals((Integer) i, buffer[i]);
        }
    }

    @Test
    public void testQueueCapacity() {
        CacheFriendlyQueue<Integer> queue = new CacheFriendlyQueue<>(16);
        assertEquals(16, queue.capacity());
        
        CacheFriendlyQueue<String> largeQueue = new CacheFriendlyQueue<>(1024);
        assertEquals(1024, largeQueue.capacity());
    }

    @Test
    public void testPowerOfTwoCapacity() {
        // Test that capacity is rounded to power of 2
        CacheFriendlyQueue<Integer> queue1 = new CacheFriendlyQueue<>(10);
        assertEquals(16, queue1.capacity()); // Next power of 2
        
        CacheFriendlyQueue<Integer> queue2 = new CacheFriendlyQueue<>(17);
        assertEquals(32, queue2.capacity()); // Next power of 2
    }

    @Test
    public void testEmptyPoll() {
        CacheFriendlyQueue<String> queue = new CacheFriendlyQueue<>(8);
        
        assertNull(queue.poll());
        assertTrue(queue.isEmpty());
        assertEquals(0, queue.size());
        
        String[] buffer = new String[5];
        int polled = queue.pollBatch(buffer, 5);
        assertEquals(0, polled);
    }
}
