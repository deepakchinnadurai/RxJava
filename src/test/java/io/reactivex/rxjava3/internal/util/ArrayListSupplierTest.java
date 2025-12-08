package io.reactivex.rxjava3.internal.util;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

public class ArrayListSupplierTest {

    @Test
    public void testGetMethod() {
        List<Object> list = ArrayListSupplier.INSTANCE.get();
        
        assertNotNull(list);
        assertTrue(list.isEmpty());
        assertEquals(0, list.size());
        
        // Test that it's a functional ArrayList
        list.add("test");
        assertEquals(1, list.size());
        assertEquals("test", list.get(0));
    }

    @Test
    public void testApplyMethod() {
        List<Object> list = ArrayListSupplier.INSTANCE.apply("ignored");
        
        assertNotNull(list);
        assertTrue(list.isEmpty());
        assertEquals(0, list.size());
        
        // Test that it's a functional ArrayList
        list.add(42);
        assertEquals(1, list.size());
        assertEquals(42, list.get(0));
    }

    @Test
    public void testAsSupplier() {
        io.reactivex.rxjava3.functions.Supplier<List<String>> supplier = ArrayListSupplier.<String>asSupplier();
        
        try {
            List<String> list = supplier.get();
            assertNotNull(list);
            assertTrue(list.isEmpty());
            
            // Test type safety
            list.add("string");
            assertEquals("string", list.get(0));
        } catch (Throwable e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testAsFunction() {
        io.reactivex.rxjava3.functions.Function<Integer, List<String>> function = ArrayListSupplier.<String, Integer>asFunction();
        
        try {
            List<String> list = function.apply(42);
            assertNotNull(list);
            assertTrue(list.isEmpty());
            
            // Test type safety
            list.add("converted");
            assertEquals("converted", list.get(0));
        } catch (Throwable e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testOptimizedCapacity() {
        List<Object> list = ArrayListSupplier.INSTANCE.get();
        
        // Test that the optimized capacity (8) works efficiently
        for (int i = 0; i < 8; i++) {
            list.add("item" + i);
        }
        
        assertEquals(8, list.size());
        
        // Verify all items are present
        for (int i = 0; i < 8; i++) {
            assertEquals("item" + i, list.get(i));
        }
    }

    @Test
    public void testMultipleInstances() {
        List<Object> list1 = ArrayListSupplier.INSTANCE.get();
        List<Object> list2 = ArrayListSupplier.INSTANCE.get();
        
        // Should be different instances
        assertNotSame(list1, list2);
        
        // But both should be empty ArrayLists
        assertTrue(list1.isEmpty());
        assertTrue(list2.isEmpty());
        
        // Modifications to one shouldn't affect the other
        list1.add("test1");
        list2.add("test2");
        
        assertEquals(1, list1.size());
        assertEquals(1, list2.size());
        assertEquals("test1", list1.get(0));
        assertEquals("test2", list2.get(0));
    }

    @Test
    public void testSingletonInstance() {
        // Verify it's a singleton enum
        assertSame(ArrayListSupplier.INSTANCE, ArrayListSupplier.valueOf("INSTANCE"));
        assertEquals(1, ArrayListSupplier.values().length);
    }

    @Test
    public void testPerformanceCharacteristics() {
        // Test that the optimized initial capacity reduces resizing
        List<Object> list = ArrayListSupplier.INSTANCE.get();
        
        // Add items up to the optimized capacity
        for (int i = 0; i < 8; i++) {
            list.add(i);
        }
        
        // Should handle the expected size efficiently
        assertEquals(8, list.size());
        
        // Add one more to test growth
        list.add(8);
        assertEquals(9, list.size());
    }

    @Test
    public void testTypeErasureWorkaround() {
        // Test the unchecked cast warnings are handled properly
        io.reactivex.rxjava3.functions.Supplier<List<String>> stringSupplier = ArrayListSupplier.<String>asSupplier();
        io.reactivex.rxjava3.functions.Function<String, List<Integer>> integerFunction = ArrayListSupplier.<Integer, String>asFunction();
        
        try {
            List<String> stringList = stringSupplier.get();
            List<Integer> integerList = integerFunction.apply("input");
            
            assertNotNull(stringList);
            assertNotNull(integerList);
            
            // Type safety should work at compile time
            stringList.add("string");
            integerList.add(42);
            
            assertEquals("string", stringList.get(0));
            assertEquals((Integer) 42, integerList.get(0));
        } catch (Throwable e) {
            fail("Unexpected exception: " + e.getMessage());
        }
    }

    @Test
    public void testConcurrentUsage() throws InterruptedException {
        int threadCount = 10;
        Thread[] threads = new Thread[threadCount];
        @SuppressWarnings("unchecked")
        List<Object>[] results = new List[threadCount];
        
        for (int i = 0; i < threadCount; i++) {
            final int threadIndex = i;
            threads[i] = new Thread(() -> {
                List<Object> list = ArrayListSupplier.INSTANCE.get();
                list.add("thread" + threadIndex);
                results[threadIndex] = list;
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
        
        // Verify each thread got its own list
        for (int i = 0; i < threadCount; i++) {
            assertNotNull(results[i]);
            assertEquals(1, results[i].size());
            assertEquals("thread" + i, results[i].get(0));
        }
        
        // Verify all lists are different instances
        for (int i = 0; i < threadCount; i++) {
            for (int j = i + 1; j < threadCount; j++) {
                assertNotSame(results[i], results[j]);
            }
        }
    }
}
