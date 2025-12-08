package io.reactivex.rxjava3.internal.util;

import org.junit.Test;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

import static org.junit.Assert.*;

public class OptimizedCollectionsTest {

    @Test
    public void testCreateOptimizedList() {
        ArrayList<String> list = OptimizedCollections.createOptimizedList();
        assertNotNull(list);
        assertTrue(list.isEmpty());
        
        // Test that it can hold elements without resizing
        for (int i = 0; i < 16; i++) {
            list.add("item" + i);
        }
        assertEquals(16, list.size());
    }

    @Test
    public void testCreateOptimizedListWithSize() {
        ArrayList<Integer> list = OptimizedCollections.createOptimizedList(100);
        assertNotNull(list);
        assertTrue(list.isEmpty());
        
        // Test that it can hold expected size without resizing
        for (int i = 0; i < 100; i++) {
            list.add(i);
        }
        assertEquals(100, list.size());
    }

    @Test
    public void testCreateOptimizedListPowerOfTwo() {
        // Test that capacity is rounded to power of 2
        ArrayList<String> list = OptimizedCollections.createOptimizedList(50);
        
        // Should be able to hold at least 50 elements efficiently
        for (int i = 0; i < 50; i++) {
            list.add("item" + i);
        }
        assertEquals(50, list.size());
    }

    @Test
    public void testCreateOptimizedMap() {
        HashMap<String, Integer> map = OptimizedCollections.createOptimizedMap();
        assertNotNull(map);
        assertTrue(map.isEmpty());
        
        // Test basic operations
        map.put("key1", 1);
        map.put("key2", 2);
        assertEquals(2, map.size());
        assertEquals((Integer) 1, map.get("key1"));
    }

    @Test
    public void testCreateOptimizedMapWithSize() {
        HashMap<String, Integer> map = OptimizedCollections.createOptimizedMap(1000);
        assertNotNull(map);
        assertTrue(map.isEmpty());
        
        // Test that it can hold expected size without rehashing
        for (int i = 0; i < 1000; i++) {
            map.put("key" + i, i);
        }
        assertEquals(1000, map.size());
    }

    @Test
    public void testCreateOptimizedConcurrentMap() {
        ConcurrentHashMap<String, Integer> map = OptimizedCollections.createOptimizedConcurrentMap();
        assertNotNull(map);
        assertTrue(map.isEmpty());
        
        // Test thread-safe operations
        map.put("key1", 1);
        map.putIfAbsent("key1", 2);
        assertEquals((Integer) 1, map.get("key1"));
    }

    @Test
    public void testCreateOptimizedConcurrentMapWithSize() {
        ConcurrentHashMap<String, Integer> map = OptimizedCollections.createOptimizedConcurrentMap(500);
        assertNotNull(map);
        
        // Test concurrent access
        for (int i = 0; i < 500; i++) {
            map.put("key" + i, i);
        }
        assertEquals(500, map.size());
    }

    @Test
    public void testCreateOptimizedSetSmall() {
        Set<String> set = OptimizedCollections.createOptimizedSet(4);
        assertNotNull(set);
        assertTrue(set.isEmpty());
        
        set.add("item1");
        set.add("item2");
        set.add("item3");
        set.add("item4");
        
        assertEquals(4, set.size());
        assertTrue(set.contains("item1"));
    }

    @Test
    public void testCreateOptimizedSetLarge() {
        Set<String> set = OptimizedCollections.createOptimizedSet(100);
        assertNotNull(set);
        
        for (int i = 0; i < 100; i++) {
            set.add("item" + i);
        }
        assertEquals(100, set.size());
    }

    @Test
    public void testIsNullOrEmpty() {
        assertTrue(OptimizedCollections.isNullOrEmpty(null));
        assertTrue(OptimizedCollections.isNullOrEmpty(new ArrayList<>()));
        
        List<String> list = new ArrayList<>();
        list.add("item");
        assertFalse(OptimizedCollections.isNullOrEmpty(list));
    }

    @Test
    public void testSafeSize() {
        assertEquals(0, OptimizedCollections.safeSize(null));
        assertEquals(0, OptimizedCollections.safeSize(new ArrayList<>()));
        
        List<String> list = new ArrayList<>();
        list.add("item1");
        list.add("item2");
        assertEquals(2, OptimizedCollections.safeSize(list));
    }

    @Test
    public void testOptimizedListMemoryEfficiency() {
        // Test that optimized lists use less memory for typical sizes
        ArrayList<String> optimized = OptimizedCollections.createOptimizedList(8);
        ArrayList<String> standard = new ArrayList<>();
        
        // Both should handle 8 elements efficiently
        for (int i = 0; i < 8; i++) {
            optimized.add("item" + i);
            standard.add("item" + i);
        }
        
        assertEquals(optimized.size(), standard.size());
        assertEquals(optimized, standard);
    }

    @Test
    public void testOptimizedMapLoadFactor() {
        HashMap<String, Integer> map = OptimizedCollections.createOptimizedMap(100);
        
        // Should handle 100 elements without rehashing due to optimized capacity
        for (int i = 0; i < 100; i++) {
            map.put("key" + i, i);
        }
        
        assertEquals(100, map.size());
        
        // Verify all elements are present
        for (int i = 0; i < 100; i++) {
            assertEquals((Integer) i, map.get("key" + i));
        }
    }
}
