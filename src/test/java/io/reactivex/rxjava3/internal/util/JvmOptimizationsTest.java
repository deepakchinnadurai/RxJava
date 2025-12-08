package io.reactivex.rxjava3.internal.util;

import org.junit.Test;

import static org.junit.Assert.*;

public class JvmOptimizationsTest {

    @Test
    public void testLikelyUnlikelyHints() {
        // Test that likely/unlikely methods work correctly
        assertTrue(JvmOptimizations.likely(true));
        assertFalse(JvmOptimizations.likely(false));
        
        assertTrue(JvmOptimizations.unlikely(true));
        assertFalse(JvmOptimizations.unlikely(false));
    }

    @Test
    public void testCpuRelax() {
        // Test that cpuRelax doesn't throw exceptions
        JvmOptimizations.cpuRelax();
        
        // Multiple calls should work
        for (int i = 0; i < 10; i++) {
            JvmOptimizations.cpuRelax();
        }
    }

    @Test
    public void testIsPowerOfTwo() {
        assertTrue(JvmOptimizations.isPowerOfTwo(1));
        assertTrue(JvmOptimizations.isPowerOfTwo(2));
        assertTrue(JvmOptimizations.isPowerOfTwo(4));
        assertTrue(JvmOptimizations.isPowerOfTwo(8));
        assertTrue(JvmOptimizations.isPowerOfTwo(16));
        assertTrue(JvmOptimizations.isPowerOfTwo(1024));
        
        assertFalse(JvmOptimizations.isPowerOfTwo(0));
        assertFalse(JvmOptimizations.isPowerOfTwo(-1));
        assertFalse(JvmOptimizations.isPowerOfTwo(3));
        assertFalse(JvmOptimizations.isPowerOfTwo(5));
        assertFalse(JvmOptimizations.isPowerOfTwo(7));
        assertFalse(JvmOptimizations.isPowerOfTwo(15));
    }

    @Test
    public void testFastModulo() {
        // Test with power of 2 divisors
        assertEquals(0, JvmOptimizations.fastModulo(8, 8));
        assertEquals(1, JvmOptimizations.fastModulo(9, 8));
        assertEquals(7, JvmOptimizations.fastModulo(15, 8));
        assertEquals(0, JvmOptimizations.fastModulo(16, 8));
        
        assertEquals(0, JvmOptimizations.fastModulo(4, 4));
        assertEquals(1, JvmOptimizations.fastModulo(5, 4));
        assertEquals(3, JvmOptimizations.fastModulo(7, 4));
    }

    @Test
    public void testFastHashCodeInt() {
        int value1 = 42;
        int value2 = 43;
        
        int hash1 = JvmOptimizations.fastHashCode(value1);
        int hash2 = JvmOptimizations.fastHashCode(value2);
        
        // Different values should produce different hashes (usually)
        assertNotEquals(hash1, hash2);
        
        // Same value should produce same hash
        assertEquals(hash1, JvmOptimizations.fastHashCode(value1));
    }

    @Test
    public void testFastHashCodeLong() {
        long value1 = 123456789L;
        long value2 = 987654321L;
        
        int hash1 = JvmOptimizations.fastHashCode(value1);
        int hash2 = JvmOptimizations.fastHashCode(value2);
        
        // Different values should produce different hashes (usually)
        assertNotEquals(hash1, hash2);
        
        // Same value should produce same hash
        assertEquals(hash1, JvmOptimizations.fastHashCode(value1));
    }

    @Test
    public void testFastArrayCopy() {
        String[] source = {"a", "b", "c", "d", "e", "f", "g", "h"};
        String[] dest = new String[10];
        
        // Test small array copy (should use unrolled version)
        JvmOptimizations.fastArrayCopy(source, 0, dest, 0, 5);
        
        for (int i = 0; i < 5; i++) {
            assertEquals(source[i], dest[i]);
        }
        
        // Test larger array copy (should use System.arraycopy)
        String[] largeDest = new String[20];
        JvmOptimizations.fastArrayCopy(source, 0, largeDest, 5, 8);
        
        for (int i = 0; i < 8; i++) {
            assertEquals(source[i], largeDest[i + 5]);
        }
    }

    @Test
    public void testGetOptimalBufferSize() {
        int bufferSize = JvmOptimizations.getOptimalBufferSize();
        
        assertTrue("Buffer size should be positive", bufferSize > 0);
        assertTrue("Buffer size should be reasonable", bufferSize >= 32 && bufferSize <= 256);
    }

    @Test
    public void testGetOptimalConcurrencyLevel() {
        int concurrencyLevel = JvmOptimizations.getOptimalConcurrencyLevel();
        
        assertTrue("Concurrency level should be positive", concurrencyLevel > 0);
        assertTrue("Concurrency level should not exceed 32", concurrencyLevel <= 32);
        assertTrue("Concurrency level should not exceed CPU cores", 
                   concurrencyLevel <= Runtime.getRuntime().availableProcessors());
    }

    @Test
    public void testNextRandomInt() {
        int random1 = JvmOptimizations.nextRandomInt();
        int random2 = JvmOptimizations.nextRandomInt();
        
        // Very unlikely to be the same (but theoretically possible)
        // Just test that method works without exceptions
        assertNotNull(Integer.valueOf(random1));
        assertNotNull(Integer.valueOf(random2));
    }

    @Test
    public void testNextRandomIntWithBound() {
        int bound = 100;
        
        for (int i = 0; i < 100; i++) {
            int random = JvmOptimizations.nextRandomInt(bound);
            assertTrue("Random should be >= 0", random >= 0);
            assertTrue("Random should be < bound", random < bound);
        }
    }

    @Test
    public void testJvmInformation() {
        // Test that JVM information methods work
        boolean isHotSpot = JvmOptimizations.isHotSpotJvm();
        boolean isGraal = JvmOptimizations.isGraalJvm();
        boolean supportsIntrinsics = JvmOptimizations.supportsIntrinsics();
        int cpuCores = JvmOptimizations.getCpuCores();
        boolean isX86_64 = JvmOptimizations.isX86_64();
        
        // These should return valid values
        assertTrue("CPU cores should be positive", cpuCores > 0);
        
        // At least one JVM type should be detected or neither
        // (both could be false for other JVM implementations)
        
        // These methods should not throw exceptions
        assertNotNull(Boolean.valueOf(isHotSpot));
        assertNotNull(Boolean.valueOf(isGraal));
        assertNotNull(Boolean.valueOf(supportsIntrinsics));
        assertNotNull(Boolean.valueOf(isX86_64));
    }

    @Test
    public void testPrefetchOperations() {
        Object[] array = new Object[100];
        for (int i = 0; i < 100; i++) {
            array[i] = "item" + i;
        }
        
        // Test that prefetch operations don't throw exceptions
        JvmOptimizations.prefetchForRead(array, 10);
        JvmOptimizations.prefetchForRead(array, 99);
        
        // Test batch prefetch
        JvmOptimizations.prefetchBatch(array, 0, 50);
        JvmOptimizations.prefetchBatch(array, 50, 50);
    }

    @Test
    public void testPrefetchBoundaryConditions() {
        Object[] array = new Object[10];
        for (int i = 0; i < 10; i++) {
            array[i] = "item" + i;
        }
        
        // Test boundary conditions
        JvmOptimizations.prefetchForRead(array, 0);
        JvmOptimizations.prefetchForRead(array, 9);
        JvmOptimizations.prefetchForRead(array, 100); // Out of bounds - should handle gracefully
        
        JvmOptimizations.prefetchBatch(array, 0, 10);
        JvmOptimizations.prefetchBatch(array, 5, 20); // Extends beyond array - should handle gracefully
    }

    @Test
    public void testHashCodeDistribution() {
        // Test that hash codes have reasonable distribution
        int[] values = {0, 1, 2, 3, 4, 5, 100, 1000, 10000, Integer.MAX_VALUE};
        int[] hashes = new int[values.length];
        
        for (int i = 0; i < values.length; i++) {
            hashes[i] = JvmOptimizations.fastHashCode(values[i]);
        }
        
        // Check that we don't have too many collisions
        int uniqueHashes = 0;
        for (int i = 0; i < hashes.length; i++) {
            boolean unique = true;
            for (int j = 0; j < i; j++) {
                if (hashes[i] == hashes[j]) {
                    unique = false;
                    break;
                }
            }
            if (unique) {
                uniqueHashes++;
            }
        }
        
        // Should have mostly unique hashes
        assertTrue("Should have reasonable hash distribution", uniqueHashes >= values.length * 0.8);
    }
}
