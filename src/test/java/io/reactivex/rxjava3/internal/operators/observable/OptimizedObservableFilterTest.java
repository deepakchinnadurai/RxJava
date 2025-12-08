package io.reactivex.rxjava3.internal.operators.observable;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.functions.Predicate;
import io.reactivex.rxjava3.observers.TestObserver;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class OptimizedObservableFilterTest {

    @Test
    public void testOptimizedFilterBranching() {
        AtomicInteger filterCalls = new AtomicInteger();
        
        TestObserver<Integer> to = Observable.range(1, 1000)
            .filter(n -> {
                filterCalls.incrementAndGet();
                return n % 2 == 0;
            })
            .test();
        
        to.assertValueCount(500)
          .assertComplete()
          .assertNoErrors();
        
        assertEquals(1000, filterCalls.get());
    }

    @Test
    public void testFilterExceptionHandling() {
        TestObserver<Integer> to = Observable.range(1, 10)
            .filter(n -> {
                if (n == 5) {
                    throw new RuntimeException("Filter error");
                }
                return n % 2 == 0;
            })
            .test();
        
        to.assertError(RuntimeException.class);
    }

    @Test
    public void testFilterAllPass() {
        TestObserver<Integer> to = Observable.range(1, 100)
            .filter(n -> true)
            .test();
        
        to.assertValueCount(100)
          .assertComplete()
          .assertNoErrors();
    }

    @Test
    public void testFilterAllReject() {
        TestObserver<Integer> to = Observable.range(1, 100)
            .filter(n -> false)
            .test();
        
        to.assertValueCount(0)
          .assertComplete()
          .assertNoErrors();
    }

    @Test
    public void testFilterWithNullPredicate() {
        try {
            Observable.range(1, 5)
                .filter((Predicate<Integer>) null)
                .test();
            fail("Should throw NullPointerException");
        } catch (NullPointerException expected) {
            // Expected behavior
        }
    }

    @Test
    public void testFilterPerformanceOptimization() {
        // Test that the optimized exception handling doesn't affect normal flow
        AtomicInteger processedCount = new AtomicInteger();
        
        TestObserver<Integer> to = Observable.range(1, 10000)
            .filter(n -> {
                processedCount.incrementAndGet();
                return n % 10 == 0;
            })
            .test();
        
        to.assertValueCount(1000)
          .assertComplete()
          .assertNoErrors();
        
        assertEquals(10000, processedCount.get());
    }

    @Test
    public void testFilterWithEmpty() {
        TestObserver<Integer> to = Observable.<Integer>empty()
            .filter(n -> n > 0)
            .test();
        
        to.assertValueCount(0)
          .assertComplete()
          .assertNoErrors();
    }

    @Test
    public void testFilterDispose() {
        AtomicInteger filterCalls = new AtomicInteger();
        
        TestObserver<Integer> to = Observable.range(1, 1000000)
            .filter(n -> {
                filterCalls.incrementAndGet();
                return n % 2 == 0;
            })
            .test();
        
        to.dispose();
        
        assertTrue(to.isDisposed());
        // Filter calls should be limited due to disposal
        assertTrue(filterCalls.get() < 1000000);
    }

    @Test
    public void testFilterChaining() {
        TestObserver<Integer> to = Observable.range(1, 100)
            .filter(n -> n > 50)
            .filter(n -> n % 2 == 0)
            .filter(n -> n < 90)
            .test();
        
        // Should get even numbers between 52 and 88
        to.assertValueCount(19)
          .assertComplete()
          .assertNoErrors();
        
        // Verify first and last values
        assertEquals((Integer) 52, to.values().get(0));
        assertEquals((Integer) 88, to.values().get(18));
    }
}
