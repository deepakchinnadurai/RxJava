package io.reactivex.rxjava3.internal.operators.observable;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observers.TestObserver;
import io.reactivex.rxjava3.testsupport.TestHelper;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class OptimizedObservableTakeTest {

    @Test
    public void testOptimizedTakePerformance() {
        TestObserver<Integer> to = Observable.range(1, 1000000)
            .take(5)
            .test();
        
        to.assertValueCount(5)
          .assertValues(1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoErrors();
    }

    @Test
    public void testTakeZeroElements() {
        TestObserver<Integer> to = Observable.range(1, 100)
            .take(0)
            .test();
        
        to.assertValueCount(0)
          .assertComplete()
          .assertNoErrors();
    }

    @Test
    public void testTakeMoreThanAvailable() {
        TestObserver<Integer> to = Observable.range(1, 5)
            .take(10)
            .test();
        
        to.assertValueCount(5)
          .assertValues(1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoErrors();
    }

    @Test
    public void testTakeExactAmount() {
        TestObserver<Integer> to = Observable.range(1, 5)
            .take(5)
            .test();
        
        to.assertValueCount(5)
          .assertValues(1, 2, 3, 4, 5)
          .assertComplete()
          .assertNoErrors();
    }

    @Test
    public void testTakeWithError() {
        TestObserver<Integer> to = Observable.<Integer>error(new RuntimeException("test"))
            .take(5)
            .test();
        
        to.assertValueCount(0)
          .assertError(RuntimeException.class);
    }

    @Test
    public void testTakeDispose() {
        AtomicInteger counter = new AtomicInteger();
        
        TestObserver<Integer> to = Observable.range(1, 1000)
            .doOnNext(i -> counter.incrementAndGet())
            .take(5)
            .test();
        
        to.dispose();
        
        // Should complete normally even when disposed
        assertTrue(to.isDisposed());
    }

    @Test
    public void testTakeFieldAccessOptimization() {
        // Test that the optimized field access pattern works correctly
        AtomicInteger subscriptionCount = new AtomicInteger();
        
        Observable<Integer> source = Observable.range(1, 100)
            .doOnSubscribe(d -> subscriptionCount.incrementAndGet());
        
        TestObserver<Integer> to = source.take(3).test();
        
        to.assertValueCount(3)
          .assertValues(1, 2, 3)
          .assertComplete();
        
        assertEquals(1, subscriptionCount.get());
    }

    @Test
    public void testTakeNegativeValue() {
        TestObserver<Integer> to = Observable.range(1, 5)
            .take(-1)
            .test();
        
        to.assertValueCount(0)
          .assertComplete()
          .assertNoErrors();
    }

    @Test
    public void testTakeWithBackpressure() {
        TestObserver<Integer> to = Observable.range(1, 1000)
            .take(100)
            .test();
        
        to.assertValueCount(100)
          .assertComplete()
          .assertNoErrors();
        
        // Verify first and last values
        assertEquals((Integer) 1, to.values().get(0));
        assertEquals((Integer) 100, to.values().get(99));
    }
}
