import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import io.reactivex.rxjava3.subjects.PublishSubject;
import io.reactivex.rxjava3.config.RxJavaConfig;
import io.reactivex.rxjava3.internal.util.PerformanceMonitor;


/**
 * RxJava Demo Application
 *
 * This example demonstrates various RxJava concepts:
 * - Creating Observables
 * - Transforming data streams with operators
 * - Handling errors
 * - Working with multiple Observables
 * - Using different Schedulers for concurrency
 * - Using Subjects
 */
public class RxJavaDemo {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("\n=== RxJava Demo Application ===\n");
        
        // Initialize RxJava with production-ready configuration
        RxJavaConfig.initialize();
        
        // Display configuration
        System.out.println(RxJavaConfig.getConfigurationSummary());
        System.out.println();

        // Basic Observable example
        basicObservableDemo();

        // Operators demo
        operatorsDemo();

        // Error handling demo
        errorHandlingDemo();

        // Combining Observables demo
        combiningObservablesDemo();

        // Concurrency with Schedulers demo
        schedulersDemo();

        // Subjects demo
        subjectsDemo();

        // Performance optimization demo
        performanceOptimizationDemo();

        // Sleep to allow async operations to complete
        Thread.sleep(5000);

        // Display performance statistics
        System.out.println("\n=== Performance Statistics ===");
        System.out.println(PerformanceMonitor.getStatistics());

        System.out.println("\n=== Demo Completed ===");
    }

    private static void basicObservableDemo() {
        System.out.println("\n--- Basic Observable Example ---");

        // Create a simple Observable that emits a sequence of integers
        Observable<Integer> observable = Observable.range(1, 5);

        // Create an Observer that responds to events from the Observable
        Observer<Integer> observer = new Observer<Integer>() {
            @Override
            public void onSubscribe(Disposable d) {
                System.out.println("Subscribed to Observable");
            }

            @Override
            public void onNext(Integer value) {
                System.out.println("Received value: " + value);
            }

            @Override
            public void onError(Throwable e) {
                System.err.println("Error: " + e.getMessage());
            }

            @Override
            public void onComplete() {
                System.out.println("Sequence completed");
            }
        };

        // Connect the Observable to the Observer by subscribing
        observable.subscribe(observer);
    }

    private static void operatorsDemo() {
        System.out.println("\n--- Operators Example ---");

        // Create an Observable from a list of strings
        Observable.just("apple", "banana", "cherry", "date", "elderberry")
            // Filter items based on a condition
            .filter(s -> s.length() > 5)
            // Transform each item
            .map(String::toUpperCase)
            // Take only the first 2 items
            .take(2)
            // Subscribe with simple handlers
            .subscribe(
                s -> System.out.println("Received: " + s),
                Throwable::printStackTrace,
                () -> System.out.println("Operators sequence completed")
            );
    }

    private static void errorHandlingDemo() {
        System.out.println("\n--- Error Handling Example ---");

        Observable.just("1", "2", "three", "4", "five")
            .map(s -> {
                try {
                    return Integer.parseInt(s);
                } catch (NumberFormatException e) {
                    throw new RuntimeException("Cannot parse: " + s, e);
                }
            })
            // Handle errors by returning a default value
            .onErrorReturn(e -> {
                System.err.println("Error caught: " + e.getMessage());
                return -1; // Default value on error
            })
            .subscribe(
                i -> System.out.println("Parsed value: " + i),
                Throwable::printStackTrace,
                () -> System.out.println("Error handling sequence completed")
            );
    }

    private static void combiningObservablesDemo() {
        System.out.println("\n--- Combining Observables Example ---");

        // First Observable emits some integers
        Observable<Integer> numbers = Observable.just(1, 2, 3, 4, 5);

        // Second Observable emits some strings
        Observable<String> strings = Observable.just("A", "B", "C");

        // Combine both Observables using zip operator
        Observable.zip(numbers, strings,
            // Combine each pair of items from both Observables
            (num, str) -> num + str
        )
        .subscribe(
            result -> System.out.println("Zipped result: " + result),
            Throwable::printStackTrace,
            () -> System.out.println("Zip sequence completed")
        );

        // Concatenate Observables
        Observable.concat(
            Observable.just("First", "Second"),
            Observable.just("Third", "Fourth")
        )
        .subscribe(
            s -> System.out.println("Concat item: " + s),
            Throwable::printStackTrace,
            () -> System.out.println("Concat sequence completed")
        );
    }

    private static void schedulersDemo() {
        System.out.println("\n--- Schedulers Example ---");

        Observable.just("Operation 1", "Operation 2", "Operation 3")
            // Run the Observable operations on the IO scheduler (background thread pool)
            .subscribeOn(Schedulers.io())
            // Perform a slow operation
            .map(s -> {
                System.out.println("Processing '" + s + "' on thread: "
                    + Thread.currentThread().getName());
                // Simulate work
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return s + " processed";
            })
            // Observe the results on the computation scheduler
            .observeOn(Schedulers.computation())
            .subscribe(
                result -> System.out.println("Received result '" + result + "' on thread: "
                    + Thread.currentThread().getName()),
                Throwable::printStackTrace,
                () -> System.out.println("Schedulers sequence completed")
            );
    }

    private static void subjectsDemo() {
        System.out.println("\n--- Subjects Example ---");

        // Create a PublishSubject (both an Observable and Observer)
        PublishSubject<String> subject = PublishSubject.create();

        // Subscribe to the subject
        subject.subscribe(
            s -> System.out.println("Subscriber 1 received: " + s),
            Throwable::printStackTrace,
            () -> System.out.println("Subscriber 1 completed")
        );

        // Emit a value
        subject.onNext("First value");

        // Add a second subscriber (will only receive values emitted after subscription)
        subject.subscribe(
            s -> System.out.println("Subscriber 2 received: " + s),
            Throwable::printStackTrace,
            () -> System.out.println("Subscriber 2 completed")
        );

        // Emit more values
        subject.onNext("Second value");
        subject.onNext("Third value");

        // Complete the subject
        subject.onComplete();
    }

    private static void performanceOptimizationDemo() {
        System.out.println("\n--- Performance Optimization Example ---");
        
        // Reset performance counters
        PerformanceMonitor.reset();
        
        // Demonstrate optimized operators
        System.out.println("Testing optimized take() operator...");
        Observable.range(1, 1000000)
            .take(5) // Uses our optimized TakeObserver
            .subscribe(
                num -> System.out.println("Optimized take received: " + num),
                Throwable::printStackTrace,
                () -> System.out.println("Optimized take completed")
            );
        
        System.out.println("\nTesting optimized filter() operator...");
        Observable.range(1, 100)
            .filter(n -> n % 2 == 0) // Uses our optimized FilterObserver
            .take(5)
            .subscribe(
                num -> System.out.println("Optimized filter received: " + num),
                Throwable::printStackTrace,
                () -> System.out.println("Optimized filter completed")
            );
        
        // Demonstrate performance monitoring (if enabled)
        if (RxJavaConfig.isPerformanceMonitoringEnabled()) {
            System.out.println("\nCreating multiple Observables to test monitoring...");
            for (int i = 0; i < 10; i++) {
                Observable.just("Item " + i)
                    .map(s -> s.toUpperCase())
                    .subscribe(
                        result -> {},
                        Throwable::printStackTrace
                    );
            }
        } else {
            System.out.println("\nPerformance monitoring disabled. Enable with -Drxjava3.performance.monitor=true");
        }
        
        System.out.println("\nRunning Performance Benchmark...");
        try {
            PerformanceBenchmark.runComprehensiveBenchmark();
        } catch (Exception e) {
            System.err.println("Benchmark error: " + e.getMessage());
        }
        
        System.out.println("Performance optimization demo completed");
    }
}