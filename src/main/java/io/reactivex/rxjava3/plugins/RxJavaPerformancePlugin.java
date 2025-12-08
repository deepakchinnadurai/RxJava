/*
 * Copyright (c) 2016-present, RxJava Contributors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */

package io.reactivex.rxjava3.plugins;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.internal.util.PerformanceMonitor;

/**
 * Production-ready performance monitoring plugin for RxJava.
 * 
 * Uses the RxJava plugin system to add monitoring without polluting core code.
 * Can be enabled/disabled via configuration without code changes.
 */
public final class RxJavaPerformancePlugin {
    
    private static volatile boolean enabled = false;
    
    static {
        // Check system property for monitoring enablement
        String monitoringProperty = System.getProperty("rxjava3.performance.monitor", "false");
        enabled = Boolean.parseBoolean(monitoringProperty);
    }
    
    private RxJavaPerformancePlugin() {
        throw new IllegalStateException("No instances!");
    }
    
    /**
     * Install the performance monitoring plugin.
     * This should be called once during application startup.
     */
    public static void install() {
        if (!enabled) {
            return;
        }
        
        // Hook into Observable assembly for creation tracking
        RxJavaPlugins.setOnObservableAssembly(observable -> {
            PerformanceMonitor.recordObservableCreation();
            return observable;
        });
        
        // Hook into Observable subscription for subscription tracking
        RxJavaPlugins.setOnObservableSubscribe((observable, observer) -> {
            PerformanceMonitor.recordSubscription();
            return new MonitoringObserver<>(observer);
        });
        
        // Hook into error handling
        RxJavaPlugins.setErrorHandler(throwable -> {
            PerformanceMonitor.recordError();
            // Let the default error handler deal with it
            Thread currentThread = Thread.currentThread();
            currentThread.getUncaughtExceptionHandler()
                .uncaughtException(currentThread, throwable);
        });
    }
    
    /**
     * Uninstall the performance monitoring plugin.
     */
    public static void uninstall() {
        RxJavaPlugins.reset();
    }
    
    /**
     * Check if monitoring is enabled.
     */
    public static boolean isEnabled() {
        return enabled;
    }
    
    /**
     * Enable monitoring at runtime.
     */
    public static void enable() {
        enabled = true;
        install();
    }
    
    /**
     * Disable monitoring at runtime.
     */
    public static void disable() {
        enabled = false;
        uninstall();
    }
    
    /**
     * Observer wrapper that tracks disposal events.
     */
    private static final class MonitoringObserver<T> implements Observer<T> {
        private final Observer<? super T> actual;
        
        MonitoringObserver(Observer<? super T> actual) {
            this.actual = actual;
        }
        
        @Override
        public void onSubscribe(io.reactivex.rxjava3.disposables.Disposable d) {
            actual.onSubscribe(new MonitoringDisposable(d));
        }
        
        @Override
        public void onNext(T t) {
            actual.onNext(t);
        }
        
        @Override
        public void onError(Throwable e) {
            actual.onError(e);
        }
        
        @Override
        public void onComplete() {
            actual.onComplete();
        }
    }
    
    /**
     * Disposable wrapper that tracks disposal events.
     */
    private static final class MonitoringDisposable implements io.reactivex.rxjava3.disposables.Disposable {
        private final io.reactivex.rxjava3.disposables.Disposable actual;
        
        MonitoringDisposable(io.reactivex.rxjava3.disposables.Disposable actual) {
            this.actual = actual;
        }
        
        @Override
        public void dispose() {
            PerformanceMonitor.recordDisposal();
            actual.dispose();
        }
        
        @Override
        public boolean isDisposed() {
            return actual.isDisposed();
        }
    }
}
