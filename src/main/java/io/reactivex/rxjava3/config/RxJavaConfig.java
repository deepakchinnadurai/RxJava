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

package io.reactivex.rxjava3.config;

import io.reactivex.rxjava3.plugins.RxJavaPerformancePlugin;

/**
 * Production-ready configuration for RxJava performance features.
 * 
 * Provides centralized configuration management for performance monitoring,
 * optimizations, and other runtime settings.
 */
public final class RxJavaConfig {
    
    // Configuration keys
    private static final String PERFORMANCE_MONITOR_KEY = "rxjava3.performance.monitor";
    private static final String BUFFER_SIZE_KEY = "rx3.buffer-size";
    private static final String AUTO_INSTALL_PLUGINS_KEY = "rxjava3.auto-install-plugins";
    
    // Default values
    private static final boolean DEFAULT_PERFORMANCE_MONITOR = false;
    private static final int DEFAULT_BUFFER_SIZE = 128;
    private static final boolean DEFAULT_AUTO_INSTALL = true;
    
    private RxJavaConfig() {
        throw new IllegalStateException("No instances!");
    }
    
    /**
     * Initialize RxJava with production-ready configuration.
     * Call this once during application startup.
     */
    public static void initialize() {
        // Auto-install performance monitoring if enabled
        if (isPerformanceMonitoringEnabled() && isAutoInstallEnabled()) {
            RxJavaPerformancePlugin.install();
        }
        
        // Set buffer size if configured
        int bufferSize = getBufferSize();
        if (bufferSize != DEFAULT_BUFFER_SIZE) {
            System.setProperty(BUFFER_SIZE_KEY, String.valueOf(bufferSize));
        }
    }
    
    /**
     * Check if performance monitoring is enabled.
     */
    public static boolean isPerformanceMonitoringEnabled() {
        return getBooleanProperty(PERFORMANCE_MONITOR_KEY, DEFAULT_PERFORMANCE_MONITOR);
    }
    
    /**
     * Get configured buffer size.
     */
    public static int getBufferSize() {
        return getIntProperty(BUFFER_SIZE_KEY, DEFAULT_BUFFER_SIZE);
    }
    
    /**
     * Check if auto-installation of plugins is enabled.
     */
    public static boolean isAutoInstallEnabled() {
        return getBooleanProperty(AUTO_INSTALL_PLUGINS_KEY, DEFAULT_AUTO_INSTALL);
    }
    
    /**
     * Enable performance monitoring at runtime.
     */
    public static void enablePerformanceMonitoring() {
        System.setProperty(PERFORMANCE_MONITOR_KEY, "true");
        RxJavaPerformancePlugin.enable();
    }
    
    /**
     * Disable performance monitoring at runtime.
     */
    public static void disablePerformanceMonitoring() {
        System.setProperty(PERFORMANCE_MONITOR_KEY, "false");
        RxJavaPerformancePlugin.disable();
    }
    
    /**
     * Set buffer size at runtime.
     */
    public static void setBufferSize(int size) {
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size must be positive");
        }
        System.setProperty(BUFFER_SIZE_KEY, String.valueOf(size));
    }
    
    /**
     * Get all current configuration as a formatted string.
     */
    public static String getConfigurationSummary() {
        return String.format(
            "RxJava Configuration:\n" +
            "  Performance Monitoring: %s\n" +
            "  Buffer Size: %d\n" +
            "  Auto Install Plugins: %s\n" +
            "  JVM: %s\n" +
            "  Available Processors: %d",
            isPerformanceMonitoringEnabled(),
            getBufferSize(),
            isAutoInstallEnabled(),
            System.getProperty("java.vm.name", "Unknown"),
            Runtime.getRuntime().availableProcessors()
        );
    }
    
    // Helper methods for property access
    private static boolean getBooleanProperty(String key, boolean defaultValue) {
        String value = System.getProperty(key);
        if (value == null) {
            // Check environment variables as fallback
            String envKey = key.toUpperCase().replace('.', '_');
            value = System.getenv(envKey);
        }
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }
    
    private static int getIntProperty(String key, int defaultValue) {
        String value = System.getProperty(key);
        if (value == null) {
            // Check environment variables as fallback
            String envKey = key.toUpperCase().replace('.', '_');
            value = System.getenv(envKey);
        }
        
        if (value != null) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                // Log warning and use default
                System.err.println("Invalid integer value for " + key + ": " + value + 
                                 ". Using default: " + defaultValue);
            }
        }
        return defaultValue;
    }
}
