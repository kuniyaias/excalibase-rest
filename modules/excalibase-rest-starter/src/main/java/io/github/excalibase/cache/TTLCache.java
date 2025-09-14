package io.github.excalibase.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Generic TTL (Time-To-Live) cache implementation with automatic expiration.
 *
 * <p>This cache automatically expires entries after a configured duration and provides
 * methods for cache management and statistics. It's thread-safe and uses a background
 * scheduler to periodically clean up expired entries.</p>
 *
 * <p>Key features:</p>
 * <ul>
 *   <li>Automatic expiration based on configurable TTL</li>
 *   <li>Thread-safe concurrent access</li>
 *   <li>Periodic cleanup of expired entries</li>
 *   <li>Cache statistics and management</li>
 *   <li>Compute-if-absent functionality</li>
 * </ul>
 *
 * @param <K> the type of keys maintained by this cache
 * @param <V> the type of cached values
 */
public class TTLCache<K, V> {
    private static final Logger log = LoggerFactory.getLogger(TTLCache.class);

    private final Duration ttl;
    private final ConcurrentHashMap<K, CacheEntry<V>> cache = new ConcurrentHashMap<>();
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    /**
     * Creates a new TTL cache with the specified time-to-live duration.
     *
     * @param ttl the duration after which cache entries expire
     */
    public TTLCache(Duration ttl) {
        this.ttl = ttl;
        // Schedule periodic cleanup every minute
        scheduler.scheduleAtFixedRate(this::cleanupExpiredEntries, 1, 1, TimeUnit.MINUTES);
    }

    /**
     * Associates the specified value with the specified key in this cache.
     *
     * @param key the key with which the specified value is to be associated
     * @param value the value to be associated with the specified key
     * @return the previous value associated with key, or null if there was no mapping
     */
    public V put(K key, V value) {
        CacheEntry<V> newEntry = new CacheEntry<>(value, Instant.now().plus(ttl));
        CacheEntry<V> oldEntry = cache.put(key, newEntry);
        return oldEntry != null ? oldEntry.value : null;
    }

    /**
     * Returns the value to which the specified key is mapped, or null if expired or not present.
     *
     * @param key the key whose associated value is to be returned
     * @return the value to which the specified key is mapped, or null if expired/not present
     */
    public V get(K key) {
        CacheEntry<V> entry = cache.get(key);
        if (entry == null) {
            return null;
        }

        if (entry.isExpired()) {
            cache.remove(key);
            return null;
        }

        return entry.value;
    }

    /**
     * If the specified key is not already associated with a value (or is expired),
     * attempts to compute its value using the given mapping function.
     *
     * @param key the key with which the specified value is to be associated
     * @param mappingFunction the function to compute a value
     * @return the current (existing or computed) value associated with the specified key
     */
    public V computeIfAbsent(K key, Function<? super K, ? extends V> mappingFunction) {
        CacheEntry<V> entry = cache.get(key);

        // Check if entry exists and is not expired
        if (entry != null && !entry.isExpired()) {
            return entry.value;
        }

        // Remove expired entry if present
        if (entry != null) {
            cache.remove(key);
        }

        // Compute new value
        V newValue = mappingFunction.apply(key);
        if (newValue != null) {
            put(key, newValue);
        }

        return newValue;
    }

    /**
     * Removes the mapping for a key from this cache if it is present.
     *
     * @param key the key whose mapping is to be removed from the cache
     * @return the previous value associated with key, or null if there was no mapping
     */
    public V remove(K key) {
        CacheEntry<V> entry = cache.remove(key);
        return entry != null ? entry.value : null;
    }

    /**
     * Returns the number of key-value mappings in this cache (including expired entries).
     *
     * @return the number of key-value mappings in this cache
     */
    public int size() {
        return cache.size();
    }

    /**
     * Returns true if this cache contains no key-value mappings.
     *
     * @return true if this cache contains no key-value mappings
     */
    public boolean isEmpty() {
        return cache.isEmpty();
    }

    /**
     * Removes all mappings from this cache.
     */
    public void clear() {
        cache.clear();
    }

    /**
     * Returns true if this cache contains a mapping for the specified key and it's not expired.
     *
     * @param key the key whose presence in this cache is to be tested
     * @return true if this cache contains a non-expired mapping for the specified key
     */
    public boolean containsKey(K key) {
        CacheEntry<V> entry = cache.get(key);
        if (entry == null) {
            return false;
        }

        if (entry.isExpired()) {
            cache.remove(key);
            return false;
        }

        return true;
    }

    /**
     * Removes expired entries from the cache.
     * This is called automatically by the background scheduler.
     */
    private void cleanupExpiredEntries() {
        try {
            int initialSize = cache.size();
            cache.entrySet().removeIf(entry -> entry.getValue().isExpired());
            int finalSize = cache.size();

            if (initialSize != finalSize) {
                log.debug("TTL cache cleanup: removed {} expired entries, {} entries remaining",
                         initialSize - finalSize, finalSize);
            }
        } catch (Exception e) {
            log.warn("Error during TTL cache cleanup: {}", e.getMessage());
        }
    }

    /**
     * Shuts down the cache and its background scheduler.
     * This should be called when the cache is no longer needed.
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
        clear();
    }

    /**
     * Returns cache statistics including size, TTL duration, and expired entry count.
     *
     * @return a map containing cache statistics
     */
    public CacheStats getStats() {
        int totalEntries = cache.size();
        long expiredEntries = cache.values().stream()
                .mapToLong(entry -> entry.isExpired() ? 1 : 0)
                .sum();

        return new CacheStats(totalEntries, (int) expiredEntries, ttl);
    }

    /**
     * Internal class representing a cache entry with expiration time.
     */
    private static class CacheEntry<V> {
        final V value;
        final Instant expiryTime;

        CacheEntry(V value, Instant expiryTime) {
            this.value = value;
            this.expiryTime = expiryTime;
        }

        boolean isExpired() {
            return Instant.now().isAfter(expiryTime);
        }
    }

    /**
     * Cache statistics holder.
     */
    public static class CacheStats {
        private final int totalEntries;
        private final int expiredEntries;
        private final Duration ttl;

        public CacheStats(int totalEntries, int expiredEntries, Duration ttl) {
            this.totalEntries = totalEntries;
            this.expiredEntries = expiredEntries;
            this.ttl = ttl;
        }

        public int getTotalEntries() {
            return totalEntries;
        }

        public int getExpiredEntries() {
            return expiredEntries;
        }

        public int getValidEntries() {
            return totalEntries - expiredEntries;
        }

        public Duration getTtl() {
            return ttl;
        }

        @Override
        public String toString() {
            return String.format("CacheStats{total=%d, valid=%d, expired=%d, ttl=%s}",
                               totalEntries, getValidEntries(), expiredEntries, ttl);
        }
    }
}