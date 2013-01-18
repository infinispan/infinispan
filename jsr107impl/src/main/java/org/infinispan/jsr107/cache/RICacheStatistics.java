/**
 *  Copyright 2011 Terracotta, Inc.
 *  Copyright 2011 Oracle America Incorporated
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.infinispan.jsr107.cache;

import javax.cache.Cache;
import javax.cache.CacheStatistics;
import java.io.Serializable;
import java.util.Date;
import java.util.concurrent.atomic.AtomicLong;


/**
 * The reference implementation of {@link CacheStatistics}.
 */
public class RICacheStatistics implements CacheStatistics, Serializable {

    private static final long serialVersionUID = -5589437411679003894L;
    private static final long NANOSECONDS_IN_A_MILLISECOND = 1000000L;


    private transient Cache<?, ?> cache;

    private final AtomicLong cacheRemovals = new AtomicLong();
    private final AtomicLong cacheExpiries = new AtomicLong();
    private final AtomicLong cachePuts = new AtomicLong();
    private final AtomicLong cacheHits = new AtomicLong();
    private final AtomicLong cacheMisses = new AtomicLong();
    private final AtomicLong cacheEvictions = new AtomicLong();
    private final AtomicLong cachePutTimeTakenNanos = new AtomicLong();
    private final AtomicLong cacheGetTimeTakenNanos = new AtomicLong();
    private final AtomicLong cacheRemoveTimeTakenNanos = new AtomicLong();

    private Date lastCollectionStartDate = new Date();

    /**
     * Constructs a cache statistics object
     *
     * @param cache the associated cache
     */
    public RICacheStatistics(Cache<?, ?> cache) {
        this.cache = cache;
    }

    /**
     * {@inheritDoc}
     * <p/>
     * Statistics will also automatically be cleared if internal counters overflow.
     */
    @Override
    public void clear() {
        cachePuts.set(0);
        cacheMisses.set(0);
        cacheRemovals.set(0);
        cacheExpiries.set(0);
        cacheHits.set(0);
        cacheEvictions.set(0);
        cacheGetTimeTakenNanos.set(0);
        cachePutTimeTakenNanos.set(0);
        cacheRemoveTimeTakenNanos.set(0);
        lastCollectionStartDate = new Date();
    }

    /**
     * The date from which statistics have been accumulated. Because statistics can be cleared, this is not necessarily
     * since the cache was started.
     *
     * @return the date statistics started being accumulated
     */
    @Override
    public Date getStartAccumulationDate() {
        return lastCollectionStartDate;
    }

    /**
     * @return the entry count
     */
    public long getEntryCount() {
        return ((InfinispanCache<?, ?>) cache).size();
    }

    /**
     * @return the number of hits
     */
    @Override
    public long getCacheHits() {
        return cacheHits.longValue();
    }

    /**
     * Returns cache hits as a percentage of total gets.
     *
     * @return the percentage of successful hits, as a decimal
     */
    @Override
    public float getCacheHitPercentage() {
        return getCacheHits() / getCacheGets();
    }

    /**
     * @return the number of misses
     */
    @Override
    public long getCacheMisses() {
        return cacheMisses.longValue();
    }

    /**
     * Returns cache misses as a percentage of total gets.
     *
     * @return the percentage of accesses that failed to find anything
     */
    @Override
    public float getCacheMissPercentage() {
        return getCacheMisses() / getCacheGets();
    }

    /**
     * The total number of requests to the cache. This will be equal to the sum of the hits and misses.
     * <p/>
     * A "get" is an operation that returns the current or previous value.
     *
     * @return the number of hits
     */
    @Override
    public long getCacheGets() {
        return getCacheHits() + getCacheMisses();
    }

    /**
     * The total number of puts to the cache.
     * <p/>
     * A put is counted even if it is immediately evicted. A replace includes a put and remove.
     *
     * @return the number of hits
     */
    @Override
    public long getCachePuts() {
        return cachePuts.longValue();
    }

    /**
     * The total number of removals from the cache. This does not include evictions, where the cache itself
     * initiates the removal to make space.
     * <p/>
     * A replace invcludes a put and remove.
     *
     * @return the number of hits
     */
    @Override
    public long getCacheRemovals() {
        return cacheRemovals.longValue();
    }

    /**
     * @return the number of evictions from the cache
     */
    @Override
    public long getCacheEvictions() {
        return cacheEvictions.longValue();
    }

    /**
     * The mean time to execute gets.
     *
     * @return the time in milliseconds
     */
    @Override
    public float getAverageGetMillis() {
        return (cacheGetTimeTakenNanos.longValue() / getCacheGets()) / NANOSECONDS_IN_A_MILLISECOND;
    }

    /**
     * The mean time to execute puts.
     *
     * @return the time in milliseconds
     */
    @Override
    public float getAveragePutMillis() {
        return (cachePutTimeTakenNanos.longValue() / getCacheGets()) / NANOSECONDS_IN_A_MILLISECOND;
    }

    /**
     * The mean time to execute removes.
     *
     * @return the time in milliseconds
     */
    @Override
    public float getAverageRemoveMillis() {
        return (cacheRemoveTimeTakenNanos.longValue() / getCacheGets()) / NANOSECONDS_IN_A_MILLISECOND;
    }

    //package local incrementers

    /**
     * Increases the counter by the number specified.
     * @param number the number to increase the counter by
     */
    void increaseCacheRemovals(long number) {
        cacheRemovals.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     * @param number the number to increase the counter by
     */
    void increaseCacheExpiries(long number) {
        cacheExpiries.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     * @param number the number to increase the counter by
     */
    void increaseCachePuts(long number) {
        cachePuts.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     * @param number the number to increase the counter by
     */
    void increaseCacheHits(long number) {
        cacheHits.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     * @param number the number to increase the counter by
     */
    void increaseCacheMisses(long number) {
        cacheMisses.getAndAdd(number);
    }

    /**
     * Increases the counter by the number specified.
     * @param number the number to increase the counter by
     */
    void increaseCacheEvictions(long number) {
        cacheEvictions.getAndAdd(number);
    }

    /**
     * Increments the get time accumulator
     * @param duration the time taken in nanoseconds
     */
    public void addGetTimeNano(long duration) {
        if (cacheGetTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            cacheGetTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            cacheGetTimeTakenNanos.set(duration);
        }
    }


    /**
     * Increments the put time accumulator
     * @param duration the time taken in nanoseconds
     */
    public void addPutTimeNano(long duration) {
        if (cachePutTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            cachePutTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            cachePutTimeTakenNanos.set(duration);
        }
    }

    /**
     * Increments the remove time accumulator
     * @param duration the time taken in nanoseconds
     */
    public void addRemoveTimeNano(long duration) {
        if (cacheRemoveTimeTakenNanos.get() <= Long.MAX_VALUE - duration) {
            cacheRemoveTimeTakenNanos.addAndGet(duration);
        } else {
            //counter full. Just reset.
            clear();
            cacheRemoveTimeTakenNanos.set(duration);
        }
    }

}
