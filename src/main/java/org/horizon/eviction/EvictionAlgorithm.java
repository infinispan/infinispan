/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2000 - 2008, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.horizon.eviction;

import org.horizon.Cache;
import org.horizon.container.DataContainer;
import org.horizon.eviction.events.EvictionEvent;
import org.horizon.eviction.events.EvictionEvent.Type;
import org.horizon.lifecycle.Lifecycle;

import java.util.concurrent.BlockingQueue;

/**
 * Interface for all eviction algorithms.  There is no requirement for implementations to be thread safe, as the {@link
 * org.horizon.eviction.EvictionManager} will guarantee that only a single thread will access the algorithm
 * implementation at any given time.
 *
 * @author Manik Surtani
 * @since 1.0
 */
public interface EvictionAlgorithm extends Lifecycle {
   /**
    * Entry point for eviction algorithm.  Invoking this will cause the algorithm to process the queue of {@link
    * org.horizon.eviction.events.EvictionEvent}s passed in.
    *
    * @param queue blocking queue of {@link org.horizon.eviction.events.EvictionEvent}s to process.
    * @throws EvictionException if there is a problem processing any of these events
    */
   void process(BlockingQueue<EvictionEvent> queue) throws EvictionException;

   /**
    * Reset the eviction queue.
    */
   void resetEvictionQueue();

   /**
    * Sets the eviction action policy, so the algorithm knows what to do when an entry is to be evicted.
    *
    * @param evictionAction eviction action instance to use
    */
   void setEvictionAction(EvictionAction evictionAction);

   /**
    * Initializes the algorithm instance by passing in the cache instance, data container and configuration
    *
    * @param cache                   cache to work with
    * @param dataContiner            the cache's data container
    * @param evictionAlgorithmConfig algorithm configuration to use
    */
   void init(Cache<?, ?> cache, DataContainer dataContiner,
             EvictionAlgorithmConfig evictionAlgorithmConfig);

   /**
    * Tests whether the algorithm would ignore certain event types.  This is an optimization on the {@link
    * org.horizon.eviction.EvictionManager} so that events that would eventually be ignored in {@link
    * #process(java.util.concurrent.BlockingQueue)} would not be added to the event queue, keeping the queue smaller and
    * leaving more space to deal with more important event types.
    *
    * @param eventType event type to test for
    * @return true if the event representing the parameters would be ignored by this algorithm or not.
    */
   boolean canIgnoreEvent(Type eventType);

   /**
    * @return the type of the {@link EvictionAlgorithmConfig} bean used to configure this implementation of {@link
    *         org.horizon.eviction.EvictionAlgorithm}
    */
   Class<? extends EvictionAlgorithmConfig> getConfigurationClass();
}
