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

package org.infinispan.jcache;

import javax.cache.event.CacheEntryEventFilter;
import javax.cache.event.CacheEntryListener;
import javax.cache.event.CacheEntryListenerRegistration;

/**
 * The reference implementation of the {@link CacheEntryListenerRegistration}.
 * 
 * @author Brian Oliver
 *
 * @param <K> the type of keys
 * @param <V> the type of values
 */
public class RICacheEntryListenerRegistration<K, V> implements CacheEntryListenerRegistration<K, V> {

    private CacheEntryListener<? super K, ? super V> listener;
    private CacheEntryEventFilter<? super K, ? super V> filter;
    private boolean isOldValueRequired;
    private boolean isSynchronous;
    
    /**
     * Constructs an {@link RICacheEntryListenerRegistration}.
     * 
     * @param listener            the {@link CacheEntryListener}
     * @param filter              the optional {@link CacheEntryEventFilter}
     * @param isOldValueRequired  if the old value is required for events with this listener
     * @param isSynchronous       if the listener should block the thread causing the event
     */
    public RICacheEntryListenerRegistration(CacheEntryListener<? super K, ? super V> listener, 
                                            CacheEntryEventFilter<? super K, ? super V> filter, 
                                            boolean isOldValueRequired, 
                                            boolean isSynchronous) {
        this.listener = listener;
        this.filter = filter;
        this.isOldValueRequired = isOldValueRequired;
        this.isSynchronous = isSynchronous;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CacheEntryEventFilter<? super K, ? super V> getCacheEntryFilter() {
        return filter;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public CacheEntryListener<? super K, ? super V> getCacheEntryListener() {
        return listener;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isOldValueRequired() {
        return isOldValueRequired;
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean isSynchronous() {
        return isSynchronous;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((filter == null) ? 0 : filter.hashCode());
        result = prime * result + (isOldValueRequired ? 1231 : 1237);
        result = prime * result + (isSynchronous ? 1231 : 1237);
        result = prime * result
                + ((listener == null) ? 0 : listener.hashCode());
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }
        if (object == null) {
            return false;
        }
        if (!(object instanceof RICacheEntryListenerRegistration)) {
            return false;
        }
        RICacheEntryListenerRegistration<?, ?> other = (RICacheEntryListenerRegistration<?, ?>) object;
        if (filter == null) {
            if (other.filter != null) {
                return false;
            }
        } else if (!filter.equals(other.filter)) {
            return false;
        }
        if (isOldValueRequired != other.isOldValueRequired) {
            return false;
        }
        if (isSynchronous != other.isSynchronous) {
            return false;
        }
        if (listener == null) {
            if (other.listener != null) {
                return false;
            }
        } else if (!listener.equals(other.listener)) {
            return false;
        }
        return true;
    }
}
