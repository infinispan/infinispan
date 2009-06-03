package org.infinispan.loaders.jdbm;

import java.util.Comparator;

import org.infinispan.CacheException;
import org.infinispan.loaders.LockSupportCacheStoreConfig;
import org.infinispan.marshall.Marshaller;
import org.infinispan.util.Util;

/**
 * Configures {@link JdbmCacheStore}.
 * <p/>
 * <ul>
 * <li><tt>location</tt> - a location on disk where the store can write internal
 * files.</li>
 * <li><tt>comparatorClassName</tt> - comparator class used to sort the keys
 * by the cache loader. This should only need to be set when using keys that
 * do not have a natural ordering.
 * </ul>
 * 
 * @author Elias Ross
 * @since 4.0
 */
public class JdbmCacheStoreConfig extends LockSupportCacheStoreConfig {

    private static final long serialVersionUID = 1L;
    
    String location = "jdbm";
    String comparatorClassName = NaturalComparator.class.getName();

    public JdbmCacheStoreConfig() {
        setCacheLoaderClassName(JdbmCacheStore.class.getName());
    }

    public String getLocation() {
        return location;
    }

    public void setLocation(String location) {
        testImmutability("location");
        this.location = location;
    }

    /**
     * Returns comparatorClassName.
     */
    public String getComparatorClassName() {
        return comparatorClassName;
    }

    /**
     * Sets comparatorClassName.
     */
    public void setComparatorClassName(String comparatorClassName) {
        this.comparatorClassName = comparatorClassName;
    }

    /**
     * Returns a new comparator instance based on {@link #setComparatorClassName(String)}.
     */
    public Comparator createComparator() {
        try {
            return (Comparator) Util.getInstance(comparatorClassName);
        } catch (Exception e) {
            throw new CacheException(e);
        }
    }

}
