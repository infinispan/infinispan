package org.infinispan.commons.util.concurrent;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple Set implementation backed by a {@link java.util.concurrent.ConcurrentHashMap} to deal with the fact that the
 * JDK does not have a proper concurrent Set implementation that uses efficient lock striping.
 * <p/>
 * Note that values are stored as keys in the underlying Map, with a static dummy object as value.
 *
 * @author <a href="mailto:manik@jboss.org">Manik Surtani</a>
 * @since 4.0
 * @deprecated since 10.0, use {@link ConcurrentHashMap#newKeySet()} instead.
 */
@Deprecated
public class ConcurrentHashSet<E> extends AbstractSet<E> implements Serializable {

    /**
     * The serialVersionUID
     */
    private static final long serialVersionUID = 5312604953511379869L;
    /**
     * any Serializable object will do, Integer.valueOf(0) is known cheap
     **/
    private static final Serializable DUMMY = 0;
    protected final ConcurrentMap<E, Object> map;

    public ConcurrentHashSet() {
        map = new ConcurrentHashMap<>();
    }

    /**
     * @param concurrencyLevel passed in to the underlying CHM.  See {@link java.util.concurrent.ConcurrentHashMap#ConcurrentHashMap(int,
     *                         float, int)} javadocs for details.
     */
    public ConcurrentHashSet(int concurrencyLevel) {
        map = new ConcurrentHashMap<>(16, 0.75f, concurrencyLevel);
    }

    /**
     * Params passed in to the underlying CHM.  See {@link java.util.concurrent.ConcurrentHashMap#ConcurrentHashMap(int,
     * float, int)} javadocs for details.
     */
    public ConcurrentHashSet(int initSize, float loadFactor, int concurrencyLevel) {
        map = new ConcurrentHashMap<>(initSize, loadFactor, concurrencyLevel);
    }


    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return map.containsKey(o);
    }

    @Override
    public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return map.keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return map.keySet().toArray(a);
    }

    @Override
    public boolean add(E o) {
        Object v = map.put(o, DUMMY);
        return v == null;
    }

    @Override
    public boolean remove(Object o) {
        Object v = map.remove(o);
        return v != null;
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return map.keySet().containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends E> c) {
        throw new UnsupportedOperationException("Not supported in this implementation since additional locking is required and cannot directly be delegated to multiple calls to ConcurrentHashMap");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not supported in this implementation since additional locking is required and cannot directly be delegated to multiple calls to ConcurrentHashMap");
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException("Not supported in this implementation since additional locking is required and cannot directly be delegated to multiple calls to ConcurrentHashMap");
    }

    @Override
    public void clear() {
        map.clear();
    }
}
