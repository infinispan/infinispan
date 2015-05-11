package org.infinispan.commons.collections;

import java.util.Collection;

/**
 * Synchronized version of {@link MiniSet}.
 *
 * Note that iterators are not synchronized ({@link java.util.Collections.synchronizedSet()} does not provide that neither).
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class SynchronizedMiniSet<T> extends MiniSet<T> {
    public SynchronizedMiniSet() {
        super();
    }

    public SynchronizedMiniSet(T... entries) {
        super(entries);
    }

    @Override
    public synchronized boolean contains(Object o) {
        return super.contains(o);
    }

    @Override
    public synchronized boolean add(T o) {
        return super.add(o);
    }

    @Override
    public synchronized boolean remove(Object o) {
        return super.remove(o);
    }

    @Override
    public synchronized void clear() {
        super.clear();
    }

    @Override
    public synchronized int size() {
        return super.size();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) return true;
        synchronized (this) {
            return super.equals(o);
        }
    }

    @Override
    public synchronized int hashCode() {
        return super.hashCode();
    }

    @Override
    public synchronized boolean removeAll(Collection<?> c) {
        return super.removeAll(c);
    }

    @Override
    public synchronized Object[] toArray() {
        return super.toArray();
    }

    @Override
    public synchronized <T1> T1[] toArray(T1[] a) {
        return super.toArray(a);
    }

    @Override
    public synchronized boolean containsAll(Collection<?> c) {
        return super.containsAll(c);
    }

    @Override
    public synchronized boolean addAll(Collection<? extends T> c) {
        return super.addAll(c);
    }

    @Override
    public synchronized boolean retainAll(Collection<?> c) {
        return super.retainAll(c);
    }
}
