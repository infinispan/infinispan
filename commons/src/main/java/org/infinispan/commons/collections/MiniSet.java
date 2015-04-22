package org.infinispan.commons.collections;

import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Set optimized for few keys. The optimization is mostly memory-wise, reducing amount of objects created.
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class MiniSet<T> extends AbstractSet<T> implements Serializable {
    private static final Object DUMMY = new Object();
    private static final int ARRAY_SIZE = 8;
    public static final int MAX_OPTIMIZED_CAPACITY = ARRAY_SIZE;

    /*
     union { T, T[], Map<T, Object> }
     */
    private Object holder;
    protected Type type = Type.EMPTY;

    public MiniSet() {}

    public MiniSet(T... entries) {
        if (entries.length == 0) {
            type = Type.EMPTY;
        } else if (entries.length == 1) {
            holder = entries[0];
            if (holder == null) throw new IllegalArgumentException();
            type = Type.SINGLE;
        } else if (entries.length < ARRAY_SIZE) {
            holder = new Object[ARRAY_SIZE];
            type = Type.ARRAY;
            for (T entry : entries) {
                add(entry);
            }
        } else {
            Map<T, Object> map = new HashMap<>();
            for (T entry : entries) {
                if (entry == null) throw new IllegalArgumentException();
                map.put(entry, DUMMY);
            }
            holder = map;
            type = Type.MAP;
        }
    }

    public MiniSet(Collection<T> entries) {
        int size = entries.size();
        if (size == 0) {
            // nothing to do
        } else if (size == 1) {
            holder = entries.iterator().next();
            if (holder == null) throw new IllegalArgumentException();
            type = Type.SINGLE;
        } else if (size < ARRAY_SIZE) {
            holder = new Object[ARRAY_SIZE];
            type = Type.ARRAY;
            addAll(entries);
        } else {
            Map<T, Object> map = new HashMap<>();
            for (T entry : entries) {
                if (entry == null) throw new IllegalArgumentException();
                map.put(entry, DUMMY);
            }
            holder = map;
            type = Type.MAP;
        }
    }

    @Override
    public boolean contains(Object o) {
        switch (type) {
            case EMPTY:
                return false;
            case SINGLE:
                return holder.equals(o);
            case ARRAY:
                Object[] array = (Object[]) holder;
                for (int i = 0; i < array.length; ++i) {
                    if (array[i] == null) return false;
                    if (array[i].equals(o)) return true;
                }
                return false;
            case MAP:
                return ((Map<T, Object>) holder).containsKey(o);
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean add(T o) {
        if (o == null) throw new IllegalArgumentException("Must not be null");
        Object[] array;
        switch (type) {
            case EMPTY:
                holder = o;
                type = Type.SINGLE;
                return true;
            case SINGLE:
                if (holder.equals(o)) return false;
                array = new Object[ARRAY_SIZE];
                array[0] = holder;
                array[1] = o;
                holder = array;
                type = Type.ARRAY;
                return true;
            case ARRAY:
                array = (Object[]) holder;
                for (int i = 0; i < array.length; ++i) {
                    if (array[i] == null) {
                        array[i] = o;
                        return true;
                    } else if (array[i].equals(o)) {
                        return false;
                    }
                }
                Map<T, Object> map = new HashMap<>();
                for (Object e : array) {
                    if (e != null) map.put((T) e, DUMMY);
                }
                map.put(o, DUMMY);
                holder = map;
                type = Type.MAP;
                return true;
            case MAP:
                return ((Map<T, Object>) holder).put(o, DUMMY) == null;
        }
        throw new IllegalStateException();
    }

    @Override
    public boolean remove(Object o) {
        // never shrink
        switch (type) {
            case EMPTY:
                return false;
            case SINGLE:
                if (holder.equals(o)) {
                    holder = null; // to allow the object to be GCed
                    type = Type.EMPTY;
                    return true;
                } else {
                    return false;
                }
            case ARRAY:
                Object[] array = (Object[]) holder;
                for (int i = 0; i < array.length; ++i) {
                    if (array[i] == null) return false;
                    if (array[i].equals(o)) {
                        if (removeAt(array, i)) return true;
                        return true;
                    }
                }
                return false;
            case MAP:
                return ((Map<T, Object>) holder).remove(o) != null;
        }
        throw new IllegalStateException();
    }

    private static boolean removeAt(Object[] array, int i) {
        for (int j = array.length - 1; j > i; --j) {
            if (array[j] != null) {
                array[i] = array[j];
                array[j] = null;
                return true;
            }
        }
        array[i] = null;
        return false;
    }

    @Override
    public void clear() {
        holder = null;
        type = Type.EMPTY;
    }

    @Override
    public int size() {
        switch (type) {
            case EMPTY:
                return 0;
            case SINGLE:
                return 1;
            case ARRAY:
                int size = 0;
                for (Object o : (Object[]) holder) {
                    if (o != null) size++;
                }
                return size;
            case MAP:
                return ((Map<T, Object>) holder).size();
        }
        throw new IllegalStateException();
    }

    @Override
    public java.util.Iterator<T> iterator() {
        // note: these iterators don't check against modifications
        switch (type) {
            case EMPTY:
                return EmptyIterator.INSTANCE;
            case SINGLE:
                return new SingleIterator();
            case ARRAY:
                return new ArrayIterator();
            case MAP:
                return ((Map<T, Object>) holder).keySet().iterator();
        }
        throw new IllegalStateException();
    }

    protected static class EmptyIterator<T> implements java.util.Iterator<T> {
        public static final EmptyIterator INSTANCE = new EmptyIterator();

        @Override
        public boolean hasNext() {
            return false;
        }

        @Override
        public T next() {
            throw new NoSuchElementException();
        }

        @Override
        public void remove() {
            throw new IllegalStateException();
        }
    }

    protected class SingleIterator implements java.util.Iterator<T> {
        private boolean hasNext = true;

        private void assertType() {
            if (type != Type.SINGLE) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public T next() {
            assertType();
            if (hasNext) {
                hasNext = false;
                return (T) holder;
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            assertType();
            if (hasNext) {
                throw new IllegalStateException();
            } else {
                holder = null;
                type = Type.EMPTY;
            }
        }
    }

    protected class ArrayIterator implements java.util.Iterator<T> {
        private int index = 0;

        private void assertType() {
            if (type != Type.ARRAY) {
                throw new ConcurrentModificationException();
            }
        }

        @Override
        public boolean hasNext() {
            return index < ARRAY_SIZE && ((Object[]) holder)[index] != null;
        }

        @Override
        public T next() {
            assertType();
            Object[] array = (Object[]) holder;
            if (index < ARRAY_SIZE && array[index] != null) {
                return (T) array[index++];
            } else {
                throw new NoSuchElementException();
            }
        }

        @Override
        public void remove() {
            assertType();
            if (index == 0) {
                throw new IllegalStateException();
            } else {
                removeAt((Object[]) holder, index - 1);
            }
        }
    }

    protected enum Type {
        EMPTY,
        SINGLE,
        ARRAY,
        MAP
    }
}
