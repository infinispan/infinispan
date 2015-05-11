package org.infinispan.commons.collections;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.util.AbstractList;
import java.util.Arrays;
import java.util.RandomAccess;

/**
 * Collection optimized for holding single element without allocating further objects
 *
 * @author Radim Vansa &lt;rvansa@redhat.com&gt;
 */
public class MiniList<T> extends AbstractList<T> implements RandomAccess, Serializable {
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;
    private final static int INIT_ARRAY_SIZE = 8;
    private final static Object[] EMPTY_ARRAY = new Object[0];

    /* union { T, T[] } */
    private Object holder;
    private int size;
    private Type type = Type.EMPTY;

    protected boolean equal(Object o1, Object o2) {
        return (o1 == null && o2 == null) || o1.equals(o2);
    }

    @Override
    public boolean add(T t) {
        Object[] array;
        modCount++;
        switch (type) {
            case EMPTY:
                holder = t;
                type = Type.SINGLE;
                size = 1;
                return true;
            case SINGLE:
                array = new Object[INIT_ARRAY_SIZE];
                array[0] = holder;
                array[1] = t;
                holder = array;
                size = 2;
                type = Type.ARRAY;
                return true;
            case ARRAY:
                array = ensureCapacity(size + 1);
                array[size] = t;
                size++;
                return true;
        }
        throw new IllegalStateException();
    }

    @Override
    public T set(int index, T element) {
        checkBounds(index);
        modCount++;
        T tmp;
        switch (type) {
            case SINGLE:
                tmp = (T) holder;
                return tmp;
            case ARRAY:
                Object[] array = (Object[]) this.holder;
                tmp = (T) array[index];
                array[index] = element;
                return tmp;
        }
        throw new IllegalStateException();
    }

    private void checkBounds(int index) {
        if (index >= size || index < 0) {
            throw outOfBounds(index);
        }
    }

    private void checkBoundsForAdd(int index) {
        if (index > size || index < 0) {
            throw outOfBounds(index);
        }
    }

    private IndexOutOfBoundsException outOfBounds(int index) {
        return new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
    }

    @Override
    public int indexOf(Object o) {
        switch (type) {
            case EMPTY:
                return -1;
            case SINGLE:
                return equal(holder, o) ? 0 : -1;
            case ARRAY:
                Object[] array = (Object[]) holder;
                for (int i = 0; i < size; ++i) {
                    if (equal(array[i], o)) return i;
                }
                return -1;
        }
        throw new IllegalStateException();
    }

    @Override
    public int lastIndexOf(Object o) {
        switch (type) {
            case EMPTY:
                return -1;
            case SINGLE:
                return equal(holder, o) ? 0 : -1;
            case ARRAY:
                Object[] array = (Object[]) holder;
                for (int i = size - 1; i >= 0; --i) {
                    if (equal(array[i], o)) return i;
                }
                return -1;
        }
        throw new IllegalStateException();
    }

    @Override
    public void clear() {
        modCount++;
        size = 0;
        holder = null;
        type = Type.EMPTY;
    }

    @Override
    public boolean contains(Object o) {
        switch (type) {
            case EMPTY:
                return false;
            case SINGLE:
                return equal(holder, o);
            case ARRAY:
                Object[] array = (Object[]) holder;
                for (int i = 0; i < size; ++i) {
                    if (equal(array[i], o)) return true;
                }
                return false;
        }
        throw new IllegalStateException();
    }

    @Override
    public T remove(int index) {
        checkBounds(index);
        modCount++;
        T tmp;
        switch (type) {
            case SINGLE:
                tmp = (T) holder;
                holder = null;
                size = 0;
                type = Type.EMPTY;
                return tmp;
            case ARRAY:
                Object[] array = (Object[]) holder;
                tmp = (T) array[index];
                System.arraycopy(array, index + 1, array, index, size - index - 1);
                array[--size] = null; // for GC
                return tmp;
        }
        throw new IllegalStateException();
    }

    @Override
    public void add(int index, T element) {
        checkBoundsForAdd(index);
        modCount++;
        Object[] array;
        switch (type) {
            case EMPTY:
                holder = element;
                type = Type.SINGLE;
                size = 1;
                return;
            case SINGLE:
                array = new Object[INIT_ARRAY_SIZE];
                if (index == 0) {
                    array[0] = element;
                    array[1] = holder;
                } else {
                    array[0] = holder;
                    array[1] = element;
                }
                holder = array;
                size = 2;
                type = Type.ARRAY;
                return;
            case ARRAY:
                array = ensureCapacity(size + 1);
                System.arraycopy(array, index, array, index + 1, size - index);
                array[index] = element;
                size++;
                return;
        }
        throw new IllegalStateException();
    }

    private Object[] ensureCapacity(int capacity) {
        Object[] array = (Object[]) holder;
        if (capacity > array.length) {
            int newCapacity = array.length << 1;
            if (newCapacity < 0) newCapacity = MAX_ARRAY_SIZE;
            if (capacity < 0 || capacity > newCapacity) {
                 throw new OutOfMemoryError();
            }
            array = Arrays.copyOf(array, Math.max(capacity, newCapacity));
            holder = array;
        }
        return array;
    }

    @Override
    public boolean remove(Object o) {
        Object[] array;
        switch (type) {
            case EMPTY:
                return false;
            case SINGLE:
                if (equal(holder, o)) {
                    holder = null;
                    type = Type.EMPTY;
                    size = 0;
                    modCount++;
                    return true;
                } else {
                    return false;
                }
            case ARRAY:
                array = (Object[]) holder;
                for (int i = 0; i < size; ++i) {
                    if (equal(array[i], o)) {
                        System.arraycopy(array, i + 1, array, i, size - i - 1);
                        array[--size] = null;
                        modCount++;
                        return true;
                    }
                }
                return false;
        }
        throw new IllegalStateException();
    }

    @Override
    public T get(int index) {
        checkBounds(index);
        switch (type) {
            case SINGLE:
                return (T) holder;
            case ARRAY:
                return (T) ((Object[]) holder)[index];
        }
        throw new IllegalStateException();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Object[] toArray() {
        switch (type) {
            case EMPTY:
                return EMPTY_ARRAY;
            case SINGLE:
                return new Object[] { holder };
            case ARRAY:
                return Arrays.copyOf((Object[]) holder, size);
        }
        throw new IllegalStateException();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        T1[] newArray = a.length >= size ? a : (T1[]) Array.newInstance(a.getClass().getComponentType(), size);
        switch (type) {
            case EMPTY:
                if (newArray.length > 0) newArray[0] = null;
                return newArray;
            case SINGLE:
                newArray[0] = (T1) holder;
                if (newArray.length > 1) newArray[1] = null;
                return newArray;
            case ARRAY:
                System.arraycopy(holder, 0, newArray, 0, size);
                if (newArray.length > size) newArray[size] = null;
                return newArray;
        }
        throw new IllegalStateException();
    }

    private enum Type {
        EMPTY,
        SINGLE,
        ARRAY
    }
}
