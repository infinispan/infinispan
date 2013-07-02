package org.infinispan.loaders.jdbm;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares keys using their <i>natural ordering</i>.
 * <p/>
 * 
 * @author Elias Ross
 */
public class NaturalComparator<T> implements Comparator<T>, Serializable {

    private static final long serialVersionUID = 1L;

    @Override
    public int compare(T o1, T o2) {
        return ((Comparable<T>)o1).compareTo(o2);
    }

}
