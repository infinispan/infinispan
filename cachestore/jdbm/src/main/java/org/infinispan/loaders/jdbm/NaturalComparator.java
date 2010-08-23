package org.infinispan.loaders.jdbm;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Compares keys using their <i>natural ordering</i>.
 * <p/>
 * 
 * @author Elias Ross
 */
public class NaturalComparator implements Comparator, Serializable {

    private static final long serialVersionUID = 1L;

    public int compare(Object o1, Object o2) {
        return ((Comparable)o1).compareTo(o2);
    }

}
