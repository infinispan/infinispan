package org.infinispan.cdi.common.util;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * A collection of utilities for working with Arrays that goes beyond that in
 * the JDK.
 *
 * @author Pete Muir
 */
public class Arrays2 {

    private Arrays2() {
    }

    /**
     * Create a set from an array. If the array contains duplicate objects, the
     * last object in the array will be placed in resultant set.
     *
     * @param <T>   the type of the objects in the set
     * @param array the array from which to create the set
     * @return the created sets
     */
    public static <T> Set<T> asSet(T... array) {
        Set<T> result = new HashSet<T>();
        Collections.addAll(result, array);
        return result;
    }

}
