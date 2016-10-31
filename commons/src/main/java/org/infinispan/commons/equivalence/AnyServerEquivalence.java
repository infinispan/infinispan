package org.infinispan.commons.equivalence;

import java.util.Arrays;

/**
 * AnyServerEquivalence. Works for both objects and byte[]
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class AnyServerEquivalence implements Equivalence<Object> {

    public static final Equivalence<Object> INSTANCE = new AnyServerEquivalence();

    private static boolean isByteArray(Object obj) {
        return byte[].class == obj.getClass();
    }

    @Override
    public int hashCode(Object obj) {
        if (isByteArray(obj)) {
            return 41 + Arrays.hashCode((byte[]) obj);
        } else {
            return obj.hashCode();
        }
    }

    @Override
    public boolean equals(Object obj, Object otherObj) {
        if (obj == otherObj)
            return true;
        if (obj == null || otherObj == null)
            return false;
        if (isByteArray(obj) && isByteArray(otherObj))
            return Arrays.equals((byte[]) obj, (byte[]) otherObj);
        return obj.equals(otherObj);
    }

    @Override
    public String toString(Object obj) {
        if (isByteArray(obj))
            return Arrays.toString((byte[]) obj);
        else
            return obj.toString();
    }

    @Override
    public boolean isComparable(Object obj) {
        return obj instanceof Comparable;
    }

    @Override
    @SuppressWarnings("unchecked")
    public int compare(Object obj, Object otherObj) {
       return ((Comparable<Object>) obj).compareTo(otherObj);
    }

}
