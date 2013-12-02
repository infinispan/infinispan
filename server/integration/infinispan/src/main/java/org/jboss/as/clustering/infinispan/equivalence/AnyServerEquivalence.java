/*
 * JBoss, Home of Professional Open Source
 * Copyright 2013 Red Hat Inc. and/or its affiliates and other contributors
 * as indicated by the @author tags. All rights reserved.
 * See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This copyrighted material is made available to anyone wishing to use,
 * modify, copy, or redistribute it subject to the terms and conditions
 * of the GNU Lesser General Public License, v. 2.1.
 * This program is distributed in the hope that it will be useful, but WITHOUT A
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A
 * PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more details.
 * You should have received a copy of the GNU Lesser General Public License,
 * v.2.1 along with this distribution; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 * MA  02110-1301, USA.
 */
package org.jboss.as.clustering.infinispan.equivalence;

import java.util.Arrays;

import org.infinispan.commons.equivalence.Equivalence;

/**
 * AnyServerEquivalence. Works for both objects and byte[]
 *
 * @author Tristan Tarrant
 * @since 5.3
 */
public class AnyServerEquivalence implements Equivalence<Object> {

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
