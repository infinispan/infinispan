package org.infinispan.query;

/**
 * The task of this interface is to convert keys from their original types to a String representation (which can be
 * used in Lucene) and vice versa.
 *
 * @author Manik Surtani
 * @since 4.0
 */
public interface Transformer {

   Object fromString(String s);

   String toString(Object customType);

}
