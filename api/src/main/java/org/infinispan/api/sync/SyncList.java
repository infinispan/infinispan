package org.infinispan.api.sync;

import java.util.Collection;

/**
 * @since 15.0
 **/
public interface SyncList<V> {

   String name();

   SyncContainer container();

   void offerLast(V value);

   void offerLast(V value, long count);

   void offerFirst(V value);

   void offerFirst(V value, long count);

   long size();

   Collection<V> subList(int from, int to);

   //
}