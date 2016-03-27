package org.infinispan.interceptors.impl;

import org.infinispan.interceptors.SequentialInterceptor;

/**
 * Node in a single-linked list of interceptors.
 * 
 * @author Dan Berindei
 * @since 9.0
 */
public class InterceptorListNode {
   public final SequentialInterceptor interceptor;
   public final InterceptorListNode nextNode;

   public InterceptorListNode(SequentialInterceptor interceptor, InterceptorListNode next) {
      this.interceptor = interceptor;
      this.nextNode = next;
   }
}
