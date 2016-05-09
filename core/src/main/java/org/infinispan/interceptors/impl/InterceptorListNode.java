package org.infinispan.interceptors.impl;

import org.infinispan.interceptors.AsyncInterceptor;

/**
 * Node in a single-linked list of interceptors.
 * 
 * @author Dan Berindei
 * @since 9.0
 */
class InterceptorListNode {
   public final AsyncInterceptor interceptor;
   public final InterceptorListNode nextNode;

   public InterceptorListNode(AsyncInterceptor interceptor, InterceptorListNode next) {
      this.interceptor = interceptor;
      this.nextNode = next;
   }
}
