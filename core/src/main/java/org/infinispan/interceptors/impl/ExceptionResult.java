package org.infinispan.interceptors.impl;

/**
 * @author Dan Berindei
 * @since 9.0
 */
class ExceptionResult implements ComplexResult {
   final Throwable throwable;

   ExceptionResult(Throwable throwable) {
      this.throwable = throwable;
   }
}
