package org.infinispan.interceptors;

import org.infinispan.interceptors.base.SequentialInterceptor;

import java.util.List;

/**
 * @author Dan Berindei
 * @since 8.0
 */
public interface SequentialInterceptorChain {
   List<SequentialInterceptor> getInterceptors();
}
