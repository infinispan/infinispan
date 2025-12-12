package org.infinispan.spring.embedded.provider.sample.generators;

import java.lang.reflect.Method;

import org.springframework.cache.interceptor.KeyGenerator;
import org.springframework.stereotype.Component;

/**
 * Simple implementation of {@link KeyGenerator} interface. It returns the first
 * argument passed to a method.
 *
 * @author Matej Cimbora (mcimbora@redhat.com)
 */
@Component
public class SingleArgKeyGenerator implements KeyGenerator {

   @Override
   public Object generate(Object o, Method method, Object... params) {
      if (params != null && params.length == 1) {
         return params[0];
      } else {
         throw new IllegalArgumentException("This generator requires exactly one parameter to be specified.");
      }
   }
}
