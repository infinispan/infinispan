package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Query;

import java.util.Collections;
import java.util.List;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
public class DummyQuery implements Query {

   @Override
   public <T> List<T> list() {
      return Collections.emptyList();
   }

   @Override
   public int getResultSize() {
      return 0;
   }
}
