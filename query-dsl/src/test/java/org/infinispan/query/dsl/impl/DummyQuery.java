package org.infinispan.query.dsl.impl;

import org.infinispan.query.dsl.Query;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author anistor@redhat.com
 * @since 7.0
 */
class DummyQuery implements Query {

   @Override
   public Map<String, Object> getParameters() {
      return null;
   }

   @Override
   public Query setParameter(String paramName, Object paramValue) {
      return this;
   }

   @Override
   public Query setParameters(Map<String, Object> paramValues) {
      return null;
   }

   @Override
   public <T> List<T> list() {
      return Collections.emptyList();
   }

   @Override
   public int getResultSize() {
      return 0;
   }
}
