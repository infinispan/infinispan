package org.infinispan.query.dsl.impl;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.infinispan.query.dsl.Query;

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

   @Override
   public String[] getProjection() {
      return new String[0];
   }

   @Override
   public Query startOffset(long startOffset) {
      return this;
   }

   @Override
   public Query maxResults(int maxResults) {
      return this;
   }
}
