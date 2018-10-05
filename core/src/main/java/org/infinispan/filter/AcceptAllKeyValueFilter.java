package org.infinispan.filter;

import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.marshall.core.Ids;
import org.infinispan.metadata.Metadata;

/**
 * A key value filter that accepts all entries found.
 * <p>
 * <b>This filter should be used carefully as it may cause the operation to perform very slowly
 * as all entries are accepted.</b>
 *
 * @author wburns
 * @since 7.0
 */
public final class AcceptAllKeyValueFilter implements KeyValueFilter<Object, Object> {

   private AcceptAllKeyValueFilter() {
   }

   private static class StaticHolder {
      private static final AcceptAllKeyValueFilter INSTANCE = new AcceptAllKeyValueFilter();
   }

   public static AcceptAllKeyValueFilter getInstance() {
      return StaticHolder.INSTANCE;
   }

   @Override
   public boolean accept(Object key, Object value, Metadata metadata) {
      return true;
   }

   public static final class Externalizer extends AbstractExternalizer<AcceptAllKeyValueFilter> {
      @Override
      public Set<Class<? extends AcceptAllKeyValueFilter>> getTypeClasses() {
         return Collections.singleton(AcceptAllKeyValueFilter.class);
      }

      @Override
      public void writeObject(ObjectOutput output, AcceptAllKeyValueFilter object) {
      }

      @Override
      public AcceptAllKeyValueFilter readObject(ObjectInput input) {
         return AcceptAllKeyValueFilter.getInstance();
      }

      @Override
      public Integer getId() {
         return Ids.ACCEPT_ALL_KEY_VALUE_FILTER;
      }
   }
}
