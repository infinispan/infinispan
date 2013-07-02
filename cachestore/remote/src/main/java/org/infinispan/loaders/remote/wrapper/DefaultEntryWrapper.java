package org.infinispan.loaders.remote.wrapper;

import org.infinispan.client.hotrod.MetadataValue;

/**
 * DefaultEntryWrapper.
 *
 * @author Tristan Tarrant
 * @since 5.2
 */
public class DefaultEntryWrapper implements EntryWrapper<Object, Object> {

   @Override
   public Object wrapKey(Object key) {
      return key;
   }

   @Override
   public Object wrapValue(MetadataValue<?> value) {
      return value.getValue();
   }

}
