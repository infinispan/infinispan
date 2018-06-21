package org.infinispan.notifications.cachelistener;

import org.infinispan.encoding.DataConversion;

/**
 * @since 9.1
 */
public class ListenerHolder {

   private final Object listener;
   private final DataConversion keyDataConversion;
   private final DataConversion valueDataConversion;
   private final boolean filterOnStorageFormat;

   public ListenerHolder(Object listener, DataConversion keyDataConversion, DataConversion valueDataConversion, boolean filterOnStorageFormat) {
      this.listener = listener;
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
      this.filterOnStorageFormat = filterOnStorageFormat;
   }

   public boolean isFilterOnStorageFormat() {
      return filterOnStorageFormat;
   }

   public Object getListener() {
      return listener;
   }

   public DataConversion getKeyDataConversion() {
      return keyDataConversion;
   }

   public DataConversion getValueDataConversion() {
      return valueDataConversion;
   }
}
