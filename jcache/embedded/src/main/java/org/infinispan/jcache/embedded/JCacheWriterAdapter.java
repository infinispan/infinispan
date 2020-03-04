package org.infinispan.jcache.embedded;

import javax.cache.integration.CacheWriter;

import org.infinispan.encoding.DataConversion;
import org.infinispan.jcache.Exceptions;
import org.infinispan.jcache.JCacheEntry;
import org.infinispan.persistence.spi.InitializationContext;
import org.infinispan.persistence.spi.MarshallableEntry;

public class JCacheWriterAdapter<K, V> implements org.infinispan.persistence.spi.CacheWriter {

   private CacheWriter<? super K, ? super V> delegate;
   private DataConversion keyDataConversion;
   private DataConversion valueDataConversion;

   public JCacheWriterAdapter() {
      // Empty constructor required so that it can be instantiated with
      // reflection. This is a limitation of the way the current cache
      // loader configuration works.
      this.keyDataConversion = DataConversion.IDENTITY_KEY;
      this.valueDataConversion = DataConversion.IDENTITY_KEY;
   }

   public void setCacheWriter(CacheWriter<? super K, ? super V> delegate) {
      this.delegate = delegate;
   }

   public void setDataConversion(DataConversion keyDataConversion, DataConversion valueDataConversion) {
      this.keyDataConversion = keyDataConversion;
      this.valueDataConversion = valueDataConversion;
   }

   @Override
   public void init(InitializationContext ctx) {
   }

   @Override
   public void write(MarshallableEntry entry) {
      try {
         delegate.write(new JCacheEntry(keyDataConversion.fromStorage(entry.getKey()), valueDataConversion.fromStorage(entry.getValue())));
      } catch (Exception e) {
         throw Exceptions.launderCacheWriterException(e);
      }
   }

   @Override
   public boolean delete(Object key) {
      try {
         delegate.delete(keyDataConversion.fromStorage(key));
      } catch (Exception e) {
         throw Exceptions.launderCacheWriterException(e);
      }
      return false;
   }

   @Override
   public void start() {
   }

   @Override
   public void stop() {
   }

}
