package org.infinispan.persistence.rocksdb.logging;

import java.lang.invoke.MethodHandles;

import org.infinispan.commons.CacheConfigurationException;
import org.jboss.logging.BasicLogger;
import org.jboss.logging.Logger;
import org.jboss.logging.annotations.Message;
import org.jboss.logging.annotations.MessageLogger;
import org.jboss.logging.annotations.ValidIdRange;

/**
 * Log abstraction for the RocksDB cache store.
 */
@MessageLogger(projectCode = "ISPN")
@ValidIdRange(min = 23001, max = 24000)
public interface Log extends BasicLogger {
   static Log getLog(Class<?> clazz) {
      return Logger.getMessageLogger(MethodHandles.lookup(), Log.class, clazz.getName());
   }

//   @LogMessage(level = ERROR)
//   @Message(value = "Error executing parallel store task", id = 252)
//   void errorExecutingParallelStoreTask(@Cause Throwable cause);

//   @LogMessage(level = INFO)
//   @Message(value = "Ignoring XML attribute %s, please remove from configuration file", id = 293)
//   void ignoreXmlAttribute(Object attribute);

   @Message(value = "RocksDB properties %s, contains an unknown property", id = 23001)
   CacheConfigurationException rocksDBUnknownPropertiesSupplied(String properties);
}
