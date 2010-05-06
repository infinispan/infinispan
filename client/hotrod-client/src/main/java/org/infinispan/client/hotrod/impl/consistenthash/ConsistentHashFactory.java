package org.infinispan.client.hotrod.impl.consistenthash;

import org.infinispan.client.hotrod.impl.transport.VHelper;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * // TODO: Document this
 *
 * @author Mircea.Markus@jboss.com
 * @since 4.1
 */
public class ConsistentHashFactory {
   
   private static Log log = LogFactory.getLog(ConsistentHashFactory.class);

   public final Map<Integer, String> version2ConsistentHash = new HashMap<Integer, String>();

   public void init(Properties props) {
      for (String propName : props.stringPropertyNames()) {
         if (propName.indexOf("consistent-hash") >=0) {
            if (log.isTraceEnabled()) log.trace("Processing consistent hash: " + propName);
            String versionString = propName.substring("consistent-hash.".length());
            int version = Integer.parseInt(versionString);
            String hashFunction = props.getProperty(versionString);
            version2ConsistentHash.put(version, hashFunction);
            if (log.isTraceEnabled()) {
               log.trace("Added consistent hash version " + version + ": " + hashFunction);
            }
         }
      }
   }

   public ConsistentHash newConsistentHash(int version) {
      String hashFunctionClass = version2ConsistentHash.get(version);
      if (hashFunctionClass == null) {
         log.trace("No hash function configured for version " + version);
         hashFunctionClass = ConsistentHashFactory.class.getPackage().getName() + ".ConsitentHashV" + version;
         if (log.isTraceEnabled()) log.trace("Trying to use default value: " + hashFunctionClass);
      }
      ConsistentHash consistentHash = null;
      try {
         consistentHash = (ConsistentHash) VHelper.newInstance(hashFunctionClass);
      } catch (RuntimeException re) {
         log.warn("Could not instantiate consistent hash for version " + version + ": " + hashFunctionClass, re);
      }
      return consistentHash;
   }
}
