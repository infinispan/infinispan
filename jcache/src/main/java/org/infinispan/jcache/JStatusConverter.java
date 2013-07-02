package org.infinispan.jcache;

import javax.cache.Status;

import org.infinispan.lifecycle.ComponentStatus;

/**
 * InfinispanStatusConverter converts Infinispan cache status to {@link javax.cache.Status}.
 * 
 * @author Vladimir Blagojevic
 * @since 5.3
 */
public class JStatusConverter {

   public static Status convert(ComponentStatus status) {
      Status convertedStatus;
      switch (status) {
         case FAILED:
         case TERMINATED:
         case STOPPING:
            convertedStatus = Status.STOPPED;
            break;
         case INITIALIZING:
         case INSTANTIATED:
            convertedStatus = Status.UNINITIALISED;
            break;
         case RUNNING:
            convertedStatus = Status.STARTED;
            break;
         default:
            convertedStatus = Status.UNINITIALISED;
            break;
      }
      return convertedStatus;
   }
}
