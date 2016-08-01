package org.infinispan.xsite;

import org.infinispan.remoting.RpcException;

import java.util.HashMap;
import java.util.Map;

/**
 * Exception to be used to signal failures to backup to remote sites.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupFailureException extends RpcException {

   private Map<String,Throwable> failures;
   private String                localCacheName;


   public BackupFailureException(String localCacheName) {
      this.localCacheName  = localCacheName;
   }

   public BackupFailureException() {
   }

   public void addFailure(String site, Throwable t) {
      if(site != null && t != null) {
         if(failures == null)
            failures = new HashMap<>(3);
         failures.put(site, t);
      }
   }

   public String getRemoteSiteNames() {
      return failures != null? failures.keySet().toString() : null;
   }

   public String getLocalCacheName() {
      return localCacheName;
   }

   @Override
   public String toString() {
      if(failures == null || failures.isEmpty())
         return super.toString();
      StringBuilder sb=new StringBuilder("The local cache " + localCacheName + " failed to backup data to the remote sites:\n");
      for(Map.Entry<String,Throwable> entry: failures.entrySet())
         sb.append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
      return sb.toString();
   }
}
