package org.infinispan.xsite;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import org.infinispan.remoting.RpcException;

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
   public String getMessage() {
      if(failures == null || failures.isEmpty())
         return super.getMessage();
      return "The local cache " + localCacheName + " failed to backup data to the remote sites:\n" +
            failures.entrySet().stream().map(e -> e.getKey() + ": " + e.getValue())
                    .collect(Collectors.joining("\n"));
   }
}
