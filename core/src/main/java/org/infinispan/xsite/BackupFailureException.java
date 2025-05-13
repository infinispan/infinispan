package org.infinispan.xsite;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.infinispan.remoting.RpcException;
import org.infinispan.util.logging.Log;

/**
 * Exception to be used to signal failures to backup to remote sites.
 *
 * @author Mircea Markus
 * @since 5.2
 */
public class BackupFailureException extends RpcException {
   /**
    * Empty constructor, used for generic backup exceptions
    */
   public BackupFailureException() {
   }

   /**
    * Constructor used to indicate a cache failure. Use {@link #addSuppressed(Throwable)} to add site-specific failures
    *
    * @param cacheName the name of the cache
    */
   public BackupFailureException(String cacheName) {
      super(cacheName); // Store the cache name as the message as we have a custom getMessage method
   }

   /**
    * Constructor used to indicate a cache failure related to a site. Should be added to an instance of BackupFailureException
    * created with the {@link BackupFailureException(String)} constructor.
    *
    * @param siteName  the name of the site which caused the failure
    * @param throwable the cause of the failure
    */
   public BackupFailureException(String siteName, Throwable throwable) {
      super(siteName, throwable);
   }

   @Override
   public String getMessage() {
      if (getSuppressed().length == 0) {
         // Generic backup failure exception
         return super.getMessage();
      } else {
         // A cache-specific failure with itemized site exceptions
         return Log.XSITE.failedToBackupData(super.getMessage(), Arrays.stream(getSuppressed()).map(s -> s.getCause().getMessage()).collect(Collectors.joining("\n")));
      }
   }
}
