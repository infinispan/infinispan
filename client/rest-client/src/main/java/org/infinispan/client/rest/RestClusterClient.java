package org.infinispan.client.rest;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletionStage;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 10.0
 **/
public interface RestClusterClient {
   /**
    * Shuts down the cluster
    */
   CompletionStage<RestResponse> stop();

   /**
    * Shuts down the specified servers
    */
   CompletionStage<RestResponse> stop(List<String> server);

   /**
    * Creates a backup file containing the content of all containers in the cluster.
    *
    * @param name the name of the backup.
    */
   CompletionStage<RestResponse> createBackup(String name);

   /**
    * Retrieves a backup file with the given name from the server.
    *
    * @param name     the name of the backup.
    * @param skipBody if true, then a HEAD request is issued to the server and only the HTTP headers are returned.
    */
   CompletionStage<RestResponse> getBackup(String name, boolean skipBody);

   /**
    * @return the names of all backups.
    */
   CompletionStage<RestResponse> getBackupNames();

   /**
    * Deletes a backup file from the server.
    *
    * @param name the name of the backup.
    */
   CompletionStage<RestResponse> deleteBackup(String name);

   /**
    * Restores all content from a backup file, by uploading the file to the server endpoint for processing, returning
    * once the restoration has completed.
    *
    * @param name a unique name to identify the restore request.
    * @param backup the backup {@link File} containing the data to be restored.
    */
   CompletionStage<RestResponse> restore(String name, File backup);

   /**
    * Restores all content from a backup file available to the server instance.
    *
    * @param name a unique name to identify the restore request.
    * @param backupLocation the path of the backup file already located on the server.
    */
   CompletionStage<RestResponse> restore(String name, String backupLocation);

   /**
    * Polls a restore request progress with the given name. 201 indicates that the request has completed, 202 that it's
    * in progress and 404 that it can't be found.
    *
    * @param name the name of the restore.
    */
   CompletionStage<RestResponse> getRestore(String name);

   /**
    * @return the names of all restores.
    */
   CompletionStage<RestResponse> getRestoreNames();

   /**
    * Deletes a restore request from the server. Container content is not affected.
    *
    * @param name the name of the restore.
    */
   CompletionStage<RestResponse> deleteRestore(String name);

   /**
    * @return The cluster distribution information.
    */
   CompletionStage<RestResponse> distribution();
}
