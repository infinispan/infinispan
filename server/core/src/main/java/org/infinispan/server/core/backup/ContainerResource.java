package org.infinispan.server.core.backup;

import java.util.Properties;
import java.util.concurrent.CompletionStage;
import java.util.zip.ZipFile;

import org.infinispan.commons.CacheException;
import org.infinispan.server.core.BackupManager;

/**
 * An interface that defines how a container resource is backed up and
 * restored by the {@link org.infinispan.server.core.BackupManager}.
 *
 * @author Ryan Emerson
 * @since 12.0
 */
public interface ContainerResource {

   /**
    * A method to ensure that the resources requested in the {@link BackupManager.Resources}
    * are valid and can be included in a backup. This method is called for all {@link ContainerResource} implementations
    * before the backup process begins in order to allow a backup to fail-fast before any data is processed.
    *
    * @throws CacheException if an invalid parameter is specified, e.g. a unknown resource name.
    */
   void prepareAndValidateBackup() throws CacheException;

   /**
    * Writes the backup files for the {@link BackupManager.Resources.Type} to the local
    * filesystem, where it can then be packaged for distribution.
    * <p>
    * Implementations of this method depend on content created by {@link #prepareAndValidateBackup()}.
    *
    * @return a {@link CompletionStage} that completes once the backup of this {@link
    * BackupManager.Resources.Type} has finished.
    */
   CompletionStage<Void> backup();

   /**
    * Writes the name of the individual resources that have been included in this backup. The {@link
    * BackupManager.Resources.Type} associated with an implementation is the key, whilst the
    * value is a csv of resource names.
    * <p>
    * Implementations of this method depend on state created by {@link #backup()}.
    *
    * @param properties the {@link Properties} instance to add the key/value property.
    */
   void writeToManifest(Properties properties);

   /**
    * A method to ensure that the resources requested in the {@link BackupManager.Resources}
    * are contained in the backup to be restored. This method is called for all {@link ContainerResource}
    * implementations before the restore process begins in order to allow a restore to fail-fast before any state is
    * restored to a container.
    */
   void prepareAndValidateRestore(Properties properties);

   /**
    * Restores the {@link BackupManager.Resources.Type} content from the provided {@link
    * ZipFile} to the target container.
    *
    * @param zip the {@link ZipFile} to restore content from.
    * @return a {@link CompletionStage} that completes once the restoration of this {@link
    * BackupManager.Resources.Type} has finished.
    */
   CompletionStage<Void> restore(ZipFile zip);
}
