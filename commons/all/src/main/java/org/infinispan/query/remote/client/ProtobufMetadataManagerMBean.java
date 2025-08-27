package org.infinispan.query.remote.client;

/**
 * MBean interface for ProtobufMetadataManager, suitable for building invocation proxies with one of the {@link
 * javax.management.JMX#newMBeanProxy} methods.
 *
 * @author anistor@redhat.com
 * @author gustavonalle
 * @since 7.1
 */
public interface ProtobufMetadataManagerMBean extends ProtobufMetadataManagerConstants {

   /**
    * Register a *.proto schema file. If there are any syntax or semantic errors a *.proto.errors key will be created in
    * the underlying cache and its value will be the actual error message. The error message, if any, can be retrieved
    * using {@link #getFileErrors(String fileName)} method. The list of offending files can be retrieved using {@link
    * #getFilesWithErrors()} method.
    *
    * @param fileName the full name of the file (name can contain '/'); must end with ".proto" suffix
    * @param contents the file contents
    * @throws Exception in case of failure
    */
   void registerProtofile(String fileName, String contents) throws Exception;

   /**
    * Registers multiple *.proto schema files. If there are any syntax or semantic errors a *.proto.errors key will be
    * created in the underlying cache for each offending file and its value will be the actual error message. The error
    * message, if any, can be retrieved using {@link #getFileErrors(String fileName)} method. The list of offending
    * files can be retrieved using {@link #getFilesWithErrors()} method.
    *
    * @param fileNames the full names of the files (name can contain '/'); names must end with ".proto" suffix
    * @param contents  the contents of each file; this array must have the same length as {@code fileNames}
    * @throws Exception in case of failure
    */
   void registerProtofiles(String[] fileNames, String[] contents) throws Exception;

   /**
    * Unregister a *.proto schema file.
    *
    * @param fileName the full name of the file (name can contain '/'); must end with ".proto" suffix
    * @throws Exception in case of failure
    */
   void unregisterProtofile(String fileName) throws Exception;

   /**
    * Unregisters multiple *.proto schema files.
    *
    * @param fileNames the full names of the files (name can contain '/'); names must end with ".proto" suffix
    * @throws Exception in case of failure
    */
   void unregisterProtofiles(String[] fileNames) throws Exception;

   /**
    * Get the full names of all registered schema files.
    *
    * @return the array of all registered schema file names or an empty array if there are no files (never null)
    */
   String[] getProtofileNames();

   /**
    * Gets the contents of a registered *.proto schema file.
    *
    * @param fileName the name of the file; must end with ".proto" suffix
    * @return the file contents or {@code null} if the file does not exist
    */
   String getProtofile(String fileName);

   /**
    * Get the full names of all files with errors.
    *
    * @return the array of all file names with errors or an empty array if there are no files with errors (never null)
    */
   String[] getFilesWithErrors();

   /**
    * Gets the error messages (caused by parsing, linking, etc) associated to a *.proto schema file.
    *
    * @param fileName the name of the file; must end with ".proto" suffix
    * @return the error text or {@code null} if there are no errors
    */
   String getFileErrors(String fileName);
}
