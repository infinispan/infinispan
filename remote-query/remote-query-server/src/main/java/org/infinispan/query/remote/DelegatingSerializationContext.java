package org.infinispan.query.remote;

import org.infinispan.protostream.BaseMarshaller;
import org.infinispan.protostream.DescriptorParserException;
import org.infinispan.protostream.FileDescriptorSource;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.config.Configuration;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.protostream.descriptors.EnumDescriptor;
import org.infinispan.protostream.descriptors.FileDescriptor;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * A wrapper around a real {@link SerializationContext} that intercepts calls to {@link
 * SerializationContext#registerProtoFiles} and {@link SerializationContext#unregisterProtoFile} and instead of
 * delegating the call it just updates the Protobuf metadata cache. The rest of the {@link SerializationContext} are
 * delegated directly.
 *
 * @author anistor@redhat.com
 * @since 7.0
 */
final class DelegatingSerializationContext implements SerializationContext {

   private final ProtobufMetadataManager protobufMetadataManager;

   /**
    * The wrapped serialization context.
    */
   private final SerializationContext delegate;

   public DelegatingSerializationContext(ProtobufMetadataManager protobufMetadataManager) {
      this.protobufMetadataManager = protobufMetadataManager;
      this.delegate = protobufMetadataManager.getSerializationContext();
   }

   /**
    * Return the real SerializationContext.
    */
   public SerializationContext getDelegate() {
      return delegate;
   }

   @Override
   public Configuration getConfiguration() {
      return delegate.getConfiguration();
   }

   @Override
   public void registerProtoFiles(FileDescriptorSource source) throws IOException, DescriptorParserException {
      Map<String, char[]> fileDescriptors = source.getFileDescriptors();
      Map<String, String> files = new HashMap<String, String>(fileDescriptors.size());
      for (String key : fileDescriptors.keySet()) {
         files.put(key, new String(fileDescriptors.get(key)));
      }
      protobufMetadataManager.getCache().putAll(files);
   }

   @Override
   public void unregisterProtoFile(String name) {
      protobufMetadataManager.getCache().remove(name);
   }

   @Override
   public Map<String, FileDescriptor> getFileDescriptors() {
      return delegate.getFileDescriptors();
   }

   @Override
   public <T> void registerMarshaller(BaseMarshaller<T> marshaller) {
      delegate.registerMarshaller(marshaller);
   }

   @Override
   public Descriptor getMessageDescriptor(String fullName) {
      return delegate.getMessageDescriptor(fullName);
   }

   @Override
   public EnumDescriptor getEnumDescriptor(String fullName) {
      return delegate.getEnumDescriptor(fullName);
   }

   @Override
   public boolean canMarshall(Class clazz) {
      return delegate.canMarshall(clazz);
   }

   @Override
   public boolean canMarshall(String descriptorFullName) {
      return delegate.canMarshall(descriptorFullName);
   }

   @Override
   public <T> BaseMarshaller<T> getMarshaller(String descriptorFullName) {
      return delegate.getMarshaller(descriptorFullName);
   }

   @Override
   public <T> BaseMarshaller<T> getMarshaller(Class<T> clazz) {
      return delegate.getMarshaller(clazz);
   }
}
