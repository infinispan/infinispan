package org.infinispan.query.remote.indexing;

import com.google.protobuf.Descriptors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author anistor@redhat.com
 * @since 6.0
 */
class MessageContext {

   private final String fieldName;

   /**
    * The descriptor of the current message.
    */
   private final Descriptors.Descriptor messageDescriptor;

   /**
    * The map of field descriptors by name, for easier lookup (more efficient than Descriptors.Descriptor.findFieldByName()).
    */
   private final Map<String, Descriptors.FieldDescriptor> fieldDescriptors;

   private final Set<Integer> readFields;

   /**
    * The context of the outer message or null if this is a top level message.
    */
   private final MessageContext parentContext;

   MessageContext(Descriptors.Descriptor messageDescriptor) {
      this(null, null, messageDescriptor);
   }

   MessageContext(String fieldName, MessageContext parentContext, Descriptors.Descriptor messageDescriptor) {
      if (messageDescriptor == null) {
         throw new IllegalArgumentException("messageDescriptor cannot be null");
      }
      this.fieldName = fieldName;
      this.parentContext = parentContext;
      this.messageDescriptor = messageDescriptor;
      List<Descriptors.FieldDescriptor> fields = messageDescriptor.getFields();
      fieldDescriptors = new HashMap<String, Descriptors.FieldDescriptor>(fields.size());
      for (Descriptors.FieldDescriptor fd : fields) {
         fieldDescriptors.put(fd.getName(), fd);
      }
      readFields = new HashSet<Integer>(fieldDescriptors.size());
   }

   String getFieldName() {
      return fieldName;
   }

   Descriptors.Descriptor getMessageDescriptor() {
      return messageDescriptor;
   }

   MessageContext getParentContext() {
      return parentContext;
   }

   Set<Integer> getReadFields() {
      return readFields;
   }

   Descriptors.FieldDescriptor getFieldByName(String fieldName) {
      Descriptors.FieldDescriptor fd = fieldDescriptors.get(fieldName);
      if (fd == null) {
         throw new IllegalArgumentException("Unknown field : " + fieldName);   //todo [anistor] throw a better exception
      }
      return fd;
   }
}
