package org.infinispan.interceptors2;

import org.infinispan.metadata.Metadata;

/**
 * This interface is not generic because we sometimes need to store MarshalledValues in the context.
 * We could instead extend this interface to perform the role of MarshalledValue, and add methods
 * like getKeyBytes(), getOldValueBytes() etc.
 *
 * @author Dan Berindei
 * @since 8.0
 */
public interface ContextEntry {
   enum Locality { PRIMARY, BACKUP, WRITE_ONLY_BACKUP, NONE}

   Object getKey();
   Object getOldValue();
   void setOldValue(Object oldValue);
   Object getNewValue();
   void setNewValue();
   Metadata getOldMetadata();
   void setOldMetadata();
   Metadata getNewMetadata();
   void setNewMetadata();

   // isModified(), isRemoved(), isCreated() only make sense when there we know the old value
   boolean hasOldValue();
   boolean isModified();
   boolean isRemoved();
   boolean isCreated();

   Locality getLocality();
}
