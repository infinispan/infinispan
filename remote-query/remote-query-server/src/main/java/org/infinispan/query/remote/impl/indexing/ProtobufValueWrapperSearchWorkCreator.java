package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;
import java.io.Serializable;

import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.backend.SearchWorkCreator;

/**
 * Wraps a {@link SearchWorkCreator} in order to intercept values of type {@link ProtobufValueWrapper} and parse the
 * underlying WrappedMessage to discover the actual user message type. Knowledge of the message type is needed in order
 * to be able to tell later if this values gets to be indexed or not.
 *
 * @author anistor@redhat.com
 * @since 9.4.1
 */
public final class ProtobufValueWrapperSearchWorkCreator implements SearchWorkCreator {

   private final SearchWorkCreator delegate;

   private final SerializationContext serializationContext;

   private final Descriptor wrapperDescriptor;

   public ProtobufValueWrapperSearchWorkCreator(SearchWorkCreator searchWorkCreator, SerializationContext serializationContext) {
      if (searchWorkCreator == null) {
         throw new IllegalArgumentException("searchWorkCreator argument cannot be null");
      }
      this.delegate = searchWorkCreator;
      this.serializationContext = serializationContext;
      this.wrapperDescriptor = serializationContext.getMessageDescriptor(WrappedMessage.PROTOBUF_TYPE_NAME);
   }

   @Override
   public Work createPerEntityWork(Object entity, Serializable id, WorkType workType) {
      if (entity instanceof ProtobufValueWrapper) {
         discoverMessageType((ProtobufValueWrapper) entity);
      }
      return delegate.createPerEntityWork(entity, id, workType);
   }

   @Override
   public Work createPerEntityTypeWork(IndexedTypeIdentifier entityType, WorkType workType) {
      return delegate.createPerEntityTypeWork(entityType, workType);
   }

   @Override
   public Work createPerEntityWork(Serializable id, IndexedTypeIdentifier entityType, WorkType workType) {
      return delegate.createPerEntityWork(id, entityType, workType);
   }

   /**
    * Discovers the type of the protobuf payload and if it is a message type it sets the descriptor using {@link
    * ProtobufValueWrapper#setMessageDescriptor}.
    *
    * @param valueWrapper the wrapper of the protobuf binary payload
    */
   private void discoverMessageType(ProtobufValueWrapper valueWrapper) {
      try {
         ProtobufParser.INSTANCE.parse(new WrappedMessageTagHandler(valueWrapper, serializationContext), wrapperDescriptor, valueWrapper.getBinary());
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
