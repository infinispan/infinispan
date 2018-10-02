package org.infinispan.query.remote.impl.indexing;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;

import org.hibernate.search.backend.spi.Work;
import org.hibernate.search.backend.spi.WorkType;
import org.hibernate.search.spi.IndexedTypeIdentifier;
import org.infinispan.commons.CacheException;
import org.infinispan.protostream.ProtobufParser;
import org.infinispan.protostream.SerializationContext;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.descriptors.Descriptor;
import org.infinispan.query.backend.ExtendedSearchWorkCreator;
import org.infinispan.query.backend.SearchWorkCreator;
import org.infinispan.query.backend.SearchWorkCreatorContext;

/**
 * Wraps a {@link SearchWorkCreator} in order to intercept values of type {@link ProtobufValueWrapper} and parse the
 * underlying WrappedMessage to discover the actual user message type. Knowledge of the message type is needed in order
 * to be able to tell later if this values gets to be indexed or not.
 *
 * @author anistor@redhat.com
 * @since 9.4.1
 */
public final class ProtobufValueWrapperSearchWorkCreator {

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

   public SearchWorkCreator get() {
      return delegate instanceof ExtendedSearchWorkCreator ? new ExtendedDelegate() : new Delegate();
   }

   private class Delegate implements SearchWorkCreator {

      @Override
      public Collection<Work> createPerEntityTypeWorks(IndexedTypeIdentifier entityType, WorkType workType) {
         return delegate.createPerEntityTypeWorks(entityType, workType);
      }

      @Override
      public Collection<Work> createPerEntityWorks(Object entity, Serializable id, WorkType workType) {
         return delegate.createPerEntityWorks(interceptValue(entity), id, workType);
      }

      @Override
      public Work createEntityWork(Serializable id, IndexedTypeIdentifier entityType, WorkType workType) {
         return delegate.createEntityWork(id, entityType, workType);
      }
   }

   private final class ExtendedDelegate extends Delegate implements ExtendedSearchWorkCreator {

      @Override
      public boolean shouldRemove(SearchWorkCreatorContext context) {
         return ((ExtendedSearchWorkCreator) delegate).shouldRemove(
               new SearchWorkCreatorContext(interceptValue(context.getPreviousValue()), interceptValue(context.getCurrentValue())));
      }
   }

   private Object interceptValue(Object value) {
      if (value instanceof ProtobufValueWrapper) {
         discoverMessageType((ProtobufValueWrapper) value);
      }
      return value;
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
