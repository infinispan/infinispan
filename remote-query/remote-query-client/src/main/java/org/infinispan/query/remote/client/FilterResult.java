package org.infinispan.query.remote.client;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.WrappedMessage;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * When using Ickle based filters with client event listeners you will get the event data (see
 * org.infinispan.client.hotrod.event.ClientCacheEntryCustomEvent.getEventData) wrapped by this FilterResult.
 *
 * @author anistor@redhat.com
 * @since 7.2
 */
@ProtoTypeId(ProtoStreamTypeIds.REMOTE_QUERY_ICKLE_FILTER_RESULT)
public final class FilterResult {

   private final Object instance;

   private final Object[] projection;

   private final Comparable[] sortProjection;

   public FilterResult(Object instance, Object[] projection, Comparable[] sortProjection) {
      if (instance == null && projection == null) {
         throw new IllegalArgumentException("instance and projection cannot be both null");
      }
      this.instance = instance;
      this.projection = projection;
      this.sortProjection = sortProjection;
   }

   @ProtoFactory
   FilterResult(WrappedMessage wrappedInstance, List<WrappedMessage> wrappedProjection, List<WrappedMessage> wrappedSortProjection) {
      this.instance = wrappedInstance == null ? null : wrappedInstance.getValue();
      this.projection = wrappedProjection == null ? null : wrappedProjection.stream().map(WrappedMessage::getValue).toArray();
      this.sortProjection = wrappedSortProjection == null ? null :
            wrappedSortProjection.stream()
                  .map(WrappedMessage::getValue)
                  .map(c -> (Comparable<?>) c)
                  .toArray(Comparable[]::new);
   }

   @ProtoField(value = 1, name = "instance")
   WrappedMessage getWrappedInstance() {
      return getProjection() == null ? new WrappedMessage(instance) : null;
   }

   @ProtoField(value = 2, name = "projection", collectionImplementation = ArrayList.class)
   List<WrappedMessage> getWrappedProjection() {
      return projection == null ? null :
            Arrays.stream(projection)
                  .map(WrappedMessage::new)
                  .collect(Collectors.toList());
   }

   @ProtoField(value = 3, name = "sortProjection", collectionImplementation = ArrayList.class)
   List<WrappedMessage> getWrappedSortProjection() {
      return sortProjection == null ? null :
            Arrays.stream(sortProjection)
                  .map(WrappedMessage::new)
                  .collect(Collectors.toList());
   }

   /**
    * Returns the matched object. This is non-null unless projections are present.
    */
   public Object getInstance() {
      return instance;
   }

   /**
    * Returns the projection, if a projection was requested or {@code null} otherwise.
    */
   public Object[] getProjection() {
      return projection;
   }

   /**
    * Returns the projection of fields that appear in the 'order by' clause, if any, or {@code null} otherwise.
    * <p>
    * Please note that no actual sorting is performed! The 'order by' clause is ignored but the fields listed there are
    * still projected and returned so the caller can easily sort the results if needed. Do not use 'order by' with
    * filters if this behaviour does not suit you.
    */
   public Comparable[] getSortProjection() {
      return sortProjection;
   }

   @Override
   public String toString() {
      return "FilterResult{" +
            "instance=" + instance +
            ", projection=" + Arrays.toString(projection) +
            ", sortProjection=" + Arrays.toString(sortProjection) +
            '}';
   }
}
