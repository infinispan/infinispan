package org.infinispan.query.remote.impl.filter;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.query.remote.client.FilterResult;
import org.infinispan.query.remote.impl.ExternalizerIds;

/**
 * A 'remote' FilterResult needs jboss-marshalling serializability between nodes when running with object storage.
 * It will only be marshalled using protobuf before passing it to the remote client.
 *
 * @author anistor@redhat.com
 * @since 9.0
 */
public final class FilterResultExternalizer extends AbstractExternalizer<FilterResult> {

   @Override
   public void writeObject(ObjectOutput output, FilterResult filterResult) throws IOException {
      Object[] projection = filterResult.getProjection();
      if (projection == null) {
         // skip marshalling the instance if there is a projection
         output.writeInt(-1);
         output.writeObject(filterResult.getInstance());
      } else {
         output.writeInt(projection.length);
         for (Object prj : projection) {
            output.writeObject(prj);
         }
      }

      Comparable[] sortProjection = filterResult.getSortProjection();
      if (sortProjection == null) {
         output.writeInt(-1);
      } else {
         output.writeInt(sortProjection.length);
         for (Object prj : sortProjection) {
            output.writeObject(prj);
         }
      }
   }

   @Override
   public FilterResult readObject(ObjectInput input) throws IOException, ClassNotFoundException {
      Object instance;
      Object[] projection;
      Comparable[] sortProjection;

      int projLen = input.readInt();
      if (projLen == -1) {
         instance = input.readObject();
         projection = null;
      } else {
         instance = null;
         projection = new Object[projLen];
         for (int i = 0; i < projLen; i++) {
            projection[i] = input.readObject();
         }
      }

      int sortProjLen = input.readInt();
      if (sortProjLen == -1) {
         sortProjection = null;
      } else {
         sortProjection = new Comparable[sortProjLen];
         for (int i = 0; i < sortProjLen; i++) {
            sortProjection[i] = (Comparable) input.readObject();
         }
      }

      return new FilterResult(instance, projection, sortProjection);
   }

   @Override
   public Integer getId() {
      return ExternalizerIds.ICKLE_FILTER_RESULT;
   }

   @Override
   public Set<Class<? extends FilterResult>> getTypeClasses() {
      return Collections.singleton(FilterResult.class);
   }
}
