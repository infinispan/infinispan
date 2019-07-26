package org.infinispan.jboss.marshalling.core;

import java.io.IOException;
import java.io.ObjectOutput;
import java.lang.invoke.SerializedLambda;
import java.util.Arrays;

import org.infinispan.commons.CacheException;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.marshall.core.GlobalMarshaller;
import org.infinispan.marshall.core.MarshallableFunctions;
import org.infinispan.marshall.persistence.PersistenceMarshaller;
import org.infinispan.util.function.SerializableFunction;
import org.jboss.marshalling.ObjectTable;
import org.jboss.marshalling.Unmarshaller;

/**
 * The object table used for the GlobalMarshaller. Allows internal and external externalizers to be used for marshalling
 *
 * @author Ryan Emerson
 * @since 10.0
 */
class JbossInternalObjectTable implements ObjectTable {

   private final GlobalMarshaller gm;
   private final PersistenceMarshaller pm;

   JbossInternalObjectTable(GlobalMarshaller gm) {
      this.gm = gm;
      this.pm = gm.getPersistenceMarshaller();
   }

   @Override
   public ObjectTable.Writer getObjectWriter(Object obj) {
      Class<?> clazz = obj.getClass();
      AdvancedExternalizer internalExt = GlobalMarshaller.getInteralExternalizer(gm, clazz);
      if (internalExt != null)
         return (out, object) -> GlobalMarshaller.writeInternalClean(object, internalExt, out);

      // We still try to write via any configured user externalizers, regardless of the user marshaller impl
      // because calls to writeObject may still fall through to the user marshaller impl.
      AdvancedExternalizer externalExt = GlobalMarshaller.getExternalExternalizer(gm, clazz);
      if (externalExt != null)
         return (out, object) -> GlobalMarshaller.writeExternalClean(object, externalExt, out);

      try {
         // The persistence marshaller should not handle lambdas or functions as these should never be persisted
         if (!isFunctionOrArrayOf(obj) && pm.isMarshallable(obj))
            return this::writeUnknownClean;
      } catch (Exception ignore) {
      }
      return null;
   }

   private boolean isFunctionOrArrayOf(Object o) {
      Class clazz = o.getClass();
      if (clazz.isArray())
         return Arrays.stream((Object[]) o).anyMatch(this::isFunctionOrArrayOf);

      if (clazz.isSynthetic() || o instanceof SerializedLambda || o instanceof SerializableFunction)
         return true;

      Class enclosingClass = clazz.getEnclosingClass();
      return enclosingClass != null && enclosingClass.equals(MarshallableFunctions.class);
   }

   @Override
   public Object readObject(Unmarshaller unmarshaller) throws IOException, ClassNotFoundException {
      return GlobalMarshaller.readObjectFromObjectInput(gm, unmarshaller);
   }

   private void writeUnknownClean(ObjectOutput out, Object obj) {
      try {
         GlobalMarshaller.writeUnknown(pm, obj, out);
      } catch (IOException e) {
         throw new CacheException(e);
      }
   }
}
