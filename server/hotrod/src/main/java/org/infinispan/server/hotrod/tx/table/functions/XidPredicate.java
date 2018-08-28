package org.infinispan.server.hotrod.tx.table.functions;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.tx.XidImpl;
import org.infinispan.server.core.ExternalizerIds;
import org.infinispan.server.hotrod.tx.table.CacheXid;

/**
 * A {@link Predicate} to filter the {@link CacheXid} by its {@link XidImpl}.
 *
 * @author Pedro Ruivo
 * @since 9.4
 */
public class XidPredicate implements Predicate<CacheXid> {

   public static final AdvancedExternalizer<XidPredicate> EXTERNALIZER = new Externalizer();

   private final XidImpl xid;

   public XidPredicate(XidImpl xid) {
      this.xid = xid;
   }

   @Override
   public boolean test(CacheXid cacheXid) {
      return cacheXid.sameXid(xid);
   }

   private static class Externalizer implements AdvancedExternalizer<XidPredicate> {

      @Override
      public Set<Class<? extends XidPredicate>> getTypeClasses() {
         return Collections.singleton(XidPredicate.class);
      }

      @Override
      public Integer getId() {
         return ExternalizerIds.XID_PREDICATE;
      }

      @Override
      public void writeObject(ObjectOutput output, XidPredicate object) throws IOException {
         XidImpl.writeTo(output, object.xid);
      }

      @Override
      public XidPredicate readObject(ObjectInput input) throws IOException {
         return new XidPredicate(XidImpl.readFrom(input));
      }
   }

}
