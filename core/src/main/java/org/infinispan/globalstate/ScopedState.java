package org.infinispan.globalstate;

import java.util.Objects;

import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Key for scoped entries in the ClusterConfigurationManager state cache
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.SCOPED_STATE)
public class ScopedState {
   private final String scope;
   private final String name;

   @ProtoFactory
   public ScopedState(String scope, String name) {
      this.scope = scope;
      this.name = name;
   }

   @ProtoField(number = 1)
   public String getScope() {
      return scope;
   }

   @ProtoField(number = 2)
   public String getName() {
      return name;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      ScopedState that = (ScopedState) o;
      if (!Objects.equals(scope, that.scope))
         return false;
      return Objects.equals(name, that.name);
   }

   @Override
   public int hashCode() {
      int result = scope != null ? scope.hashCode() : 0;
      result = 31 * result + (name != null ? name.hashCode() : 0);
      return result;
   }

   @Override
   public String toString() {
      return "ScopedState{" +
            "scope='" + scope + '\'' +
            ", name='" + name + '\'' +
            '}';
   }
}
