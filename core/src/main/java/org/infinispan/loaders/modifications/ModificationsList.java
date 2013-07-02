package org.infinispan.loaders.modifications;

import java.util.List;

/**
 * ModificationsList contains a List<Modification>
 * 
 * @author Sanne Grinovero
 * @since 4.1
 */
public class ModificationsList implements Modification {
   
   private final List<? extends Modification> list;

   public ModificationsList(List<? extends Modification> list) {
      this.list = list;
   }

   @Override
   public Type getType() {
      return Modification.Type.LIST;
   }

   public List<? extends Modification> getList() {
      return list;
   }

   @Override
   public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((list == null) ? 0 : list.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj) {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      ModificationsList other = (ModificationsList) obj;
      if (list == null) {
         if (other.list != null)
            return false;
      } else if (!list.equals(other.list))
         return false;
      return true;
   }
   
   @Override
   public String toString() {
      return "ModificationsList: [" + list + "]";
   }

}
