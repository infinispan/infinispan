package org.infinispan.marshall;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Objects;

import org.infinispan.marshall.core.ExternalPojo;

public class CustomClasses {
   public static class CustomClass implements Serializable, ExternalPojo {
      final String val;

      public CustomClass(String val) {
         this.val = val;
      }

      public String getVal() {
         return val;
      }

      @Override
      public String toString() {
         return "CustomClass{" +
               "val='" + val + '\'' +
               '}';
      }

      @Override
      public boolean equals(Object o) {
         if (this == o) return true;
         if (o == null || getClass() != o.getClass()) return false;

         CustomClass that = (CustomClass) o;

         if (val != null ? !val.equals(that.val) : that.val != null) return false;

         return true;
      }

      @Override
      public int hashCode() {
         return val != null ? val.hashCode() : 0;
      }
   }

   public static class CustomReadObjectMethod implements Serializable, ExternalPojo {
      private static final long serialVersionUID = 1L;
      String lastName;
      String ssn;
      transient boolean deserialized;

      public CustomReadObjectMethod() {
         this("Zamarreno", "234-567-8901");
      }

      private CustomReadObjectMethod(String lastName, String ssn) {
         this.lastName = lastName;
         this.ssn = ssn;
      }

      @Override
      public boolean equals(Object obj) {
         if (obj == this) return true;
         if (!(obj instanceof CustomReadObjectMethod)) return false;
         CustomReadObjectMethod pk = (CustomReadObjectMethod) obj;
         return lastName.equals(pk.lastName) && ssn.equals(pk.ssn);
      }

      @Override
      public int hashCode() {
         int result = 17;
         result = result * 31 + lastName.hashCode();
         result = result * 31 + ssn.hashCode();
         return result;
      }

      private void readObject(ObjectInputStream ois) throws IOException, ClassNotFoundException {
         ois.defaultReadObject();
         deserialized = true;
      }
   }

   public static class ObjectThatContainsACustomReadObjectMethod implements Serializable, ExternalPojo {
      private static final long serialVersionUID = 1L;
      public CustomReadObjectMethod anObjectWithCustomReadObjectMethod;
      Integer balance;

      public boolean equals(Object obj) {
         if (obj == this)
            return true;
         if (!(obj instanceof ObjectThatContainsACustomReadObjectMethod))
            return false;
         ObjectThatContainsACustomReadObjectMethod acct = (ObjectThatContainsACustomReadObjectMethod) obj;
         if (!safeEquals(balance, acct.balance))
            return false;
         if (!safeEquals(anObjectWithCustomReadObjectMethod, acct.anObjectWithCustomReadObjectMethod))
            return false;
         return true;
      }

      public int hashCode() {
         int result = 17;
         result = result * 31 + safeHashCode(balance);
         result = result * 31 + safeHashCode(anObjectWithCustomReadObjectMethod);
         return result;
      }

      private static int safeHashCode(Object obj) {
         return obj == null ? 0 : obj.hashCode();
      }

      private static boolean safeEquals(Object a, Object b) {
         return Objects.equals(a, b);
      }
   }
}
