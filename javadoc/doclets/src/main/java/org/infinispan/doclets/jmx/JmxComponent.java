package org.infinispan.doclets.jmx;

/**
 * A JMX Component
 *
 * @author Manik Surtani
 * @since 4.0
 */
public abstract class JmxComponent implements Comparable<JmxComponent> {
   public String name;
   public String desc = "";

   @Override
   public int compareTo(JmxComponent other) {
      return name.compareTo(other.name);
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      JmxComponent that = (JmxComponent) o;

      if (name != null ? !name.equals(that.name) : that.name != null) return false;

      return true;
   }

   @Override
   public int hashCode() {
      return name != null ? name.hashCode() : 0;
   }
}
