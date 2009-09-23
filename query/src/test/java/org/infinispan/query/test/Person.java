package org.infinispan.query.test;


import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.FieldBridge;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.ProvidedId;
import org.hibernate.search.annotations.Store;
import org.hibernate.search.bridge.builtin.StringBridge;
import org.infinispan.util.logging.Log;
import org.infinispan.util.logging.LogFactory;

import java.io.Serializable;

/**
 * @author Navin Surtani
 */
@ProvidedId (bridge = @FieldBridge(impl = StringBridge.class))
@Indexed
public class Person implements Serializable
{
   private static final Log log = LogFactory.getLog(Person.class);


   @Field (store = Store.YES)
   private String name;
   @Field (store = Store.YES)
   private String blurb;
   @Field (store = Store.YES, index = Index.UN_TOKENIZED )
   private int age;

   public String getName()
   {
      return name;
   }

   public void setName(String name)
   {
      this.name = name;
   }

   public String getBlurb()
   {
      return blurb;
   }

   public void setBlurb(String blurb)
   {
      this.blurb = blurb;
   }

   public int getAge()
   {
      return age;
   }

   public void setAge(int age)
   {
      this.age = age;
   }

   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      Person person = (Person) o;

      if (blurb != null ? !blurb.equals(person.blurb) : person.blurb != null) return false;
      if (name != null ? !name.equals(person.name) : person.name != null) return false;

      return true;
   }

   public int hashCode()
   {
      int result;
      result = (name != null ? name.hashCode() : 0);
      result = 31 * result + (blurb != null ? blurb.hashCode() : 0);
      return result;
   }


   public String toString()
   {
      return "Person{" +
              "name='" + name + '\'' +
              ", blurb='" + blurb + '\'' +
              '}';
   }
}
