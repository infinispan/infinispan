package org.infinispan.test.integration.as.jms.model;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;
import java.io.Serializable;

@Entity
@Indexed
@Table(uniqueConstraints = @UniqueConstraint(columnNames = "email"))
public class RegisteredMember implements Serializable {

   /**
    * Default value included to remove warning. Remove or modify at will. *
    */
   private static final long serialVersionUID = 1L;

   @Id
   @GeneratedValue
   private Long id;

   @Field(index = Index.YES, store = Store.NO)
   private String name;

   @Field(index = Index.YES, store = Store.NO)
   private String email;

   public Long getId() {
      return id;
   }

   public void setId(Long id) {
      this.id = id;
   }

   public String getName() {
      return name;
   }

   public void setName(String name) {
      this.name = name;
   }

   public String getEmail() {
      return email;
   }

   public void setEmail(String email) {
      this.email = email;
   }

   @Override
   public String toString() {
      return "[" + id + ", " + name + ", " + email + "]";
   }
}
