package org.infinispan.hibernate.search;

import org.hibernate.search.annotations.Analyze;
import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Store;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;

/**
 * @author Sanne Grinovero
 */
@Entity
@Indexed(index = "emails")
public class SimpleEmail {

   @Id
   @GeneratedValue
   public Long id;

   @Field(analyze = Analyze.NO)
   @Column(name = "recipient")
   public String to = "";

   @Field(store = Store.COMPRESS)
   public String message = "";

   @Field(analyze = Analyze.NO)
   public Integer sequential;

}
