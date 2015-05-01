package org.infinispan.test.integration.as.wildfly.model;

import java.io.Serializable;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.UniqueConstraint;

import org.hibernate.search.annotations.Field;
import org.hibernate.search.annotations.Index;
import org.hibernate.search.annotations.Indexed;
import org.hibernate.search.annotations.Latitude;
import org.hibernate.search.annotations.Longitude;
import org.hibernate.search.annotations.Spatial;
import org.hibernate.search.annotations.Store;

@Entity
@Indexed
@Spatial
@Table(uniqueConstraints = @UniqueConstraint(columnNames = "email"))
public class Member implements Serializable {

   /**
    * Default value included to remove warning. Remove or modify at will. *
    */
   private static final long serialVersionUID = 1L;

   @Id
   @GeneratedValue
   private Long id;

   @Latitude
   private Double latitude;

   @Longitude
   private Double longitude;

   @Field(index = Index.YES, store = Store.YES)
   private String name;

   @Field(index = Index.YES, store = Store.NO)
   private String email;

   @Column(name = "phone_number")
   private String phoneNumber;

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

   public String getPhoneNumber() {
      return phoneNumber;
   }

   public void setPhoneNumber(String phoneNumber) {
      this.phoneNumber = phoneNumber;
   }

   public Double getLatitude() {
      return latitude;
   }

   public void setLatitude(Double latitude) {
      this.latitude = latitude;
   }

   public Double getLongitude() {
      return longitude;
   }

   public void setLongitude(Double longitude) {
      this.longitude = longitude;
   }
}
