package org.infinispan.nearcache.cdi;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.inject.Named;

/**
 * CDI controller
 *
 * @author Galder Zamarreño
 * @since // TODO
 */
@Named @RequestScoped
public class AddressController {

   @Inject
   private AddressDao dao;
   private String name;
   private String street;
   private String result;

   public void store() {
      result = dao.storeAddress(name, new Address().street(street));
   }

   public void get() {
      dao.getAddress(name);
   }

   public void remove() {
      result = dao.removeAddress(name);
   }

   public String getName() { return name; }
   public void setName(String name) { this.name = name; }
   public String getStreet() { return street; }
   public void setStreet(String street) { this.street = street; }
   public String getResult() { return result; }

}
