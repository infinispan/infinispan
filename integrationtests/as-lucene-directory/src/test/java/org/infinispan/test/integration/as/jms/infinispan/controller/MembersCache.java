package org.infinispan.test.integration.as.jms.infinispan.controller;

import javax.annotation.Resource;
import javax.ejb.Stateless;

/**
 * A simple cache to store some information about a newly registered member.
 *
 * @author Davide D'Alto
 */
@Stateless
public class MembersCache {

   @Resource(lookup = "java:jboss/infinispan/cache/membersCache/default")
   private org.infinispan.Cache<String, String> cache;

   public String get(String key) {
      return this.cache.get(key);
   }

   public void put(String key, String value) {
      this.cache.put(key, value);
   }

}
