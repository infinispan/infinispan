package org.infinispan.globalstate.impl;

import java.util.EnumSet;
import java.util.List;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.marshall.ProtoStreamTypeIds;
import org.infinispan.protostream.annotations.ProtoFactory;
import org.infinispan.protostream.annotations.ProtoField;
import org.infinispan.protostream.annotations.ProtoTypeId;

/**
 * Cache State information stored in a cluster-wide cache
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@ProtoTypeId(ProtoStreamTypeIds.CACHE_STATE)
public class CacheState {
   private final String template;
   private final String configuration;
   private final EnumSet<CacheContainerAdmin.AdminFlag> flags;


   public CacheState(String template, String configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      this.template = template;
      this.configuration = configuration;
      this.flags = flags.clone();
   }

   @ProtoFactory
   CacheState(String template, String configuration, List<CacheContainerAdmin.AdminFlag> flagIt) {
      this.template = template;
      this.configuration = configuration;
      this.flags = flagIt.isEmpty() ? EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class) : EnumSet.copyOf(flagIt);
   }

   @ProtoField(1)
   public String getTemplate() {
      return template;
   }

   @ProtoField(2)
   public String getConfiguration() {
      return configuration;
   }

   @ProtoField(number = 3, name = "flags")
   Iterable<CacheContainerAdmin.AdminFlag> getFlagIt() {
      return flags;
   }

   public EnumSet<CacheContainerAdmin.AdminFlag> getFlags() {
      return flags;
   }
}
