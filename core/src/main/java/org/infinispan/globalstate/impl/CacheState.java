package org.infinispan.globalstate.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

import org.infinispan.commons.api.CacheContainerAdmin;
import org.infinispan.commons.marshall.AdvancedExternalizer;
import org.infinispan.commons.marshall.Ids;
import org.infinispan.commons.marshall.MarshallUtil;
import org.infinispan.commons.marshall.SerializeWith;

/**
 * Cache State information stored in a cluster-wide cache
 *
 * @author Tristan Tarrant
 * @since 9.2
 */
@SerializeWith(CacheState.Externalizer.class)
public class CacheState {
   private final String template;
   private final String configuration;
   private final EnumSet<CacheContainerAdmin.AdminFlag> flags;


   CacheState(String template, String configuration, EnumSet<CacheContainerAdmin.AdminFlag> flags) {
      this.template = template;
      this.configuration = configuration;
      this.flags = flags.clone();
   }

   public String getTemplate() {
      return template;
   }

   public String getConfiguration() {
      return configuration;
   }

   public EnumSet<CacheContainerAdmin.AdminFlag> getFlags() {
      return flags;
   }

   public static class Externalizer implements AdvancedExternalizer<CacheState> {

      @Override
      public void writeObject(ObjectOutput output, CacheState state) throws IOException {
         MarshallUtil.marshallString(state.template, output);
         MarshallUtil.marshallString(state.configuration, output);
         output.writeObject(state.flags);
      }

      @Override
      public CacheState readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String template = MarshallUtil.unmarshallString(input);
         String configuration = MarshallUtil.unmarshallString(input);
         EnumSet<CacheContainerAdmin.AdminFlag> flags = (EnumSet<CacheContainerAdmin.AdminFlag>) input.readObject();
         return new CacheState(template, configuration, flags == null ? EnumSet.noneOf(CacheContainerAdmin.AdminFlag.class) : flags);
      }

      @Override
      public Set<Class<? extends CacheState>> getTypeClasses() {
         return Collections.singleton(CacheState.class);
      }

      @Override
      public Integer getId() {
         return Ids.CACHE_STATE;
      }
   }
}
