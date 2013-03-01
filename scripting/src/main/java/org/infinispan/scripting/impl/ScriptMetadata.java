package org.infinispan.scripting.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.Metadata;

/**
 * ScriptMetadata.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class ScriptMetadata implements Metadata {

   public static enum MetadataProperties {
      NAME("name"), LANGUAGE("language"), MODE("mode"), REDUCER("reducer"), COLLATOR("collator"), COMBINER("combiner"), EXTENSION("extension");

      private final String s;

      MetadataProperties(String s) {
         this.s = s;
      }

      @Override
      public String toString() {
         return s;
      }
   }

   final private Map<MetadataProperties, String> properties;

   private ScriptMetadata(Map<MetadataProperties, String> properties) {
      this.properties = properties;
   }

   public String property(MetadataProperties property) {
      return properties.get(property);
   }


   public String name() {
      return properties.get(MetadataProperties.NAME);
   }

   public ExecutionMode mode() {
      return ExecutionMode.valueOf(properties.get(MetadataProperties.MODE));
   }

   @Override
   public long lifespan() {
      return -1;
   }

   @Override
   public long maxIdle() {
      return -1;
   }

   @Override
   public EntryVersion version() {
      return null;
   }

   @Override
   public Builder builder() {
      return new Builder().properties(properties);
   }

   @Override
   public String toString() {
      return "ScriptMetadata [properties=" + properties + "]";
   }

   public static class Builder implements Metadata.Builder {
      private Map<MetadataProperties, String> properties = new HashMap<>(4);

      public ScriptMetadata.Builder property(MetadataProperties property, String value) {
         properties.put(property, value);
         return this;
      }

      ScriptMetadata.Builder properties(Map<MetadataProperties, String> properties) {
         this.properties = properties;
         return this;
      }

      @Override
      public ScriptMetadata.Builder lifespan(long time, TimeUnit unit) {
         return this;
      }

      @Override
      public ScriptMetadata.Builder lifespan(long time) {
         return this;
      }

      @Override
      public ScriptMetadata.Builder maxIdle(long time, TimeUnit unit) {
         return this;
      }

      @Override
      public ScriptMetadata.Builder maxIdle(long time) {
         return this;
      }

      @Override
      public ScriptMetadata.Builder version(EntryVersion version) {
         return this;
      }

      @Override
      public ScriptMetadata build() {
         return new ScriptMetadata(properties);
      }

      @Override
      public ScriptMetadata.Builder merge(Metadata metadata) {
         return this;
      }

   }

   public static class Externalizer extends AbstractExternalizer<ScriptMetadata> {

      /** The serialVersionUID */
      private static final long serialVersionUID = 6882540247700043640L;

      public Externalizer() {
      }



      @Override
      public Integer getId() {
         return ExternalizerIds.SCRIPT_METADATA;
      }

      @Override
      public Set<Class<? extends ScriptMetadata>> getTypeClasses() {
         return Util.<Class<? extends ScriptMetadata>>asSet(ScriptMetadata.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ScriptMetadata object) throws IOException {
         output.writeInt(object.properties.size());
         for(Entry<MetadataProperties, String> e : object.properties.entrySet()) {
            output.writeUTF(e.getKey().name());
            output.writeUTF(e.getValue());
         }
      }

      @Override
      public ScriptMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Builder builder = new ScriptMetadata.Builder();
         int size = input.readInt();
         for(int i = 0; i < size; i++) {
            String key = input.readUTF();
            String value = input.readUTF();
            builder.property(MetadataProperties.valueOf(key), value);
         }
         return builder.build();
      }

   }


}
