package org.infinispan.scripting.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.io.OptionalObjectInputOutput;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.commons.util.InfinispanCollections;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.Metadata;

/**
 * ScriptMetadata. Holds meta information about a script obtained either implicitly by the script
 * name and extension, or explictly by its header
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class ScriptMetadata implements Metadata {
   private final String name;
   private final String language;
   private final ExecutionMode mode;
   private final String extension;
   private final Set<String> parameters;
   private Optional<String> role;

   private Optional<String> reducer;
   private Optional<String> collator;
   private Optional<String> combiner;

   ScriptMetadata(String name, String language, String extension, ExecutionMode mode, Set<String> parameters,
         Optional<String> role, Optional<String> reducer, Optional<String> collator, Optional<String> combiner) {
      this.name = name;
      this.language = language;
      this.extension = extension;
      this.mode = mode;
      this.parameters = Collections.unmodifiableSet(parameters);
      this.role = role;
      this.reducer = reducer;
      this.collator = collator;
      this.combiner = combiner;
   }

   public String language() {
      return language;
   }

   public String extension() {
      return extension;
   }

   public Set<String> parameters() {
      return parameters;
   }

   public Optional<String> role() {
      return role;
   }

   public String name() {
      return name;
   }

   public ExecutionMode mode() {
      return mode;
   }

   public String reducer() {
      return reducer.get();
   }

   public String combiner() {
      return combiner.get();
   }

   public String collator() {
      return collator.get();
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
      return new Builder().name(name).language(language).extension(extension).mode(mode).parameters(parameters);
   }

   @Override
   public String toString() {
      return "ScriptMetadata [name=" + name + ", language=" + language + ", mode=" + mode + ", extension=" + extension
            + ", parameters=" + parameters + ", role=" + role + ", reducer=" + reducer + ", collator=" + collator
            + ", combiner=" + combiner + "]";
   }

   public static class Builder implements Metadata.Builder {
      String name;
      String extension;
      String language;
      ExecutionMode mode;
      Set<String> parameters = InfinispanCollections.emptySet();
      Optional<String> role = Optional.empty();
      Optional<String> combiner = Optional.empty();
      Optional<String> collator = Optional.empty();
      Optional<String> reducer = Optional.empty();

      public ScriptMetadata.Builder name(String name) {
         this.name = name;
         return this;
      }

      public ScriptMetadata.Builder mode(ExecutionMode mode) {
         this.mode = mode;
         return this;
      }

      public ScriptMetadata.Builder extension(String extension) {
         this.extension = extension;
         return this;
      }

      public ScriptMetadata.Builder language(String language) {
         this.language = language;
         return this;
      }

      public ScriptMetadata.Builder parameters(Set<String> parameters) {
         this.parameters = parameters;
         return this;
      }

      public ScriptMetadata.Builder role(String role) {
         this.role = Optional.of(role);
         return this;
      }

      public ScriptMetadata.Builder reducer(String reducer) {
         this.reducer = Optional.of(reducer);
         return this;
      }

      public ScriptMetadata.Builder collator(String collator) {
         this.collator = Optional.of(collator);
         return this;
      }

      public ScriptMetadata.Builder combiner(String combiner) {
         this.combiner = Optional.of(combiner);
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
         return new ScriptMetadata(name, language, extension, mode, parameters, role, reducer, collator, combiner);
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
         return Util.<Class<? extends ScriptMetadata>> asSet(ScriptMetadata.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ScriptMetadata object) throws IOException {
         output.writeUTF(object.name);
         output.writeUTF(object.extension);
         output.writeUTF(object.language);
         output.writeUTF(object.mode.name());
         writeSet(output, object.parameters);

         OptionalObjectInputOutput.writeOptional(output, object.role);
         OptionalObjectInputOutput.writeOptional(output, object.reducer);
         OptionalObjectInputOutput.writeOptional(output, object.collator);
         OptionalObjectInputOutput.writeOptional(output, object.combiner);
      }

      @Override
      public ScriptMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         Builder builder = new ScriptMetadata.Builder();
         builder.name(input.readUTF()).extension(input.readUTF()).language(input.readUTF())
               .mode(ExecutionMode.valueOf(input.readUTF()));
         builder.parameters(readSet(input));

         OptionalObjectInputOutput.readOptionalUTF(input).ifPresent(s -> builder.role(s));
         OptionalObjectInputOutput.readOptionalUTF(input).ifPresent(s -> builder.reducer(s));
         OptionalObjectInputOutput.readOptionalUTF(input).ifPresent(s -> builder.collator(s));
         OptionalObjectInputOutput.readOptionalUTF(input).ifPresent(s -> builder.combiner(s));
         return builder.build();
      }

      private void writeSet(ObjectOutput output, Set<String> set) throws IOException {
         output.writeInt(set.size());
         for (String s : set) {
            output.writeUTF(s);
         }
      }

      private Set<String> readSet(ObjectInput input) throws IOException {
         int count = input.readInt();
         if (count == 0) {
            return InfinispanCollections.emptySet();
         } else {
            Set<String> set = new HashSet<>(count);
            for (int i = 0; i < count; i++) {
               set.add(input.readUTF());
            }
            return set;
         }
      }

   }
}
