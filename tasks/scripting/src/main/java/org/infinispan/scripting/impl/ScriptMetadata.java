package org.infinispan.scripting.impl;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commands.CommandInvocationId;
import org.infinispan.commands.InvocationRecord;
import org.infinispan.commons.marshall.AbstractExternalizer;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.Metadata;

/**
 * ScriptMetadata. Holds meta information about a script obtained either implicitly by the script
 * name and extension, or explicitly by its header. See the "Script metadata" chapter in the User Guide for
 * the syntax and format.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class ScriptMetadata implements Metadata {
   private final String name;
   private final ExecutionMode mode;
   private final String extension;
   private final Set<String> parameters;
   // TODO: using Optional as field type is an anti-pattern
   private final Optional<String> language;
   private final Optional<String> role;
   private final Optional<String> reducer;
   private final Optional<String> collator;
   private final Optional<String> combiner;
   private final DataType dataType;

   ScriptMetadata(String name, Optional<String> language, String extension, ExecutionMode mode, Set<String> parameters,
         Optional<String> role, Optional<String> reducer, Optional<String> collator, Optional<String> combiner,
         DataType dataType) {
      this.name = name;
      this.language = language;
      this.extension = extension;
      this.mode = mode;
      this.parameters = Collections.unmodifiableSet(parameters);
      this.role = role;
      this.reducer = reducer;
      this.collator = collator;
      this.combiner = combiner;
      this.dataType = dataType;
   }

   public Optional<String> language() {
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

   public Optional<String> reducer() {
      return reducer;
   }

   public Optional<String> combiner() {
      return combiner;
   }

   public Optional<String> collator() {
      return collator;
   }

   public DataType dataType() {
      return dataType;
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
   public InvocationRecord lastInvocation() {
      return null;
   }

   @Override
   public InvocationRecord invocation(CommandInvocationId id) {
      return null;
   }

   @Override
   public Builder builder() {
      Builder builder = new Builder().name(name).extension(extension).mode(mode).parameters(parameters)
            .dataType(dataType);
      language.ifPresent(builder::language);
      role.ifPresent(builder::role);
      reducer.ifPresent(builder::reducer);
      collator.ifPresent(builder::collator);
      combiner.ifPresent(builder::combiner);
      return builder;
   }

   @Override
   public String toString() {
      return "ScriptMetadata [name=" + name + ", language=" + language + ", mode=" + mode + ", extension=" + extension
            + ", parameters=" + parameters + ", role=" + role + ", reducer=" + reducer + ", collator=" + collator
            + ", combiner=" + combiner + ", dataType=" + dataType + "]";
   }

   public static class Builder implements Metadata.Builder {
      String name;
      String extension;
      Optional<String> language = Optional.empty();
      ExecutionMode mode;
      Set<String> parameters = Collections.emptySet();
      Optional<String> role = Optional.empty();
      Optional<String> combiner = Optional.empty();
      Optional<String> collator = Optional.empty();
      Optional<String> reducer = Optional.empty();
      DataType dataType = DataType.DEFAULT;

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
         this.language = Optional.of(language);
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

      public ScriptMetadata.Builder dataType(DataType dataType) {
         this.dataType = dataType;
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
      public Metadata.Builder invocation(CommandInvocationId id, Object previousValue, Metadata previousMetadata, long timestamp) {
         return this;
      }

      @Override
      public Metadata.Builder invocations(InvocationRecord invocations) {
         return this;
      }

      @Override
      public InvocationRecord invocations() {
         return null;
      }

      @Override
      public ScriptMetadata build() {
         return new ScriptMetadata(name, language, extension, mode, parameters, role, reducer, collator, combiner, dataType);
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
         return Collections.singleton(ScriptMetadata.class);
      }

      @Override
      public void writeObject(ObjectOutput output, ScriptMetadata object) throws IOException {
         output.writeUTF(object.name);
         output.writeUTF(object.extension);
         output.writeUTF(object.mode.name());
         output.writeObject(object.parameters);
         output.writeObject(object.language);
         output.writeObject(object.role);
         output.writeObject(object.reducer);
         output.writeObject(object.collator);
         output.writeObject(object.combiner);
         output.writeObject(object.dataType);
      }

      @Override
      public ScriptMetadata readObject(ObjectInput input) throws IOException, ClassNotFoundException {
         String name = input.readUTF();
         String extension = input.readUTF();
         ExecutionMode mode = ExecutionMode.valueOf(input.readUTF());
         Set<String> parameters = (Set<String>) input.readObject();

         Optional<String> language = (Optional<String>) input.readObject();
         Optional<String> role = (Optional<String>) input.readObject();
         Optional<String> reducer = (Optional<String>) input.readObject();
         Optional<String> collator = (Optional<String>) input.readObject();
         Optional<String> combiner = (Optional<String>) input.readObject();
         DataType dataType = (DataType) input.readObject();

         return new ScriptMetadata(name, language, extension, mode, parameters, role, reducer, collator, combiner, dataType);
      }
   }
}
