package org.infinispan.scripting.impl;

import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_JSON_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.APPLICATION_XML_TYPE;
import static org.infinispan.commons.dataconversion.MediaType.TEXT_PLAIN_TYPE;

import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.infinispan.commons.dataconversion.MediaType;
import org.infinispan.commons.util.Util;
import org.infinispan.container.versioning.EntryVersion;
import org.infinispan.metadata.Metadata;
import org.infinispan.protostream.annotations.ProtoField;

/**
 * ScriptMetadata. Holds meta information about a script obtained either implicitly by the script
 * name and extension, or explicitly by its header. See the "Script metadata" chapter in the User Guide for
 * the syntax and format.
 *
 * @author Tristan Tarrant
 * @since 7.2
 */
public class ScriptMetadata implements Metadata {

   private final static Set<String> TEXT_BASED_MEDIA = Util.asSet(TEXT_PLAIN_TYPE, APPLICATION_JSON_TYPE, APPLICATION_XML_TYPE);

   @ProtoField(number = 1)
   String name;

   @ProtoField(number = 2)
   ExecutionMode mode;

   @ProtoField(number = 3)
   String extension;

   @ProtoField(number = 4, collectionImplementation = HashSet.class)
   Set<String> parameters;

   @ProtoField(number = 5)
   MediaType dataType;

   @ProtoField(number = 6)
   String language;

   @ProtoField(number = 7)
   String role;

   ScriptMetadata() {}

   ScriptMetadata(String name, String language, String extension, ExecutionMode mode, Set<String> parameters,
                  String role, MediaType dataType) {
      this.name = name;
      this.language = language;
      this.extension = extension;
      this.mode = mode;
      this.parameters = Collections.unmodifiableSet(parameters);
      this.role = role;
      this.dataType = dataType;
   }

   public Optional<String> language() {
      return Optional.ofNullable(language);
   }

   public String extension() {
      return extension;
   }

   public Set<String> parameters() {
      return parameters;
   }

   public Optional<String> role() {
      return Optional.ofNullable(role);
   }

   public String name() {
      return name;
   }

   public ExecutionMode mode() {
      return mode;
   }

   public MediaType dataType() {
      if (TEXT_BASED_MEDIA.contains(dataType.getTypeSubtype())) {
         return dataType.withClassType(String.class);
      }
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
   public Builder builder() {
      return new Builder().name(name).extension(extension).mode(mode).parameters(parameters);
   }

   @Override
   public String toString() {
      return "ScriptMetadata{" +
            "name='" + name + '\'' +
            ", mode=" + mode +
            ", extension='" + extension + '\'' +
            ", parameters=" + parameters +
            ", dataType=" + dataType +
            ", language=" + language +
            ", role=" + role +
            '}';
   }

   public static class Builder implements Metadata.Builder {
      String name;
      String extension;
      String language;
      String role;
      ExecutionMode mode;
      Set<String> parameters = Collections.emptySet();
      MediaType dataType = MediaType.APPLICATION_OBJECT;

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
         this.role = role;
         return this;
      }

      public ScriptMetadata.Builder dataType(MediaType dataType) {
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
      public ScriptMetadata build() {
         return new ScriptMetadata(name, language, extension, mode, parameters, role, dataType);
      }

      @Override
      public ScriptMetadata.Builder merge(Metadata metadata) {
         return this;
      }
   }
}
