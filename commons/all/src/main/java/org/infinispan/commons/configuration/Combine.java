package org.infinispan.commons.configuration;

/**
 * <p>
 * Defines how {@link org.infinispan.commons.configuration.attributes.Attribute}s and {@link org.infinispan.commons.configuration.attributes.ConfigurationElement}s
 * are combined when overlaying a configuration (the overlay) on top of another (the template) using {@link Builder#read(Object, Combine)}.
 * </p>
 * <p>
 * The strategies for combining attributes:
 * <li>
 *    <ul><strong>MERGE</strong>: the overlay attributes overwrite those of the template only if they have been modified</ul>
 *    <ul><strong>OVERRIDE</strong>: the overlay attributes are used, regardless of their modification status.</ul>
 * </li>
 * <p>
 * The strategies for combining repeatable attributes:
 * <li>
 *    <ul><strong>MERGE</strong>: repeatable attributes will be a combination of both the template and the overlay.</ul>
 *    <ul><strong>OVERRIDE</strong>: only the overlay repeatable attributes will be used.</ul>
 * </li>
 * </p>
 *
 * @since 15.0
 **/
public class Combine {
   public static final Combine DEFAULT = new Combine(RepeatedAttributes.OVERRIDE, Attributes.MERGE);

   public enum RepeatedAttributes {
      MERGE,
      OVERRIDE
   }

   public enum Attributes {
      MERGE,
      OVERRIDE
   }

   private final RepeatedAttributes repeatedAttributes;
   private final Attributes attributes;

   public Combine(RepeatedAttributes repeatedAttributes, Attributes attributes) {
      this.repeatedAttributes = repeatedAttributes;
      this.attributes = attributes;
   }

   public RepeatedAttributes repeatedAttributes() {
      return repeatedAttributes;
   }

   public Attributes attributes() {
      return attributes;
   }

   @Override
   public String toString() {
      return "Combine{" +
            "repeatedAttributes=" + repeatedAttributes +
            ", attributes=" + attributes +
            '}';
   }
}
