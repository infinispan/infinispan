package org.infinispan.commons.configuration;

/**
 * <p>
 * Defines how {@link org.infinispan.commons.configuration.attributes.Attribute}s and {@link org.infinispan.commons.configuration.attributes.ConfigurationElement}s
 * are combined when overlaying a configuration (the overlay) on top of another (the template) using {@link Builder#read(Object, Combine)}.
 * </p>
 * The strategies for combining attributes:
 * <ul>
 *    <li><b>MERGE</b> the overlay attributes overwrite those of the template only if they have been modified</li>
 *    <li><b>OVERRIDE</b> the overlay attributes are used, regardless of their modification status.</li>
 * </ul>
 * The strategies for combining repeatable attributes:
 * <ul>
 *    <li><b>MERGE</b> repeatable attributes will be a combination of both the template and the overlay.</li>
 *    <li><b>OVERRIDE</b> only the overlay repeatable attributes will be used.</li>
 * </ul>
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
