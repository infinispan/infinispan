package org.infinispan.server.hotrod.configuration;

import java.util.Arrays;
import java.util.List;

import org.infinispan.commons.configuration.ConfigurationInfo;
import org.infinispan.commons.configuration.elements.DefaultElementDefinition;
import org.infinispan.commons.configuration.elements.ElementDefinition;

/**
 * @since 10.0
 */
public class PolicyConfiguration implements ConfigurationInfo {
   private static final ElementDefinition ELEMENT_DEFINITION = new DefaultElementDefinition("policy");

   private BooleanElementConfiguration forwardSecrecy, noActive, noAnonymous, noDictionary, noPlainText, passCredentials;

   private final List<ConfigurationInfo> configurations;

   PolicyConfiguration(BooleanElementConfiguration forwardSecrecy, BooleanElementConfiguration noActive,
                       BooleanElementConfiguration noAnonymous, BooleanElementConfiguration noDictionary,
                       BooleanElementConfiguration noPlainText, BooleanElementConfiguration passCredentials) {
      this.forwardSecrecy = forwardSecrecy;
      this.noActive = noActive;
      this.noAnonymous = noAnonymous;
      this.noDictionary = noDictionary;
      this.noPlainText = noPlainText;
      this.passCredentials = passCredentials;
      this.configurations = Arrays.asList(forwardSecrecy, noActive, noAnonymous, noDictionary, noPlainText, passCredentials);
   }

   @Override
   public ElementDefinition getElementDefinition() {
      return ELEMENT_DEFINITION;
   }

   @Override
   public List<ConfigurationInfo> subElements() {
      return configurations;
   }

   BooleanElementConfiguration forwardSecrecy() {
      return forwardSecrecy;
   }

   BooleanElementConfiguration noActive() {
      return noActive;
   }

   BooleanElementConfiguration noAnonymous() {
      return noAnonymous;
   }

   BooleanElementConfiguration noDictionary() {
      return noDictionary;
   }

   BooleanElementConfiguration noPlainText() {
      return noPlainText;
   }

   BooleanElementConfiguration passCredentials() {
      return passCredentials;
   }

   @Override
   public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      PolicyConfiguration that = (PolicyConfiguration) o;

      if (!forwardSecrecy.equals(that.forwardSecrecy)) return false;
      if (!noActive.equals(that.noActive)) return false;
      if (!noAnonymous.equals(that.noAnonymous)) return false;
      if (!noDictionary.equals(that.noDictionary)) return false;
      if (!noPlainText.equals(that.noPlainText)) return false;
      return passCredentials.equals(that.passCredentials);
   }

   @Override
   public int hashCode() {
      int result = forwardSecrecy.hashCode();
      result = 31 * result + noActive.hashCode();
      result = 31 * result + noAnonymous.hashCode();
      result = 31 * result + noDictionary.hashCode();
      result = 31 * result + noPlainText.hashCode();
      result = 31 * result + passCredentials.hashCode();
      return result;
   }

   @Override
   public String toString() {
      return "PolicyConfiguration{" +
            "forwardSecrecy=" + forwardSecrecy +
            ", noActive=" + noActive +
            ", noAnonymous=" + noAnonymous +
            ", noDictionary=" + noDictionary +
            ", noPlainText=" + noPlainText +
            ", passCredentials=" + passCredentials +
            '}';
   }
}
