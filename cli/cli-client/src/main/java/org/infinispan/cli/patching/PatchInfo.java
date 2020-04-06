package org.infinispan.cli.patching;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.annotation.JsonNaming;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
@JsonNaming(PropertyNamingStrategy.KebabCaseStrategy.class)
public class PatchInfo {
   private final Date creationDate;
   private Date installationDate;
   private final String brandName;
   private final String sourceVersion;
   private final String targetVersion;
   private final String qualifier;
   private final List<PatchOperation> operations;

   public PatchInfo(String brandName, String sourceVersion, String targetVersion, String qualifier) {
      this(brandName, sourceVersion, targetVersion, qualifier, new Date(), null, new ArrayList<>());
   }

   @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
   PatchInfo(@JsonProperty("brandName") String brandName, @JsonProperty("sourceVersion") String sourceVersion, @JsonProperty("targetVersion") String targetVersion,
             @JsonProperty("qualifier") String qualifier, @JsonProperty("creationDate") Date creationDate, @JsonProperty("installationDate") Date installationDate,
             @JsonProperty("operations") List<PatchOperation> operations) {
      this.brandName = brandName;
      this.sourceVersion = sourceVersion;
      this.targetVersion = targetVersion;
      this.qualifier = qualifier;
      this.creationDate = creationDate;
      this.installationDate = installationDate;
      this.operations = operations;
   }

   public String getQualifier() {
      return qualifier;
   }

   public String getBrandName() {
      return brandName;
   }

   public String getSourceVersion() {
      return sourceVersion;
   }

   public String getTargetVersion() {
      return targetVersion;
   }

   public List<PatchOperation> getOperations() {
      return operations;
   }

   public Date getCreationDate() {
      return creationDate;
   }

   public Date getInstallationDate() {
      return installationDate;
   }

   public void setInstallationDate(Date installationDate) {
      this.installationDate = installationDate;
   }

   public String toString() {
      return brandName + " patch target=" + targetVersion + (qualifier.isEmpty() ? "" : "(" + qualifier + ")") + " source=" + sourceVersion + " created=" + creationDate + (installationDate != null ? " installed=" + installationDate : "");
   }
}
