package org.infinispan.cli.patching;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

import org.infinispan.commons.dataconversion.internal.Json;
import org.infinispan.commons.dataconversion.internal.JsonSerialization;

/**
 * @author Tristan Tarrant &lt;tristan@infinispan.org&gt;
 * @since 11.0
 **/
public class PatchInfo implements JsonSerialization {
   private static final String BRAND_NAME = "brandName";
   private static final String SOURCE_VERSION = "sourceVersion";
   private static final String TARGET_VERSION = "targetVersion";
   private static final String QUALIFIER = "qualifier";
   private static final String CREATION_DATE = "creationDate";
   private static final String INSTALLATION_DATE = "installationDate";
   private static final String OPERATIONS = "operations";

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


   PatchInfo(String brandName, String sourceVersion, String targetVersion, String qualifier, Date creationDate,
             Date installationDate, List<PatchOperation> operations) {
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

   public static PatchInfo fromJson(Json json) {
      Json brandName = json.at(BRAND_NAME);
      Json sourceVersion = json.at(SOURCE_VERSION);
      Json targetVersion = json.at(TARGET_VERSION);
      Json qualifier = json.at(QUALIFIER);
      Json creationDate = json.at(CREATION_DATE);
      Json installationDate = json.at(INSTALLATION_DATE);
      Json operations = json.at(OPERATIONS);

      String brandValue = brandName != null ? brandName.asString() : null;
      String sourceValue = sourceVersion != null ? sourceVersion.asString() : null;
      String targetValue = targetVersion != null ? targetVersion.asString() : null;
      String qualifierValue = qualifier != null ? qualifier.asString() : null;
      Date creationValue = creationDate != null ? creationDate.isNull() ? null : new Date(creationDate.asLong()) : null;
      Date installationValue = installationDate != null ? installationDate.isNull() ? null : new Date(installationDate.asLong()) : null;
      List<PatchOperation> operationsValue = operations != null ? operations.asJsonList().stream().map(PatchOperation::fromJson).collect(Collectors.toList()) : null;
      return new PatchInfo(brandValue, sourceValue, targetValue, qualifierValue, creationValue, installationValue, operationsValue);
   }

   @Override
   public Json toJson() {
      return Json.object()
            .set(BRAND_NAME, brandName)
            .set(SOURCE_VERSION, sourceVersion)
            .set(TARGET_VERSION, targetVersion)
            .set(QUALIFIER, qualifier)
            .set(CREATION_DATE, creationDate != null ? creationDate.getTime() : null)
            .set(INSTALLATION_DATE, installationDate != null ? installationDate.getTime() : null)
            .set(OPERATIONS, Json.make(operations));
   }
}
