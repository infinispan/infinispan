<#-- @ftlvariable name="props" type="org.rhq.helpers.pluginGen.Props" -->
<?xml version="1.0"?>
<plugin name="${props.pluginName}"
        displayName="${props.pluginName}Plugin"
        description="${props.pluginDescription}"
<#if props.usePluginLifecycleListenerApi>
        pluginLifecycleListener="${props.componentClass}"
</#if>
        package="${props.pkg}"
        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
        xmlns="urn:xmlns:rhq-plugin"
        xmlns:c="urn:xmlns:rhq-configuration">

<#if props.dependsOnJmxPlugin>
   <depends plugin="JMX" useClasses="true"/>
</#if>

   <${props.category.lowerName} <#include "ispnDescriptorMain.ftl"/>

   <#-- Those are the embedded children -->
   <#list props.children as props>
       <${props.category.lowerName} <#include "./ispnDescriptorMain.ftl"/>
       </${props.category.lowerName}>
   </#list>
   </${props.category.lowerName}>

</plugin>
