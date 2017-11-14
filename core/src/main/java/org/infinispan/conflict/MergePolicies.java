package org.infinispan.conflict;

/**
 * @author Ryan Emerson
 * @since 9.1
 * @deprecated since 9.2 please use {@link MergePolicy} instead
 */
@Deprecated
public class MergePolicies {

   public static final EntryMergePolicy PREFERRED_ALWAYS = MergePolicy.PREFERRED_ALWAYS;

   public static final EntryMergePolicy PREFERRED_NON_NULL = MergePolicy.PREFERRED_NON_NULL;

   public static final EntryMergePolicy REMOVE_ALL = MergePolicy.REMOVE_ALL;
}
