package org.infinispan.query.remote.search;

import com.google.protobuf.Descriptors;
import org.antlr.runtime.tree.Tree;
import org.hibernate.hql.ast.common.JoinType;
import org.hibernate.hql.ast.origin.hql.resolve.path.PathedPropertyReference;
import org.hibernate.hql.ast.origin.hql.resolve.path.PathedPropertyReferenceSource;
import org.hibernate.hql.ast.origin.hql.resolve.path.PropertyPath;
import org.hibernate.hql.ast.spi.QueryResolverDelegate;
import org.hibernate.hql.lucene.internal.ast.HSearchEmbeddedEntityTypeDescriptor;
import org.hibernate.hql.lucene.internal.ast.HSearchPropertyTypeDescriptor;
import org.hibernate.hql.lucene.internal.ast.HSearchTypeDescriptor;
import org.hibernate.hql.lucene.internal.logging.Log;
import org.hibernate.hql.lucene.internal.logging.LoggerFactory;
import org.infinispan.protostream.SerializationContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * This extends the ANTLR generated AST walker to transform a parsed tree
 * into a Lucene Query and collect the target entity types of the query.
 * <br/>
 * <b>TODO:</b>
 *   <li>It is currently human-written but should evolve into another ANTLR
 * generated tree walker, not extending GeneratedHQLResolver but using its
 * output as a generic normalization AST transformer.</li>
 *   <li>We are assembling the Lucene Query directly, but this doesn't take
 *   into account parameter types which might need some transformation;
 *   the Hibernate Search provided {@code QueryBuilder} could do this.</li>
 *   <li>Implement more predicates</li>
 *   <li>Support multiple types being targeted by the Query</li>
 *   <li>Support positional parameters (currently only consumed named parameters)<li>
 *
 * @author Sanne Grinovero <sanne@hibernate.org> (C) 2011 Red Hat Inc.
 * @author Gunnar Morling
 * @author anistor@redhat.com
 */
class IspnLuceneQueryResolverDelegate implements QueryResolverDelegate {

	private enum Status {
		DEFINING_SELECT, DEFINING_FROM;
	}

	private static final Log log = LoggerFactory.make();
	/**
	 * Persister space: keep track of aliases and entity names.
	 */
	private final Map<String, String> aliasToEntityType = new HashMap<String, String>();

	private Status status;

   private Descriptors.Descriptor targetType;

   private final SerializationContext serializationContext;

   public IspnLuceneQueryResolverDelegate(SerializationContext serializationContext) {
      this.serializationContext = serializationContext;
   }

   Descriptors.Descriptor getTargetType() {
      return targetType;
   }

	/**
	 * See rule entityName
	 */
	@Override
	public void registerPersisterSpace(Tree entityName, Tree alias) {
		String put = aliasToEntityType.put( alias.getText(), entityName.getText() );
		if ( put != null && !put.equalsIgnoreCase( entityName.getText() ) ) {
			throw new UnsupportedOperationException(
					"Alias reuse currently not supported: alias " + alias.getText()
					+ " already assigned to type " + put );
		}
      Descriptors.Descriptor targetedType = serializationContext.getMessageDescriptor(entityName.getText());
      if ( targetType != null ) {
			throw new IllegalStateException( "Can't target multiple types: " + targetType + " already selected before " + targetedType );
		}
		targetType = targetedType;
	}

	@Override
	public boolean isUnqualifiedPropertyReference() {
		return true; // TODO - very likely always true for our supported use cases
	}

	@Override
	public PathedPropertyReferenceSource normalizeUnqualifiedPropertyReference(Tree property) {
		if ( aliasToEntityType.containsKey( property.getText() ) ) {
			return normalizeQualifiedRoot( property );
		}

		return normalizeProperty(
            new ProtobufValueWrapperTypeDescriptor(targetType),
				Collections.<String>emptyList(),
				property.getText()
		);
	}

	@Override
	public boolean isPersisterReferenceAlias() {
		if ( aliasToEntityType.size() == 1 ) {
			return true; // should be safe
		}
		else {
			throw new UnsupportedOperationException( "Unexpected use case: not implemented yet?" );
		}
	}

	@Override
	public PathedPropertyReferenceSource normalizeUnqualifiedRoot(Tree identifier382) {
		throw new UnsupportedOperationException( "Not yet implemented" );
	}

	@Override
	public PathedPropertyReferenceSource normalizeQualifiedRoot(Tree root) {
		String entityNameForAlias = aliasToEntityType.get( root.getText() );

		if ( entityNameForAlias == null ) {
			throw log.getUnknownAliasException( root.getText() );
		}

      Descriptors.Descriptor descriptor = serializationContext.getMessageDescriptor(entityNameForAlias);

		return new PathedPropertyReference(
				root.getText(),
            new ProtobufValueWrapperTypeDescriptor(descriptor),
				true
		);
	}

	@Override
	public PathedPropertyReferenceSource normalizePropertyPathIntermediary(
			PropertyPath path, Tree propertyName) {

      ProtobufValueWrapperTypeDescriptor sourceType = (ProtobufValueWrapperTypeDescriptor) path.getLastNode().getType();

		if ( !sourceType.hasProperty( propertyName.getText() ) ) {
			throw log.getNoSuchPropertyException( sourceType.toString(), propertyName.getText() );
		}

      Descriptors.Descriptor descriptor = sourceType.getMessageDescriptor().findFieldByName(propertyName.getText()).getMessageType();

      PathedPropertyReference property = new PathedPropertyReference(propertyName.getText(), new ProtobufValueWrapperTypeDescriptor(descriptor), false);

		return property;
	}

	@Override
	public PathedPropertyReferenceSource normalizeIntermediateIndexOperation(
			PathedPropertyReferenceSource propertyReferenceSource, Tree collectionProperty, Tree selector) {
		throw new UnsupportedOperationException( "Not yet implemented" );
	}

	@Override
	public void normalizeTerminalIndexOperation(
			PathedPropertyReferenceSource propertyReferenceSource, Tree collectionProperty, Tree selector) {
		throw new UnsupportedOperationException( "Not yet implemented" );
	}

	@Override
	public PathedPropertyReferenceSource normalizeUnqualifiedPropertyReferenceSource(Tree identifier394) {
		throw new UnsupportedOperationException( "Not yet implemented" );
	}

	@Override
	public PathedPropertyReferenceSource normalizePropertyPathTerminus(PropertyPath path, Tree propertyNameNode) {
		// receives the property name on a specific entity reference _source_
		return normalizeProperty( (HSearchTypeDescriptor) path.getLastNode().getType(), path.getNodeNamesWithoutAlias(), propertyNameNode.getText() );
	}

	private PathedPropertyReferenceSource normalizeProperty(HSearchTypeDescriptor type, List<String> path, String propertyName) {

		if ( !type.hasProperty( propertyName ) ) {
			throw log.getNoSuchPropertyException( type.toString(), propertyName );
		}

		if ( status != Status.DEFINING_SELECT && !type.isEmbedded( propertyName ) && type.isAnalyzed( propertyName ) ) {
			throw log.getQueryOnAnalyzedPropertyNotSupportedException( type.getIndexedEntityType().getCanonicalName(), propertyName );
		}

		if ( type.isEmbedded( propertyName ) ) {
         ProtobufValueWrapperTypeDescriptor sourceType = (ProtobufValueWrapperTypeDescriptor) type;
         Descriptors.Descriptor descriptor = sourceType.getMessageDescriptor().findFieldByName(propertyName).getMessageType();
         return new PathedPropertyReference(
               propertyName,
               new ProtobufValueWrapperTypeDescriptor(descriptor),
               false);
		}
		else {
			return new PathedPropertyReference(
					propertyName,
					new HSearchPropertyTypeDescriptor(),
					false
			);
		}
	}

	@Override
	public void pushFromStrategy(
			JoinType joinType,
			Tree assosiationFetchTree,
			Tree propertyFetchTree,
			Tree alias) {
		throw new UnsupportedOperationException( "Not yet implemented" );
	}

	@Override
	public void pushSelectStrategy() {
		status = Status.DEFINING_SELECT;
	}

	@Override
	public void popStrategy() {
		status = null;
	}

	@Override
	public void propertyPathCompleted(PropertyPath path) {
		if ( status == Status.DEFINING_SELECT && path.getLastNode().getType() instanceof HSearchEmbeddedEntityTypeDescriptor ) {
			HSearchEmbeddedEntityTypeDescriptor type = (HSearchEmbeddedEntityTypeDescriptor) path.getLastNode().getType();

			throw log.getProjectionOfCompleteEmbeddedEntitiesNotSupportedException(
					type.getIndexedEntityType().getCanonicalName(),
					path.asStringPathWithoutAlias()
			);
		}
	}
}
