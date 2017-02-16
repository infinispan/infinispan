package org.jboss.as.clustering.infinispan.subsystem;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

import org.jboss.as.controller.OperationFailedException;
import org.jboss.as.controller.operations.validation.ModelTypeValidator;
import org.jboss.dmr.ModelNode;
import org.jboss.dmr.ModelType;

/**
 * @author Tristan Tarrant
 * @since 8.2
 */
public class DateTimeValidator extends ModelTypeValidator {
    private final DateTimeFormatter dateTimeFormatter;

    public DateTimeValidator(DateTimeFormatter dateTimeFormatter, boolean nullable, boolean allowExpressions) {
        super(ModelType.STRING, nullable, allowExpressions, true);
        this.dateTimeFormatter = dateTimeFormatter;
    }

    public void validateParameter(String parameterName, ModelNode value) throws OperationFailedException {
        super.validateParameter(parameterName, value);
        if(value.isDefined() && value.getType() != ModelType.EXPRESSION) {
            try {
                ZonedDateTime.parse(value.asString(), dateTimeFormatter);
            } catch (DateTimeParseException e) {
                throw new OperationFailedException(e);
            }
        }

    }
}
