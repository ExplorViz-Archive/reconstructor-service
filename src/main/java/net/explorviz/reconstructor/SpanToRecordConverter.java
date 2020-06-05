package net.explorviz.reconstructor;

import java.time.Instant;
import java.util.Arrays;
import java.util.Optional;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import net.explorviz.landscape.flat.Application;
import net.explorviz.landscape.flat.LandscapeRecord;
import net.explorviz.landscape.flat.Node;
import net.explorviz.reconstructor.verifier.InvalidSpanException;
import net.explorviz.reconstructor.verifier.SpanValidator;
import net.explorviz.trace.EVSpan;

/**
 * Maps {@link net.explorviz.trace.EVSpan} objects to {@link net.explorviz.landscape.flat.LandscapeRecord}
 * objects by extracting the data.
 */
@ApplicationScoped
public class SpanToRecordConverter {


  private SpanValidator validator;

  @Inject
  public SpanToRecordConverter(SpanValidator validator) {
    this.validator = validator;
  }

  /**
   * Converts a {@link EVSpan} to a {@link LandscapeRecord} using the structural information
   * given in the span.
   *
   * @param span the span
   * @return a records containing the structural information of the span
   * @throws InvalidSpanException if the span could not be converted
   */
  public LandscapeRecord toRecord(EVSpan span) throws InvalidSpanException {

    EVSpan validated = validator.validate(span);

    // Create new builder
    LandscapeRecord.Builder recordBuilder =
        LandscapeRecord.newBuilder().setLandscapeToken(span.getLandscapeToken());

    // Use start time as timestamp
    Instant timestamp = Instant
        .ofEpochSecond(span.getStartTime().getSeconds(), span.getStartTime().getNanoAdjust());
    recordBuilder.setTimestamp(timestamp.toEpochMilli());

    // Set node and application
    recordBuilder.setTimestamp(timestamp.toEpochMilli())
        .setNode(new Node(span.getHostIpAddress(), span.getHostname()))
        .setApplication(
            new Application(span.getAppName(), span.getAppPid(), span.getAppLanguage()));


    /*
      By definition getOperationName().split("."):
        Last entry is method name,
        next to last is class name,
        remaining elements form the package name
    */
    String[] operationFqnSplit = span.getOperationName().split("\\.");


    String pkgName =
        String.join(".", Arrays.copyOf(operationFqnSplit, operationFqnSplit.length - 2));
    String className = operationFqnSplit[operationFqnSplit.length - 2];
    String methodName = operationFqnSplit[operationFqnSplit.length - 1];


    recordBuilder.setPackage$(pkgName);
    recordBuilder.setClass$(className);
    recordBuilder.setMethod(methodName);


    return recordBuilder.build();

  }



  /**
   * Throws if the given span is invalid and can't be converted into a valid record
   *
   * @param span the span
   */
  private void validate(EVSpan span) throws InvalidSpanException {

  }



}
