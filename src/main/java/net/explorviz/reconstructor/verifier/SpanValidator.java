package net.explorviz.reconstructor.verifier;

import net.explorviz.trace.EVSpan;

/**
 * Validates and possibly manipulates {@link EVSpan}s prior to processing.
 */
public interface SpanValidator {

  /**
   * Validates the given span and returns possibly sanitized version.
   *
   * @param span the span
   * @return a valid version of the given span
   * @throws InvalidSpanException if the span is invalid and can't be recovered
   */
  EVSpan validate(EVSpan span) throws InvalidSpanException;

}
