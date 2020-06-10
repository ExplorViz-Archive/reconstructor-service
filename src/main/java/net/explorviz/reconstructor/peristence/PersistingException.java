package net.explorviz.reconstructor.peristence;

/**
 * Thrown if a record could not be persisted.
 */
public class PersistingException extends Exception {
  public PersistingException() {
  }

  public PersistingException(String message) {
    super(message);
  }

  public PersistingException(String message, Throwable cause) {
    super(message, cause);
  }

  public PersistingException(Throwable cause) {
    super(cause);
  }

  public PersistingException(String message, Throwable cause, boolean enableSuppression,
                             boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }
}

