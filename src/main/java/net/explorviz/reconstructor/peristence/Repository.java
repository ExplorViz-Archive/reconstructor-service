package net.explorviz.reconstructor.peristence;

/**
 * Manages (usually persistent) access to a collection of objects.
 *
 * @param <T> type of objects the repository manages.
 */
public interface Repository<T> {

  /**
   * Inserts an item into the repository
   */
  void add(T item) throws PersistingException;


}
