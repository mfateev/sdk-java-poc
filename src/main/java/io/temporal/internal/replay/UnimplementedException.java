package io.temporal.internal.replay;

/** Indicates a workflow context operation that still needs a Core implementation. */
final class UnimplementedException extends UnsupportedOperationException {
  UnimplementedException(String operation) {
    super(operation + " is unimplemented in the Core workflow context");
  }
}
