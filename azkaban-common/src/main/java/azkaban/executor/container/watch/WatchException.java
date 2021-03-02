package azkaban.executor.container.watch;

public class WatchException extends RuntimeException {

  public WatchException() {
  }

  public WatchException(String message) {
    super(message);
  }

  public WatchException(String message, Throwable cause) {
    super(message, cause);
  }

  public WatchException(Throwable cause) {
    super(cause);
  }
}
