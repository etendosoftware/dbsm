package org.openbravo.dbsm.test.base;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

public class TestLogAppender extends AppenderSkeleton {
  private static List<String> msgs = new ArrayList<String>();

  @Override
  protected void append(LoggingEvent logEvent) {
    if (logEvent.getLevel().isGreaterOrEqual(Level.WARN)) {
      msgs.add(logEvent.getLevel() + ": " + logEvent.getMessage());
    }

  }

  public static List<String> getWarnAndErrors() {
    return msgs;
  }

  public static void reset() {
    msgs = new ArrayList<String>();
  }

  @Override
  public void close() {
  }

  @Override
  public boolean requiresLayout() {
    return false;
  }

}
