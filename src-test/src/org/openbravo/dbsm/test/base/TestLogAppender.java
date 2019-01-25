package org.openbravo.dbsm.test.base;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.Core;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;

import java.util.ArrayList;
import java.util.List;

@Plugin(name = "TestLogAppender", category = Core.CATEGORY_NAME, elementType = Appender.ELEMENT_TYPE)
public class TestLogAppender extends AbstractAppender {
  private static List<String> msgs = new ArrayList<>();

  protected TestLogAppender(String name, Filter filter) {
    super(name, filter, null);
  }

  @PluginFactory
  public static TestLogAppender createAppender(@PluginAttribute("name") String name,
      @PluginElement("Filter") Filter filter) {
    return new TestLogAppender(name, filter);
  }

  @Override
  public void append(LogEvent logEvent) {
    if (logEvent.getLevel().isMoreSpecificThan(Level.WARN)) {
      msgs.add(logEvent.getLevel() + ": " + logEvent.getMessage());
    }
  }

  public static List<String> getWarnAndErrors() {
    return msgs;
  }

  public static void reset() {
    msgs = new ArrayList<>();
  }

}
