package com.swapnil.demo;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class Configuration {

  private final Properties properties;

  public Configuration() throws IOException {
    properties = new Properties();
    final InputStream inputStream = getClass().getClassLoader().getResourceAsStream("application.properties");
    properties.load(inputStream);
  }

  public String getProperty(String key) {
    return properties.getProperty(key);
  }
}
