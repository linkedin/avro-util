/*
 * Copyright 2022 LinkedIn Corp.
 * Licensed under the BSD 2-Clause License (the "License").
 * See License in the project root for license information.
 */

package com.linkedin.avroutil1.builder.plugins;

import joptsimple.OptionParser;
import joptsimple.OptionSet;

import java.util.Set;


public interface BuilderPlugin {

  /**
   * @return plugin name, mostly for logging. SHOULD BE UNIQUE
   */
  default String name() {
    return getClass().getSimpleName();
  }

  /**
   * plugins must declare what plugin API versions they support. any plugins that
   * do not support the current API version will not be loaded by the utility.
   *
   * this method cannot safely have a default implementation (would make
   * ancient plugins declare latest API supported by default).
   * @return a set of (plugin) API versions supported by a particular plugin.
   */
  Set<Integer> supportedApiVersions();

  /**
   * allow plugins to customize the CLI - things like add new command line options
   * @param parser the CLI parser
   */
  default void customizeCLI(OptionParser parser) {
    //nop
  }

  /**
   * allow plugins to validate any CLI arguments they might care about.
   * exceptions thrown out of this method are expected to be descriptive and helpful
   * @param options parsed CLI arguments
   * @throws IllegalArgumentException if any arguments are not to a plugin's liking
   */
  default void parseAndValidateOptions(OptionSet options) throws IllegalArgumentException {
    //nop
  }

  default void createOperations(BuilderPluginContext context) {
    //nop
  }
}
