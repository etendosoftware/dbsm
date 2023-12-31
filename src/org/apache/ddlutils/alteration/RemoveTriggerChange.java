package org.apache.ddlutils.alteration;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Trigger;

/**
 * Represents the removal of a table from a model.
 * 
 * @version $Revision: $
 */
public class RemoveTriggerChange implements ModelChange {

  /** The trigger. */
  private Trigger _trigger;
  private String _triggerName;

  /**
   * Creates a remove change object.
   * 
   * @param trigger
   *          The trigger
   */
  public RemoveTriggerChange(Trigger trigger) {
    _trigger = trigger;
  }

  public RemoveTriggerChange() {
  }

  /**
   * Returns the trigger.
   * 
   * @return The trigger
   */
  public Trigger getTrigger() {
    return _trigger;
  }

  /**
   * Returns the trigger name.
   * 
   * @return The trigger name
   */
  public String getTriggerName() {
    if (_triggerName != null) {
      return _triggerName;
    }
    if (_trigger != null) {
      return _trigger.getName();
    }
    return null;
  }

  /**
   * Sets the trigger name.
   * 
   * @param triggerName
   *          The name to be set for the trigger
   */
  public void setTriggerName(String triggerName) {
    _triggerName = triggerName;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void apply(Database database, boolean caseSensitive) {
    Trigger trigger = database.findTrigger(_triggerName != null ? _triggerName : _trigger.getName(),
        caseSensitive);

    database.removeTrigger(trigger);
  }

  public void applyInReverse(Database database, boolean caseSensitive) {
    // Do nothing, as there is no information about the trigger definition
  }

  @Override
  public String toString() {
    return "RemoveTriggerChange. Trigger Name: " + getTriggerName();
  }
}
