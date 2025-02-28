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
import org.apache.ddlutils.model.MaterializedView;

/**
 * Represents the removal of a materialized view from a model.
 * 
 * @version $Revision: $
 */
public class RemoveMaterializedViewChange implements ModelChange {

  /** The materialized view. */
  private MaterializedView materializedView;

  /**
   * Creates a remove change object.
   * 
   * @param materializedView
   *          The materialized view
   */
  public RemoveMaterializedViewChange(MaterializedView materializedView) {
    this.materializedView = materializedView;
  }

  /**
   * Returns the materialized view.
   * 
   * @return The materialized view
   */
  public MaterializedView getMaterializedView() {
    return materializedView;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void apply(Database database, boolean caseSensitive) {
    // nothing, it is referenced in SqlBuilder
  }

  @Override
  public String toString() {
    return "RemoveMaterializedViewChange. Name: " + materializedView.getName();
  }
}
