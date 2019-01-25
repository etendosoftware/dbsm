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
import org.apache.ddlutils.model.Sequence;

/**
 * Represents the removal of a table from a model.
 * 
 * @version $Revision: $
 */
public class RemoveSequenceChange implements ModelChange {

  /** The sequence. */
  private Sequence _sequence;

  /**
   * Creates a remove change object.
   * 
   * @param sequence
   *          The sequence
   */
  public RemoveSequenceChange(Sequence sequence) {
    _sequence = sequence;
  }

  /**
   * Returns the sequence.
   * 
   * @return The sequence
   */
  public Sequence getSequence() {
    return _sequence;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public void apply(Database database, boolean caseSensitive) {
    Sequence sequence = database.findSequence(_sequence.getName(), caseSensitive);

    database.removeSequence(sequence);
  }

  @Override
  public String toString() {
    return "RemoveSequenceChange. Name: " + _sequence.getName();
  }
}
