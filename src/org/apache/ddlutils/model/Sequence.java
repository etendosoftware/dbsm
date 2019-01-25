package org.apache.ddlutils.model;

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

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;

/**
 * Represents a database sequence.
 * 
 * @version $Revision$
 */
public class Sequence implements StructureObject, Cloneable {

  /** The name of the sequence, may be <code>null</code>. */
  private String _name;
  /** The start attribute of the sequence. */
  private int _start;
  /** The increment attribute of the sequence. */
  private int _increment;

  /** Creates a new instance of Sequence */
  public Sequence() {
    this(null);
  }

  /** Creates a new instance of Sequence */
  public Sequence(String name) {
    _name = name;
    setStart(1);
    setIncrement(1);
  }

  /**
   * Returns the name of this sequence.
   * 
   * @return The name
   */
  @Override
  public String getName() {
    return _name;
  }

  /**
   * Sets the name of this sequence.
   * 
   * @param name
   *          The name
   */
  public void setName(String name) {
    _name = name;
  }

  /**
   * Returns the start attribute of this sequence.
   * 
   * @return The start attribute
   */
  public int getStart() {
    return _start;
  }

  /**
   * Sets the start attribute of this sequence.
   * 
   * @param start
   *          The start attribute
   */
  public void setStart(int start) {
    _start = start;
  }

  /**
   * Returns the increment attribute of this sequence.
   * 
   * @return The increment attribute
   */
  public int getIncrement() {
    return _increment;
  }

  /**
   * Sets the increment attribute of this sequence.
   * 
   * @param increment
   *          The increment attribute
   */
  public void setIncrement(int increment) {
    _increment = increment;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public Object clone() throws CloneNotSupportedException {

    Sequence result = (Sequence) super.clone();

    result._name = _name;
    result._start = _start;
    result._increment = _increment;

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj) {

    if (obj instanceof Sequence) {
      Sequence otherSequence = (Sequence) obj;

      // Note that this compares case sensitive
      // Note also that we can simply compare the references regardless of
      // their order
      // (which is irrelevant for ccs) because they are contained in a set
      return new EqualsBuilder().append(_name, otherSequence._name)
          .append(_start, otherSequence._start).append(_increment, otherSequence._increment)
          .isEquals();
    } else {
      return false;
    }
  }

  /**
   * Compares this sequence to the given one while ignoring the case of identifiers.
   * 
   * @param otherSequence
   *          The other sequence
   * @return <code>true</code> if this sequence is equal (ignoring case) to the given one
   */
  public boolean equalsIgnoreCase(Sequence otherSequence) {
    return UtilsCompare.equalsIgnoreCase(_name, otherSequence._name)
        && new EqualsBuilder().append(_start, otherSequence._start)
            .append(_increment, otherSequence._increment).isEquals();

  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(_name).append(_start).append(_increment).toHashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    StringBuffer result = new StringBuffer();

    result.append("Sequence [");
    if ((getName() != null) && (getName().length() > 0)) {
      result.append("name=");
      result.append(getName());
      result.append("; ");
      result.append("start=");
      result.append(getStart());
      result.append("; ");
      result.append("increment=");
      result.append(getIncrement());
      result.append("; ");
    }
    result.append("]");

    return result.toString();
  }
}
