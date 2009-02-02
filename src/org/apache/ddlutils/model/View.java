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
 * Represents a database view.
 * 
 * @version $Revision$
 */
public class View implements StructureObject, Cloneable {

  /** The name of the view, may be <code>null</code>. */
  private String _name;
  /** The statement of the view. */
  private String _statement;

  /** Creates a new instance of View */
  public View() {
    this(null);
  }

  public View(String name) {
    _name = name;
    _statement = null;
  }

  /**
   * Returns the name of this view.
   * 
   * @return The name
   */
  public String getName() {
    return _name;
  }

  /**
   * Sets the name of this view.
   * 
   * @param name
   *          The name
   */
  public void setName(String name) {
    _name = name;
  }

  /**
   * Returns the statement of this view.
   * 
   * @return The statement
   */
  public String getStatement() {
    return _statement;
  }

  /**
   * Sets the statement of this view.
   * 
   * @param statement
   *          The statement
   */
  public void setStatement(String statement) {
    _statement = statement;
  }

  /**
   * {@inheritDoc}
   */
  public Object clone() throws CloneNotSupportedException {

    View result = (View) super.clone();

    result._name = _name;
    result._statement = _statement;

    return result;
  }

  /**
   * {@inheritDoc}
   */
  public boolean equals(Object obj) {

    if (obj instanceof View) {
      View otherView = (View) obj;

      // Note that this compares case sensitive
      // Note also that we can simply compare the references regardless of
      // their order
      // (which is irrelevant for ccs) because they are contained in a set
      return new EqualsBuilder().append(_name, otherView._name).append(_statement.trim(),
          otherView._statement.trim()).isEquals();
    } else {
      return false;
    }
  }

  /**
   * Compares this view to the given one while ignoring the case of identifiers.
   * 
   * @param otherView
   *          The other view
   * @return <code>true</code> if this view is equal (ignoring case) to the given one
   */
  public boolean equalsIgnoreCase(View otherView) {
    return UtilsCompare.equalsIgnoreCase(_name, otherView._name)
        && new EqualsBuilder().append(_statement.trim(), otherView._statement.trim()).isEquals();

  }

  /**
   * {@inheritDoc}
   */
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(_name).append(_statement).toHashCode();
  }

  /**
   * {@inheritDoc}
   */
  public String toString() {
    StringBuffer result = new StringBuffer();

    result.append("View [");
    if ((getName() != null) && (getName().length() > 0)) {
      result.append("name=");
      result.append(getName());
      result.append("; ");
    }
    result.append("]");

    return result.toString();
  }

  public String toVerboseString() {
    StringBuffer result = new StringBuffer();

    result.append("View [");
    if ((getName() != null) && (getName().length() > 0)) {
      result.append("name=");
      result.append(getName());
      result.append("; ");
    }
    result.append("statement=");
    result.append(getStatement());
    result.append("]");

    return result.toString();
  }
}
