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
import org.apache.ddlutils.translation.NullTranslation;
import org.apache.ddlutils.translation.Translation;

/**
 * Represents a database function or procedure.
 * 
 * @version $Revision$
 */
public class Trigger implements StructureObject, Cloneable {

  public static final int FIRES_BEFORE = 0;
  public static final int FIRES_AFTER = 1;

  public static final int FOR_EACH_ROW = 0;
  public static final int FOR_EACH_STATEMENT = 1;

  /** The name of the function, may be <code>null</code>. */
  private String _name;
  /** The table associated. */
  private String _table;
  /** The code of the fires option. */
  private int _firesCode;
  /** the insert event. */
  private boolean _insert;
  /** the update event. */
  private boolean _update;
  /** the delete event. */
  private boolean _delete;
  /** the code of the for each option. */
  private int _foreachCode;
  /** The body of the trigger. */
  private String _body;
  /** The translation object used to translate the trigger body */
  private Translation _translation = new NullTranslation();

  /** Creates a new instance of Trigger */
  public Trigger() {
    _name = null;
    _table = null;
    _firesCode = FIRES_BEFORE;
    _insert = false;
    _update = false;
    _delete = false;
    _foreachCode = FOR_EACH_ROW;
    _body = null;
  }

  /**
   * Returns the name of this trigger.
   * 
   * @return The name
   */
  public String getName() {
    return _name;
  }

  /**
   * Sets the name of this trigger.
   * 
   * @param name
   *          The name
   */
  public void setName(String name) {
    _name = name;
  }

  /**
   * Returns the table of this trigger.
   * 
   * @return The table
   */
  public String getTable() {
    return _table;
  }

  /**
   * Sets the table of this trigger.
   * 
   * @param table
   *          The table
   */
  public void setTable(String table) {
    _table = table;
  }

  /**
   * Returns the code of the fires option of this trigger.
   * 
   * @return The code of the fires option
   */
  public int getFiresCode() {
    return _firesCode;
  }

  /**
   * Sets the code of the fires option of this trigger.
   * 
   * @param firesCode
   *          The code of the fires option
   */
  public void setFiresCode(int firesCode) {
    _firesCode = firesCode;
  }

  /**
   * Returns the fires option of this trigger.
   * 
   * @return The fires option
   */
  public String getFires() {
    switch (_firesCode) {
    case FIRES_AFTER:
      return "after";
    case FIRES_BEFORE:
    default:
      return "before";
    }
  }

  /**
   * Sets the fires option of this trigger.
   * 
   * @param fires
   *          The fires option
   */
  public void setFires(String fires) {
    if ("after".equals(fires)) {
      _firesCode = FIRES_AFTER;
    } else if ("before".equals(fires)) {
      _firesCode = FIRES_BEFORE;
    } else {
      _firesCode = FIRES_BEFORE;
    }
  }

  /**
   * Returns the insert event of this trigger.
   * 
   * @return The insert event
   */
  public boolean isInsert() {
    return _insert;
  }

  /**
   * Sets the insert event of this trigger.
   * 
   * @param insert
   *          The insert event
   */
  public void setInsert(boolean insert) {
    _insert = insert;
  }

  /**
   * Returns the update event of this trigger.
   * 
   * @return The update event
   */
  public boolean isUpdate() {
    return _update;
  }

  /**
   * Sets the update event of this trigger.
   * 
   * @param update
   *          The update event
   */
  public void setUpdate(boolean update) {
    _update = update;
  }

  /**
   * Returns the update event of this trigger.
   * 
   * @return The update event
   */
  public boolean isDelete() {
    return _delete;
  }

  /**
   * Sets the delete event of this trigger.
   * 
   * @param delete
   *          The delete event
   */
  public void setDelete(boolean delete) {
    _delete = delete;
  }

  /**
   * Returns the code of the for each option of this trigger.
   * 
   * @return The code of the for each option
   */
  public int getForeachCode() {
    return _foreachCode;
  }

  /**
   * Sets the code of the for each option of this trigger.
   * 
   * @param foreachCode
   *          The code of the for each option
   */
  public void setForeachCode(int foreachCode) {
    _foreachCode = foreachCode;
  }

  /**
   * Returns the for each option of this trigger.
   * 
   * @return The for each option
   */
  public String getForeach() {
    switch (_foreachCode) {
    case FOR_EACH_STATEMENT:
      return "statement";
    case FOR_EACH_ROW:
    default:
      return "row";
    }
  }

  /**
   * Sets the fires option of this trigger.
   * 
   * @param fires
   *          The fires option
   */
  public void setForeach(String foreach) {
    if ("statement".equals(foreach)) {
      _foreachCode = FOR_EACH_STATEMENT;
    } else if ("row".equals(foreach)) {
      _foreachCode = FOR_EACH_ROW;
    } else {
      _foreachCode = FOR_EACH_ROW;
    }
  }

  /**
   * Returns the body of this function.
   * 
   * @return The body
   */
  public String getBody() {
    return _body;
  }

  /**
   * Sets the body of this function.
   * 
   * @param body
   *          The body
   */
  public void setBody(String body) {
    _body = body;
  }

  /**
   * {@inheritDoc}
   */
  public Object clone() throws CloneNotSupportedException {
    Trigger result = (Trigger) super.clone();

    result._name = _name;
    result._table = _table;
    result._firesCode = _firesCode;
    result._insert = _insert;
    result._update = _update;
    result._delete = _delete;
    result._foreachCode = _foreachCode;
    result._body = _body;

    return result;
  }

  /**
   * {@inheritDoc}
   */
  public boolean equals(Object obj) {
    if (obj instanceof Trigger) {
      Trigger other = (Trigger) obj;

      // Note that this compares case sensitive
      // TODO: For now we ignore catalog and schema (type should be
      // irrelevant anyways)
      return new EqualsBuilder().append(_name, other._name).append(_table, other._table).append(
          _firesCode, other._firesCode).append(_insert, other._insert).append(_update,
          other._update).append(_delete, other._delete).append(_foreachCode, other._foreachCode)
          .append(_body, other._body).isEquals();
    } else {
      return false;
    }
  }

  /**
   * Compares this trigger to the given one while ignoring the case of identifiers.
   * 
   * @param otherTrigger
   *          The other trigger
   * @return <code>true</code> if this trigger is equal (ignoring case) to the given one
   */
  public boolean equalsIgnoreCase(Trigger otherTrigger) {

    return UtilsCompare.equalsIgnoreCase(_name, otherTrigger._name)
        && new EqualsBuilder().append(_table, otherTrigger._table).append(_firesCode,
            otherTrigger._firesCode).append(_insert, otherTrigger._insert).append(_update,
            otherTrigger._update).append(_delete, otherTrigger._delete).append(_foreachCode,
            otherTrigger._foreachCode).append(_body, otherTrigger._body).isEquals();
  }

  /**
   * Sets the translation object of the trigger.
   * 
   * @param translation
   *          The translation object used to translate the trigger.
   */
  public void setTranslation(Translation translation) {
    _translation = translation;
    // _body=translation.exec(_body);
  }

  /**
   * {@inheritDoc}
   */
  public int hashCode() {
    // TODO: For now we ignore catalog and schema (type should be irrelevant
    // anyways)
    return new HashCodeBuilder(17, 37).append(_name).append(_table).append(_firesCode).append(
        _insert).append(_update).append(_delete).append(_foreachCode).append(_body).toHashCode();
  }

  /**
   * {@inheritDoc}
   */
  public String toString() {
    StringBuffer result = new StringBuffer();

    result.append("Trigger [");
    if ((getName() != null) && (getName().length() > 0)) {
      result.append("name=");
      result.append(getName());
      result.append("; ");
    }
    result.append(" ]");

    return result.toString();
  }

}
