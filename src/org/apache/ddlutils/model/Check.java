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
 * Represents a database constraint check.
 * 
 * @version $Revision$
 */
public class Check implements ConstraintObject, Cloneable {

    /** The name of the constraint check, may be <code>null</code>. */
    private String _name;
    /** The condition of the constraint check. */
    private String _condition;

    /** Creates a new check constraint object that has no name. */
    public Check() {
        this(null);
    }

    /**
     * Creates a new check constraint object.
     * 
     * @param name
     *            The name of the check constraint
     */
    public Check(String name) {
        _name = name;
        _condition = null;
    }

    /**
     * Returns the name of this constraint check.
     * 
     * @return The name
     */
    public String getName() {
        return _name;
    }

    /**
     * Sets the name of this constraint check.
     * 
     * @param name
     *            The name
     */
    public void setName(String name) {
        _name = name;
    }

    /**
     * Returns the condition of this constraint check.
     * 
     * @return The condition
     */
    public String getCondition() {
        return _condition.toUpperCase();
    }

    /**
     * Sets the condition of this constraint check.
     * 
     * @param condition
     *            The condition
     */
    public void setCondition(String condition) {
        if (condition != null) {
            _condition = condition;
        }
    }

    /**
     * Returns the condition of this constraint check.
     * 
     * @return The condition
     */
    public String getConditionattr() {
        return null;
    }

    /**
     * Sets the condition of this constraint check.
     * 
     * @param condition
     *            The condition
     */
    public void setConditionattr(String condition) {
        if (condition != null) {
            _condition = condition;
        }
    }

    /**
     * {@inheritDoc}
     */
    public Object clone() throws CloneNotSupportedException {

        Check result = (Check) super.clone();

        result._name = _name;
        result._condition = _condition;

        return result;
    }

    /**
     * {@inheritDoc}
     */
    public boolean equals(Object obj) {

        if (obj instanceof Check) {
            Check otherCC = (Check) obj;

            // Note that this compares case sensitive
            return new EqualsBuilder().append(_name, otherCC._name).append(
                    _condition.toUpperCase(), otherCC._condition.toUpperCase())
                    .isEquals();
        } else {
            return false;
        }
    }

    /**
     * Compares this check to the given one while ignoring the case of
     * identifiers.
     * 
     * @param otherCheck
     *            The other check
     * @return <code>true</code> if this check is equal (ignoring case) to the
     *         given one
     */
    public boolean equalsIgnoreCase(Check otherCheck) {
        return UtilsCompare.equalsIgnoreCase(_name, otherCheck._name)
                && new EqualsBuilder().append(_condition.toUpperCase(),
                        otherCheck._condition.toUpperCase()).isEquals();
    }

    /**
     * {@inheritDoc}
     */
    public int hashCode() {
        return new HashCodeBuilder(17, 37).append(_name).append(
                _condition.toUpperCase()).toHashCode();
    }

    /**
     * {@inheritDoc}
     */
    public String toString() {
        StringBuffer result = new StringBuffer();

        result.append("Constraint check [");
        if ((getName() != null) && (getName().length() > 0)) {
            result.append("name=");
            result.append(getName());
            result.append("; ");
        }
        result.append("condition=");
        result.append(getCondition());
        result.append("]");

        return result.toString();
    }

    public String toVerboseString() {
        return toString();
    }

}
