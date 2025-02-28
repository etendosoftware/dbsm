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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.ddlutils.translation.Translation;

/**
 * Represents a database function or procedure.
 * 
 * @version $Revision$
 */
public class Function implements StructureObject, Cloneable {
  public enum Volatility {
    VOLATILE, STABLE, IMMUTABLE;

    public static Volatility fromPGCode(String code) {
      switch (code) {
        case "v":
          return VOLATILE;
        case "s":
          return STABLE;
        case "i":
          return IMMUTABLE;
        default:
          return VOLATILE;
      }
    }

    /**
     * Return Volatility name except for VOLATILE which an empty String is returned to prevent
     * attribute to be written in xml for default case.
     */
    @Override
    public String toString() {
      if (this == VOLATILE) {
        return "";
      }
      return super.toString();
    }
  }

  /** The name of the function, may be <code>null</code>. */
  private String _name;
  /** The parameters of the function. */
  private List<Parameter> _parameters;
  /** The JDBC type code, one of the constants in {@link java.sql.Types}. */
  private int _typeCode;
  /** The body of the function. */
  private String _body;
  private String _originalBody;

  private boolean recreationRequired = false;

  private Volatility volatility = Volatility.VOLATILE;

  /** Creates a new instance of Function */
  public Function() {
    this(null);
  }

  public Function(String name) {
    _name = name;
    _parameters = new ArrayList<>();
    _typeCode = Types.NULL;
    _body = null;
  }

  /**
   * Returns the name of this function.
   * 
   * @return The name
   */
  @Override
  public String getName() {
    return _name;
  }

  /**
   * Sets the name of this function.
   * 
   * @param name
   *          The name
   */
  public void setName(String name) {
    _name = name;
  }

  /**
   * Returns the notation of this function.
   * 
   * @return The notation
   */
  public String getNotation() {
    return _name + "("
        + _parameters.stream().map(Parameter::getType).collect(Collectors.joining(", ")) + ")";
  }

  /**
   * Returns the code (one of the constants in {@link java.sql.Types}) of the JDBC type of the
   * column.
   * 
   * @return The type code
   */
  public int getTypeCode() {
    return _typeCode;
  }

  /**
   * Sets the code (one of the constants in {@link java.sql.Types}) of the JDBC type of the column.
   * 
   * @param typeCode
   *          The type code
   */
  public void setTypeCode(int typeCode) {
    if (TypeMap.getJdbcTypeName(typeCode) == null) {
      throw new ModelException("Unknown JDBC type code " + typeCode);
    }
    _typeCode = typeCode;
  }

  /**
   * Returns the JDBC type of the parameter.
   * 
   * @return The type
   */
  public String getType() {
    return TypeMap.getJdbcTypeName(_typeCode);
  }

  /**
   * Sets the JDBC type of the parameter.
   * 
   * @param type
   *          The type
   */
  public void setType(String type) {
    Integer typeCode = TypeMap.getJdbcTypeCode(type);

    if (typeCode == null) {
      _typeCode = Types.NULL;
    } else {
      _typeCode = typeCode.intValue();
    }
  }

  /**
   * Returns the number of parameters in this function.
   * 
   * @return The number of parameters
   */
  public int getParameterCount() {
    return _parameters.size();
  }

  /**
   * Returns the parameter at the specified position.
   * 
   * @param idx
   *          The parameter index
   * @return The parameter at this position
   */
  public Parameter getParameter(int idx) {
    return _parameters.get(idx);
  }

  /**
   * Returns the parameters in this function.
   * 
   * @return The parameters
   */
  public Parameter[] getParameters() {
    return _parameters.toArray(new Parameter[_parameters.size()]);
  }

  /**
   * Adds the given parameter.
   * 
   * @param parameter
   *          The parameter
   */
  public void addParameter(Parameter parameter) {
    if (parameter != null) {
      _parameters.add(parameter);
    }
  }

  /**
   * Adds the given parameter at the specified position.
   * 
   * @param idx
   *          The index where to add the parameter
   * @param parameter
   *          The parameter
   */
  public void addParameter(int idx, Parameter parameter) {
    if (parameter != null) {
      _parameters.add(idx, parameter);
    }
  }

  /**
   * Adds the parameter after the given previous parameter.
   * 
   * @param previousParameter
   *          The parameter to add the new parameter after; use <code>null</code> for adding at the
   *          begin
   * @param parameter
   *          The parameter
   */
  public void addParameter(Parameter previousParameter, Parameter parameter) {
    if (parameter != null) {
      if (previousParameter == null) {
        _parameters.add(0, parameter);
      } else {
        _parameters.add(_parameters.indexOf(previousParameter), parameter);
      }
    }
  }

  /**
   * Adds the given parameters.
   * 
   * @param parameters
   *          The parameters
   */
  public void addParameters(Collection<Parameter> parameters) {
    _parameters.addAll(parameters);
  }

  /**
   * Removes the given parameter.
   * 
   * @param parameter
   *          The parameter to remove
   */
  public void removeParameter(Parameter parameter) {
    if (parameter != null) {
      _parameters.remove(parameter);
    }
  }

  /**
   * Removes the indicated parameter.
   * 
   * @param idx
   *          The index of the parameter to remove
   */
  public void removeParameter(int idx) {
    _parameters.remove(idx);
  }

  /**
   * Finds the parameter with the specified name, using case insensitive matching. Note that this
   * method is not called getParameter(String) to avoid introspection problems.
   * 
   * @param name
   *          The name of the parameter
   * @return The parameter or <code>null</code> if there is no such parameter
   */
  public Parameter findParameter(String name) {
    return findParameter(name, false);
  }

  /**
   * Finds the parameter with the specified name, using case insensitive matching. Note that this
   * method is not called getParameter(String) to avoid introspection problems.
   * 
   * @param name
   *          The name of the parameter
   * @param caseSensitive
   *          Whether case matters for the names
   * @return The parameter or <code>null</code> if there is no such parameter
   */
  public Parameter findParameter(String name, boolean caseSensitive) {
    for (Parameter parameter : _parameters) {
      if (caseSensitive) {
        if (parameter.getName().equals(name)) {
          return parameter;
        }
      } else {
        if (parameter.getName().equalsIgnoreCase(name)) {
          return parameter;
        }
      }
    }
    return null;
  }

  /**
   * Returns the body of this function.
   * 
   * @return The body
   */
  public String getBody() {
    return _body;
  }

  public String getOriginalBody() {
    return _originalBody;
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

  public void setOriginalBody(String body) {
    _originalBody = body;
  }

  public boolean hasOutputParameters() {
    for (Parameter parameter : _parameters) {
      if (parameter.getModeCode() == Parameter.MODE_OUT) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  @SuppressWarnings("unchecked")
  @Override
  public Object clone() throws CloneNotSupportedException {
    Function result = (Function) super.clone();

    result._name = _name;
    result._body = _body;
    result._typeCode = _typeCode;
    result._parameters = (List<Parameter>) ((ArrayList<Parameter>) _parameters).clone();

    return result;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof Function)) {
      return false;
    }
    Function other = (Function) obj;
    boolean sameIgnoreCase = equalsIgnoreCase(other);
    if (!sameIgnoreCase) {
      return false;
    }

    return new EqualsBuilder().append(_name, other._name).isEquals();
  }

  /**
   * Compares this function to the given one while ignoring the case of identifiers.
   * 
   * @param otherFunction
   *          The other function
   * @return <code>true</code> if this function is equal (ignoring case) to the given one
   */
  public boolean equalsIgnoreCase(Function otherFunction) {
    try {
      // @formatter:off
      return UtilsCompare.equalsIgnoreCase(_name, otherFunction._name)
          && new EqualsBuilder()
              .append(_parameters, otherFunction._parameters)
              .append(_body, otherFunction._body)
              .append(_typeCode, otherFunction._typeCode)
              .append(volatility, otherFunction.volatility)
              .isEquals();
      // @formatter:on
    } catch (Exception e) {
      throw new RuntimeException("Error while comparing functions " + this._name + " and "
          + otherFunction._name
          + ". Check that the parameters of both functions are correctly defined in the database and in the XML files (check that the parameters are named, and have a correct datatype",
          e);
    }
  }

  /**
   * Compares this function notation to the notation of given one.
   * 
   * @param otherFunction
   *          The other function
   * @return <code>true</code> if this function has the same notation to the given one
   */
  public boolean equalsNotation(Function otherFunction) {

    if (_parameters.size() == otherFunction._parameters.size()) {

      EqualsBuilder e = new EqualsBuilder();
      e.append(_name, otherFunction._name);

      for (int i = 0; i < _parameters.size(); i++) {
        Parameter p1 = _parameters.get(i);
        Parameter p2 = otherFunction._parameters.get(i);
        e.append(p1.getTypeCode(), p2.getTypeCode());
      }

      return e.isEquals();
    } else {
      return false;
    }
  }

  /**
   * Translates the function body
   * 
   * @param translation
   */
  public void setTranslation(Translation translation) {
    for (Parameter p : _parameters) {
      p.setTranslation(translation);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public int hashCode() {
    return new HashCodeBuilder(17, 37).append(_name)
        .append(_parameters)
        .append(_typeCode)
        .append(_body)
        .append(volatility)
        .toHashCode();
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String toString() {
    return "Function " + _name + "[notation=" + getNotation() + "; ] " + volatility;
  }

  /** Sets whether recreation is required even no other changes are detected */
  public void setRecreationRequired(boolean recreationRequired) {
    this.recreationRequired = recreationRequired;
  }

  /** Is recreation required even no other changes are detected */
  public boolean isRecreationRequired() {
    return recreationRequired;
  }

  public Volatility getVolatility() {
    return volatility;
  }

  public void setVolatility(Volatility volatility) {
    this.volatility = volatility;
  }
}
