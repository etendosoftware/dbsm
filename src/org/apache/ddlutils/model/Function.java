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
import java.util.Iterator;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.ddlutils.translation.NullTranslation;
import org.apache.ddlutils.translation.Translation;

/**
 * Represents a database function or procedure.
 * 
 * @version $Revision$
 */
public class Function implements StructureObject, Cloneable {

  /** The name of the function, may be <code>null</code>. */
  private String _name;
  /** The parameters of the function. */
  private ArrayList _parameters;
  /** The JDBC type code, one of the constants in {@link java.sql.Types}. */
  private int _typeCode;
  /** The body of the function. */
  private String _body;
  /** The translation object of the function */
  private Translation _translation = new NullTranslation();

  /** Creates a new instance of Function */
  public Function() {
    this(null);
  }

  public Function(String name) {
    _name = name;
    _parameters = new ArrayList();
    _typeCode = Types.NULL;
    _body = null;
  }

  /**
   * Returns the name of this function.
   * 
   * @return The name
   */
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
    StringBuffer s = new StringBuffer();
    s.append(_name);
    s.append("(");

    for (int i = 0; i < _parameters.size(); i++) {
      Parameter p = (Parameter) _parameters.get(i);
      if (i > 0) {
        s.append(", ");
      }
      s.append(p.getType());
    }

    s.append(")");
    return s.toString();
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
    String _type = TypeMap.getJdbcTypeName(typeCode);
    if (_type == null) {
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
    return (Parameter) _parameters.get(idx);
  }

  /**
   * Returns the parameters in this function.
   * 
   * @return The parameters
   */
  public Parameter[] getParameters() {
    return (Parameter[]) _parameters.toArray(new Parameter[_parameters.size()]);
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
  public void addParameters(Collection parameters) {
    for (Iterator it = parameters.iterator(); it.hasNext();) {
      addParameter((Parameter) it.next());
    }
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
    for (Iterator it = _parameters.iterator(); it.hasNext();) {
      Parameter parameter = (Parameter) it.next();

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
   * Determines the index of the given parameter.
   * 
   * @param parameter
   *          The parameter
   * @return The index or <code>-1</code> if it is no parameter of this table
   */
  public int getParameterIndex(Parameter parameter) {
    int idx = 0;

    for (Iterator it = _parameters.iterator(); it.hasNext(); idx++) {
      if (parameter == it.next()) {
        return idx;
      }
    }
    return -1;
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

  public boolean hasOutputParameters() {

    for (Iterator it = _parameters.iterator(); it.hasNext();) {
      Parameter parameter = (Parameter) it.next();
      if (parameter.getModeCode() == parameter.MODE_OUT) {
        return true;
      }
    }
    return false;
  }

  /**
   * {@inheritDoc}
   */
  public Object clone() throws CloneNotSupportedException {
    Function result = (Function) super.clone();

    result._name = _name;
    result._body = _body;
    result._typeCode = _typeCode;
    result._parameters = (ArrayList) _parameters.clone();

    return result;
  }

  /**
   * {@inheritDoc}
   */
  public boolean equals(Object obj) {
    if (obj instanceof Function) {
      Function other = (Function) obj;

      int typeCode2 = _typeCode;

      int othertypeCode2 = other._typeCode;

      /*
       * System.out.println(other._translation.toString()); //System.out.println
       * (other._translation.exec(other._body.trim()));
       * System.out.println("NOMBRE"+_name.equals(other._name)); System.out
       * .println("PARAMETROS: "+_parameters.equals(other._parameters)); System
       * .out.println("BODY: "+_translation.exec(_body.trim()).equals
       * (other._translation.exec(other._body.trim()))); try{ PrintWriter w1 = new PrintWriter(new
       * BufferedWriter(new FileWriter("/home/openbravo/Desktop/t1"))); PrintWriter w2 = new
       * PrintWriter(new BufferedWriter(new FileWriter("/home/openbravo/Desktop/t2"))); String
       * b1=_translation.exec(_body.trim()); String b2=other._translation.exec(other._body.trim());
       * w1.println(b1); w2.println(b2); w1.close(); w2.close();
       * System.out.println("BODYS********************");
       * System.out.println(_translation.exec(_body.trim()));
       * System.out.println(other._translation.exec(other._body.trim()));
       * System.out.println("BODYS********************"); }catch(Exception e){}
       */

      // Note that this compares case sensitive
      // TODO: For now we ignore catalog and schema (type should be
      // irrelevant anyways)
      return new EqualsBuilder().append(_name, other._name).append(_parameters, other._parameters)
          .append(_body, other._body).append(typeCode2, othertypeCode2).isEquals();
    } else {
      return false;
    }
  }

  /**
   * Compares this function to the given one while ignoring the case of identifiers.
   * 
   * @param otherFunction
   *          The other function
   * @return <code>true</code> if this function is equal (ignoring case) to the given one
   */
  public boolean equalsIgnoreCase(Function otherFunction) {

    int typeCode2 = _typeCode;

    int othertypeCode2 = otherFunction._typeCode;

    return UtilsCompare.equalsIgnoreCase(_name, otherFunction._name)
        && new EqualsBuilder().append(_parameters, otherFunction._parameters).append(_body,
            otherFunction._body).append(typeCode2, othertypeCode2).isEquals();
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
        Parameter p1 = (Parameter) _parameters.get(i);
        Parameter p2 = (Parameter) otherFunction._parameters.get(i);
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
    _translation = translation;
    for (int i = 0; i < _parameters.size(); i++) {
      Parameter p = (Parameter) _parameters.get(i);
      p.setTranslation(translation);
    }
  }

  /**
   * {@inheritDoc}
   */
  public int hashCode() {
    // TODO: For now we ignore catalog and schema (type should be irrelevant
    // anyways)
    return new HashCodeBuilder(17, 37).append(_name).append(_parameters).append(_typeCode).append(
        _body).toHashCode();
  }

  /**
   * {@inheritDoc}
   */
  public String toString() {
    StringBuffer result = new StringBuffer();

    result.append("Function [notation=");
    result.append(getNotation());
    result.append("; ");
    result.append("]");

    return result.toString();
  }
}
