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

package org.apache.ddlutils.io;

import org.apache.commons.betwixt.expression.Context;
import org.apache.commons.betwixt.strategy.DefaultObjectStringConverter;
import org.apache.ddlutils.model.Function;

/** Defines specific conversion types used when reading xml files. */
@SuppressWarnings("serial")
public class DBSMObjectStringConverter extends DefaultObjectStringConverter {
  @SuppressWarnings({ "rawtypes", "unchecked" })
  @Override
  public Object stringToObject(String value, Class type, Context context) {
    if (type.isAssignableFrom(Function.Volatility.class)) {
      return Function.Volatility.valueOf(value);
    }
    return super.stringToObject(value, type, context);
  }
}
