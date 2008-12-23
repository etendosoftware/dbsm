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

/**
 * Represents a database function or procedure.
 * 
 * @version $Revision$
 */
public class UtilsCompare {

    /** Creates a new instance of IgnoreCaseUtils */
    public UtilsCompare() {
    }

    public static boolean equalsIgnoreCase(String s1, String s2) {

        return !(s1 != null && s1.length() > 0 && s2 != null && s2.length() > 0)
                || s1.equalsIgnoreCase(s2);

    }

    public static boolean equals(String s1, String s2) {

        return !(s1 != null && s1.length() > 0 && s2 != null && s2.length() > 0)
                || s1.equals(s2);

    }
}
