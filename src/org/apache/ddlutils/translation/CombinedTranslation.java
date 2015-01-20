/*
 ************************************************************************************
 * Copyright (C) 2001-2015 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.translation;

import java.util.ArrayList;

/**
 * 
 * @author adrian
 */
public class CombinedTranslation implements Translation {

  private ArrayList<Translation> _translations;

  /** Creates a new instance of CombinedTranslation */
  public CombinedTranslation() {
    _translations = new ArrayList<Translation>();
  }

  public final CombinedTranslation append(Translation t) {
    _translations.add(t);
    return this;
  }

  public final String exec(String s) {
    String translatedCode = s;
    String initialComments = "";
    String initialBlanks = "";
    while (translatedCode.charAt(0) == '\n' || translatedCode.charAt(0) == ' ') {
      initialBlanks += translatedCode.substring(0, 1);
      translatedCode = translatedCode.substring(1);
    }
    if (translatedCode.startsWith("/*")) {
      int ind = translatedCode.indexOf("*/");
      initialComments = translatedCode.substring(0, ind);
      translatedCode = translatedCode.substring(ind);
    }

    for (Translation t : _translations) {
      translatedCode = t.exec(translatedCode);
    }
    return initialBlanks + initialComments + translatedCode;
  }

}
