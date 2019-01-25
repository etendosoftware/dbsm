/*
 ************************************************************************************
 * Copyright (C) 2001-2006 Openbravo S.L.U.
 * Licensed under the Apache Software License version 2.0
 * You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to  in writing,  software  distributed
 * under the License is distributed  on  an  "AS IS"  BASIS,  WITHOUT  WARRANTIES  OR
 * CONDITIONS OF ANY KIND, either  express  or  implied.  See  the  License  for  the
 * specific language governing permissions and limitations under the License.
 ************************************************************************************
 */

package org.apache.ddlutils.translation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;

/**
 * 
 * @author adrian
 */
public class ByLineTranslation implements Translation {

  private Translation t;

  /** Creates a new instance of ByLineTranslation */
  public ByLineTranslation(Translation t) {
    this.t = t;
  }

  @Override
  public String exec(String s) {

    StringBuffer sb = new StringBuffer();
    BufferedReader br = new BufferedReader(new StringReader(s));

    String inputStr;
    try {
      while ((inputStr = br.readLine()) != null) {
        sb.append(t.exec(inputStr));
        sb.append('\n');
      }
    } catch (IOException e) {
    }

    return sb.toString();
  }

}
