/*
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gov.nasa.jpl.mudrod.metadata.structure;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import gov.nasa.jpl.mudrod.driver.ESDriver;

/**
 * ClassName: PODAACMetadata Function: PODAACMetadata setter and getter methods
 */
public abstract class Metadata implements Serializable  {

  private static final long serialVersionUID = 1L;
  // shortname: data set short name
  protected String shortname;
  
  public Metadata() {
    // Default constructor
  }

  /**
   * Creates a new instance of PODAACMetadata.
   *
   * @param shortname data set short name
   * @param longname  data set long name
   * @param topics    data set topics
   * @param terms     data set terms
   * @param variables data set variables
   * @param keywords  data set keywords
   * @param region    list of regions
   */
  public Metadata(String shortname) {
    this.shortname = shortname;
  }

  /**
   * getShortName:get short name of data set
   *
   * @return data set short name
   */
  public String getShortName() {
    return this.shortname;
  }

  /**
   * getAbstract:get abstract of data set
   *
   * @return data set abstract
   */
  public abstract List<String> getAllTermList();
}
