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
package gov.nasa.jpl.mudrod.utils;

import org.apache.spark.mllib.linalg.distributed.RowMatrix;

import java.util.List;

/**
 * ClassName: LabeledRowMatrix
 * Function: LabeledRowMatrix strut.
 */
public class LabeledRowMatrix {

  // words: matrix row titles
  public List<String> rowkeys;
  // docs: matrix column titles
  public List<String> colkeys;
  // wordDocMatrix: a matrix in which each row is corresponding to a term and
  // each column is a doc.
  public RowMatrix rowMatrix;

  public LabeledRowMatrix() {
    // TODO Auto-generated constructor stub
  }

}