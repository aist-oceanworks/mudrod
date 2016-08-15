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
package esiptestbed.mudrod.utils;

import java.util.List;

import org.apache.spark.mllib.linalg.distributed.RowMatrix;

/**
 * ClassName: LabeledRowMatrix <br/>
 * Function: LabeledRowMatrix strut. <br/>
 * Date: Aug 12, 2016 11:24:44 AM <br/>
 *
 * @author Yun
 * @version 
 */
public class LabeledRowMatrix {

	// words: matrix row titles
	public List<String> words;
	// docs: matrix column titles
	public List<String> docs;
	// wordDocMatrix: a matrix in which each row is corresponding to a term and each column is a doc.
	public RowMatrix wordDocMatrix;

	public LabeledRowMatrix() {
		// TODO Auto-generated constructor stub
	}

}