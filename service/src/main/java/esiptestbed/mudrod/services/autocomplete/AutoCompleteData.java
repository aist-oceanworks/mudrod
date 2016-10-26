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
package esiptestbed.mudrod.services.autocomplete;

public class AutoCompleteData {
  private final String label;
  private final String value;

  public AutoCompleteData(String label, String value) {
    super();
    this.label = label;
    this.value = value;
  }

  public final String getLabel() {
    return this.label;
  }

  public final String getValue() {
    return this.value;
  }

}
