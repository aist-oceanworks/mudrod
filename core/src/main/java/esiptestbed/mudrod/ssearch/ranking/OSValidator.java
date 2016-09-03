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
package esiptestbed.mudrod.ssearch.ranking;

public class OSValidator {

  private static String OS = System.getProperty("os.name").toLowerCase();

  public static void main(String[] args) {

    System.out.println(OS);

    if (isWindows()) {
      System.out.println("This is Windows");
    } else if (isMac()) {
      System.out.println("This is Mac");
    } else if (isUnix()) {
      System.out.println("This is Unix or Linux");
    } else if (isSolaris()) {
      System.out.println("This is Solaris");
    } else {
      System.out.println("Your OS is not support!!");
    }
  }

  public static boolean isWindows() {

    return (OS.indexOf("win") >= 0);

  }

  public static boolean isMac() {

    return (OS.indexOf("mac") >= 0);

  }

  public static boolean isUnix() {

    return (OS.indexOf("nix") >= 0 || OS.indexOf("nux") >= 0 || OS.indexOf("aix") > 0 );

  }

  public static boolean isSolaris() {

    return (OS.indexOf("sunos") >= 0);

  }
}
