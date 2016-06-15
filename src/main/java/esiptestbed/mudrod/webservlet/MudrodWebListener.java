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
package esiptestbed.mudrod.webservlet;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import esiptestbed.mudrod.main.MudrodEngine;

/**
 * Application Lifecycle Listener implementation class MudrodWebListener
 *
 */
@WebListener
public class MudrodWebListener implements ServletContextListener {

  /**
   * Default constructor.
   */
  public MudrodWebListener() {
    // TODO Auto-generated constructor stub
  }

  /**
   * @see ServletContextListener#contextDestroyed(ServletContextEvent)
   */
  public void contextDestroyed(ServletContextEvent arg0) {
    // TODO Auto-generated method stub
  }

  /**
   * @see ServletContextListener#contextInitialized(ServletContextEvent)
   */
  public void contextInitialized(ServletContextEvent arg0) {
    // TODO Auto-generated method stub
    MudrodEngine mudrod = new MudrodEngine();
    ServletContext ctx = arg0.getServletContext();
    ctx.setAttribute("MudrodInstance", mudrod);
  }

}
