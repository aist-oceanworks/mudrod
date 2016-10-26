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
package esiptestbed.mudrod.services;

import java.util.Properties;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;

import esiptestbed.mudrod.driver.ESDriver;
import esiptestbed.mudrod.driver.SparkDriver;
import esiptestbed.mudrod.main.MudrodEngine;
import esiptestbed.mudrod.ssearch.Ranker;
import esiptestbed.mudrod.ssearch.Searcher;

/**
 * Application Lifecycle Listener implementation class MudrodContextListener
 *
 */
@WebListener
public class MudrodContextListener implements ServletContextListener {
  MudrodEngine me = null;
  /**
   * Default constructor.
   */
  public MudrodContextListener() {
    //default constructor
  }

  /**
   * @see ServletContextListener#contextDestroyed(ServletContextEvent)
   */
  @Override
  public void contextDestroyed(ServletContextEvent arg0) {
    me.end();
  }

  /**
   * @see ServletContextListener#contextInitialized(ServletContextEvent)
   */
  @Override
  public void contextInitialized(ServletContextEvent arg0) {
    me = new MudrodEngine();
    Properties props = me.loadConfig();
    me.setESDriver(new ESDriver(props));
    me.setSparkDriver(new SparkDriver());

    ServletContext ctx = arg0.getServletContext();
    Searcher searcher = new Searcher(me.getConfig(), me.getESDriver(), null);
    Ranker ranker = new Ranker(me.getConfig(), me.getESDriver(), me.getSparkDriver(), "SparkSVM");
    ctx.setAttribute("MudrodInstance", me);
    ctx.setAttribute("MudrodSearcher", searcher);
    ctx.setAttribute("MudrodRanker", ranker);
  }

}
