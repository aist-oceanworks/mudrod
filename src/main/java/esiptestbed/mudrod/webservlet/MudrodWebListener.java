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
    public void contextDestroyed(ServletContextEvent arg0)  { 
         // TODO Auto-generated method stub
    }

	/**
     * @see ServletContextListener#contextInitialized(ServletContextEvent)
     */
    public void contextInitialized(ServletContextEvent arg0)  { 
         // TODO Auto-generated method stub
    	MudrodEngine mudrod = new MudrodEngine();
    	ServletContext ctx = arg0.getServletContext();
    	ctx.setAttribute("MudrodInstance", mudrod);
    }
	
}
