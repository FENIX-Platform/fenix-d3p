package org.fao.fenix.d3p;

import org.fao.fenix.commons.utils.Properties;
import org.fao.fenix.d3p.process.ProcessFactory;
import org.fao.fenix.d3p.process.impl.group.RulesFactory;
import org.fao.fenix.d3s.server.init.InitListener;
import org.fao.fenix.d3s.server.init.MainController;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.io.File;
import java.util.Collections;

@WebListener
public class Startup implements ServletContextListener, InitListener {
    private
    @Inject
    ProcessFactory processFactory;
    private
    @Inject
    RulesFactory rulesFactory;
    private
    @Inject
    MainController d3sController;

    @Override
    public void init(Properties d3sInitParameters) throws Exception {
        try {
            //Init modules
            String timeoutParameter = getInitParameter("process.timeout");
            processFactory.init(
                    getInitParameter("process.impl.package"),
                    timeoutParameter != null && timeoutParameter.trim().length() > 0 ? Integer.valueOf(timeoutParameter) : null
            );

            rulesFactory.init(getInitParameter("rules.impl.package"));
        } catch (Exception e) {
            System.err.println("D3P initialization error: " + e.getMessage());
        }
    }


    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        try {
            //Append Web context parameters
            Properties initParameters = getInitParameters();
            ServletContext context = servletContextEvent.getServletContext();
            for (Object key : Collections.list(context.getInitParameterNames()))
                initParameters.setProperty((String) key, context.getInitParameter((String) key));

            d3sController.registerListener(this);
        } catch (Exception e) {
            System.err.println("D3P initialization error: " + e.getMessage());
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }

    //Utils
    private static Properties initParameters;

    public static Properties getInitParameters() throws Exception {
        if (initParameters == null)
            initParameters = Properties.getInstance(
                    "/org/fao/fenix/config/d3p.properties",
                    "file:config/d3p.properties"
            );
        return initParameters;
    }

    public String getInitParameter(String key) throws Exception {
        return getInitParameters().getProperty(key);
    }

}
