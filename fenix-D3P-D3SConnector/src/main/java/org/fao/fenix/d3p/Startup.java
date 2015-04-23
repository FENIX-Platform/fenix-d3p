package org.fao.fenix.d3p;

import org.fao.fenix.commons.utils.Properties;
import org.fao.fenix.d3p.process.ProcessFactory;

import javax.inject.Inject;
import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import javax.servlet.annotation.WebListener;
import java.io.File;
import java.util.Collections;

@WebListener
public class Startup  implements ServletContextListener {
    private @Inject ProcessFactory processFactory;


    @Override
    public void contextInitialized(ServletContextEvent servletContextEvent) {
        try {
            //Append Web context parameters
            Properties initParameters = getInitParameters();
            ServletContext context = servletContextEvent.getServletContext();
            for (Object key : Collections.list(context.getInitParameterNames()))
                initParameters.setProperty((String)key, context.getInitParameter((String)key));

            //Init modules
            processFactory.init(getInitParameter("process.impl.package"));

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void contextDestroyed(ServletContextEvent servletContextEvent) {

    }

    //Utils
    private static Properties initParameters;
    public static Properties getInitParameters() throws Exception {
        if (initParameters ==null)
            initParameters = Properties.getInstance(
                    "/org/fao/fenix/config/d3p.properties",
                    "file:config/d3p.properties"
            );
        return initParameters;
    }
    public String getInitParameter(String key) throws Exception { return getInitParameters().getProperty(key); }

}
