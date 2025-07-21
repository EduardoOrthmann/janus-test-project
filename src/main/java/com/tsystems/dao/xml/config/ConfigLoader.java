package com.tsystems.dao.xml.config;

import java.io.InputStream;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ConfigLoader {
    private static final Logger LOGGER = Logger.getLogger(ConfigLoader.class.getName());
    public ConfigLoader() {}
    public ConfigLoader(ClassLoader loader) {}

    public void load(String path, String fileName) throws org.xml.sax.SAXException {
        String resourcePath = path + "/" + fileName;
        InputStream is = getClass().getClassLoader().getResourceAsStream(resourcePath);
        if (is == null) {
            LOGGER.log(Level.SEVERE, "ConfigLoader could not find resource: {0}", resourcePath);
        } else {
            LOGGER.log(Level.INFO, "ConfigLoader successfully found resource: {0}", resourcePath);
        }
    }
}