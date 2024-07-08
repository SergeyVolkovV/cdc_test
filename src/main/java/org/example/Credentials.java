package org.example;


import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/*TODO make credentials from HVAULT*/
public class Credentials {

    Properties prop = new Properties();

    public Credentials()
    {

    }

    public  void setCredentialFromFile(String path) throws IOException {
        InputStream fis = new FileInputStream(path);
        prop.load(fis);
        System.out.println(prop);

    }




}
