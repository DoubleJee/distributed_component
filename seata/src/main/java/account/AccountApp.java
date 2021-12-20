package account;

import config.SeataFilter;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@SpringBootApplication
@Import(SeataFilter.class)
public class AccountApp {

    public static void main(String[] args) throws IOException {
        String fileName = "application-account.properties";
        Properties properties = new Properties();
        InputStream inputStream = new ClassPathResource(fileName).getInputStream();
        properties.load(inputStream);
        SpringApplication springApplication = new SpringApplication(AccountApp.class);
        springApplication.setDefaultProperties(properties);
        springApplication.run(args);
    }
}
