package order;

import config.SeataFilter;
import config.SeataRestTemplateAutoConfiguration;
import config.SeataRestTemplateInterceptor;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ClassPathResource;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

@SpringBootApplication
@Import({SeataRestTemplateInterceptor.class, SeataRestTemplateAutoConfiguration.class, SeataFilter.class})
public class OrderApp {

    public static void main(String[] args) throws IOException {
        String fileName = "application-order.properties";
        Properties properties = new Properties();
        InputStream inputStream = new ClassPathResource(fileName).getInputStream();
        properties.load(inputStream);
        SpringApplication springApplication = new SpringApplication(OrderApp.class);
        springApplication.setDefaultProperties(properties);
        springApplication.run(args);
    }

    @Bean
    public RestTemplate restTemplate(){
        return new RestTemplate();
    }
}
