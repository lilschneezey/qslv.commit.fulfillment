package qslv.transaction.fulfillment;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class FulfillCommitApplication {
	
	public static void main(String[] args) {
		SpringApplication application = new SpringApplication(FulfillCommitApplication.class);
        application.run(args);
	}

}
