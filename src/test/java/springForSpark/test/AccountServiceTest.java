package springForSpark.test;

import com.SpringAndSpark.service.IAccountService;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit.jupiter.SpringJUnitConfig;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
public class AccountServiceTest {

    @Autowired
    IAccountService service = null;

    @Test
    public void sparkTest() {
        service.queryAccount();
    }
}
