package ink.andromeda.strawberry.test;

import ink.andromeda.strawberry.core.DynamicDataSource;
import ink.andromeda.strawberry.core.StrawberryAutoConfiguration;
import ink.andromeda.strawberry.core.StrawberryService;
import ink.andromeda.strawberry.core.VirtualDataSet;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = StrawberryAutoConfiguration.class)
@Slf4j
public class ContextServiceTest {

    @Autowired
    StrawberryService strawberryService;

    @Autowired
    DynamicDataSource dynamicDataSource;

    @Test
    public void test(){

    }

    @Test
    public void joinQueryTest(){

        String sql = "SELECT * FROM master.core_system.core_order co " +
                     "JOIN capital.capital.zfpt_user_repayment_plan zurp ON co.channel_order_no=zurp.channel_order_no " +
                     "JOIN mobile_card.mobile_card.mcc_customer_consume mcc ON mcc.sequence_number = zurp.channel_order_no " +
                     "WHERE zurp.channel_order_no = '' AND mcc.repayment_status=2";
        VirtualDataSet virtualDataSet = new VirtualDataSet(0, "ss", null, null);

        virtualDataSet.setDataSourceMap(dynamicDataSource.getIncludedDataSource());

        virtualDataSet.executeQuery(sql);
    }

}
