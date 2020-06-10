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
    public void joinQueryTest() throws Exception {

        String sql = "SELECT * FROM master.core_system.core_order co " +
                     " left JOIN capital.capital.zfpt_user_repayment_plan zurp ON co.channel_order_no=zurp.channel_order_no AND co.period =zurp.period " +
                     " full JOIN mobile_card.mobile_card.mcc_customer_consume mcc ON mcc.sequence_number = co.channel_order_no  " +
                     "WHERE mcc.repayment_status = 2 AND co.channel_order_no IN ('LOAN360API2019011630081596979',\n" +
                     "'RONG3602019011629092362260',\n" +
                     "'ZZD_SD2019011691444772331',\n" +
                     "'JDQ2019011629165068163',\n" +
                     "'SDN2019011629735814010',\n" +
                     "'LOAN360API2019011630157383456',\n" +
                     "'NMD2019011625998111334',\n" +
                     "'HKX2019011627009781191',\n" +
                     "'JDQ2019011623179015859',\n" +
                     "'NMD2019011603312383670',\n" +
                     "'NMD2019011695266340823',\n" +
                     "'ZZD_SD2019011630164622425',\n" +
                     "'NMD2019011626017516966',\n" +
                     "'HKX2019011405889294662',\n" +
                     "'HKX2019011626629678117',\n" +
                     "'NMD2019011626672147553',\n" +
                     "'SD_XRZSTD2019011630175571674',\n" +
                     "'ZZD_SD2019011630175630971',\n" +
                     "'NMD2019011626170908910',\n" +
                     "'SDN2019011621304038729',\n" +
                     "'SDN2019011627206072044',\n" +
                     "'SDN2019011627222053478',\n" +
                     "'NMD2019011627240836250',\n" +
                     "'NMD2019011627241396239',\n" +
                     "'ZZD_SD2019011627843594627',\n" +
                     "'NMD2019011621952748817',\n" +
                     "'NMD2019011618522235158',\n" +
                     "'NMD2019011580371258150',\n" +
                     "'SDN2019011628388567740',\n" +
                     "'SDN2019011628796393340',\n" +
                     "'NMD2019011213373823341',\n" +
                     "'SDN2019011628131712328',\n" +
                     "'SDN2019011627257145886',\n" +
                     "'NMD2019011627266333592',\n" +
                     "'NMD2019011627350606154',\n" +
                     "'HKX2019011625844568740',\n" +
                     "'SDN2019011629318903820',\n" +
                     "'SDN2019011627439264456',\n" +
                     "'ZZD_SD2019011628988685868',\n" +
                     "'NMD2019011627446076233',\n" +
                     "'DMD2019011627451635177',\n" +
                     "'SDN2019011629582803930',\n" +
                     "'NMD2019011629572424069',\n" +
                     "'SDN2019011629692709078',\n" +
                     "'ZZD_SD2019011629017797190',\n" +
                     "'DMD2019011627457617550',\n" +
                     "'HKX2019011626085243788',\n" +
                     "'SDN2019011627474873585',\n" +
                     "'SDN2019011629697675903',\n" +
                     "'NMD2019011627478391190',\n" +
                     "'NMD2019011627493720408',\n" +
                     "'HKX2019011625980717319',\n" +
                     "'SDN2019011629737026028',\n" +
                     "'SDN2019011627521518362',\n" +
                     "'SDN2019011629801251285',\n" +
                     "'NMD2019011627538547101',\n" +
                     "'DMD2019011630187410367',\n" +
                     "'NMD2019011627538723026',\n" +
                     "'ZZD_SD2019011629318484622',\n" +
                     "'ZZD_SD2019011629335983538',\n" +
                     "'SDN2019011627581167098',\n" +
                     "'NMD2019011627597451670',\n" +
                     "'NMD2019011627613923662',\n" +
                     "'ZZD_SD2019011629131073153',\n" +
                     "'SDN2019011627623725618',\n" +
                     "'SDN2019011627644568351',\n" +
                     "'SDN2019011627654732821',\n" +
                     "'ZZD_SD2019011629098736478',\n" +
                     "'HKG2019011558025798744',\n" +
                     "'SDN2019011627663393221',\n" +
                     "'SD_XRZSTD2019011628008072059',\n" +
                     "'SDN2019011627665634613',\n" +
                     "'ZZD_SD2019011629422951757',\n" +
                     "'NMD2019011627686607711',\n" +
                     "'SDN2019011627687516812',\n" +
                     "'ZZD_SD2019011629344331654',\n" +
                     "'ZZD_SD2019011629247762195',\n" +
                     "'NMD2019011627733043618',\n" +
                     "'ZZD_SD2019011628291473400',\n" +
                     "'NMD2019011627748153265',\n" +
                     "'SDN2019011627758429134',\n" +
                     "'ZZD_SD2019011629133488796',\n" +
                     "'SDN2019011627764851727',\n" +
                     "'NMD2019011627765792223',\n" +
                     "'SD_XRZSTD2019011628944851737',\n" +
                     "'ZZD_SD2019011629249328285',\n" +
                     "'NMD2019011627770103734',\n" +
                     "'ZZD_SD2019011629439039969',\n" +
                     "'SDN2019011627771191052',\n" +
                     "'SD_XRZSTD2019011623996197715',\n" +
                     "'NMD2019011627776704283',\n" +
                     "'SDN2019011627779067691',\n" +
                     "'HKX2019011626361959953',\n" +
                     "'LOAN360API2019011630191343816',\n" +
                     "'NMD2019011627779663470',\n" +
                     "'RONG3602019011572858476115',\n" +
                     "'SDN2019011627786203832',\n" +
                     "'ZZD_SD2019011628322830797',\n" +
                     "'SDN2019011627808307069',\n" +
                     "'ZZD_SD2019011530965836268',\n" +
                     "'SDN2019011627810153326',\n" +
                     "'ZZD_SD2019011628130328975',\n" +
                     "'NMD2019011627849023515',\n" +
                     "'ZZD_SD2019011629444773845',\n" +
                     "'SDN2019011627852189990',\n" +
                     "'SDN2019011627858783441',\n" +
                     "'ZZD_SD2019011627833340404',\n" +
                     "'ZZD_SD2019011628371948408',\n" +
                     "'ZZD_SD2019011629147998761',\n" +
                     "'NMD2019011627880173876',\n" +
                     "'SDN2019011627878404436',\n" +
                     "'SDN2019011627885784083',\n" +
                     "'ZZD_SD2019011627890189864',\n" +
                     "'XJBKAPI2019011341615441576',\n" +
                     "'ZZD_SD2019011628065513248',\n" +
                     "'SDN2019011627900557397',\n" +
                     "'SDN2019011627886675759',\n" +
                     "'SD_XRZSTD2019011628638056622',\n" +
                     "'NMD2019011627912522882',\n" +
                     "'DMD2019011627912828184',\n" +
                     "'NMD2019011627940190192',\n" +
                     "'SDN2019011627948450440',\n" +
                     "'SDN2019011627966406548',\n" +
                     "'SDN2019011627992011321',\n" +
                     "'NMD2019011628002969204',\n" +
                     "'ZZD_SD2019011628074823084',\n" +
                     "'SDN2019011628008162294',\n" +
                     "'SDN2019011628009810453',\n" +
                     "'RONG3602019011097262860535',\n" +
                     "'ZZD_SD2019011629468269686',\n" +
                     "'NMD2019011628014857721',\n" +
                     "'NMD2019011628039489812',\n" +
                     "'SDN2019011628043849680',\n" +
                     "'NMD2019011628062757572',\n" +
                     "'SDN2019011628077322026',\n" +
                     "'ZZD_SD2019011629192838515',\n" +
                     "'DMD2019011625910229475',\n" +
                     "'DMD2019011628102628074',\n" +
                     "'NMD2019011628110585197',\n" +
                     "'NMD2019011628123345394',\n" +
                     "'SDN2019011628310124817',\n" +
                     "'SDN2019011628328622385',\n" +
                     "'SDN2019011628333513990',\n" +
                     "'SD_XRZSTD2019011627530267460',\n" +
                     "'SDN2019011628342525261',\n" +
                     "'ZZD_SD2019011624062428879',\n" +
                     "'SDN2019011628344007290',\n" +
                     "'SDN2019011628350528783',\n" +
                     "'ZZD_SD2019011627893452470',\n" +
                     "'DMD2019011628374466910',\n" +
                     "'SD_XRZSTD2019011628745006520',\n" +
                     "'SDN2019011628381328568',\n" +
                     "'SDN2019011628384576943',\n" +
                     "'ZZD_SD2019011627924589869',\n" +
                     "'SDN2019011628418345167',\n" +
                     "'HKX2019011582788736823',\n" +
                     "'SDN2019011629490542722',\n" +
                     "'NMD2019011628418178631',\n" +
                     "'NMD2019011628444218285',\n" +
                     "'SDN2019011628458739659',\n" +
                     "'NMD2019011628472519223',\n" +
                     "'ZZD_SD2019011618098316526',\n" +
                     "'SDN2019011617344845903',\n" +
                     "'SDN2019011619958291719',\n" +
                     "'ZZD_SD2019011628030896495',\n" +
                     "'ZZD_SD2019011620045468649')";
        VirtualDataSet virtualDataSet = new VirtualDataSet(0, "ss", null, null);

        virtualDataSet.setDataSourceMap(dynamicDataSource.getIncludedDataSource());
        // virtualDataSet.setBaseQueryCount(50);
        virtualDataSet.executeQuery(sql, 800);
    }

    @Test
    public void joinTest2() throws Exception {
        String sql = "select * from master.test.join_test_a ta " +
                     "left join master.test.join_test_b tb on ta.f1=tb.f2 " +
                     //"join master.test.join_test_a td on td.f1=tb.f2 " +
                     " join master.test.join_test_c tc on tc.f1=tb.f1 " +
                     "where ta.f1=0";
        log.info(sql);
        VirtualDataSet virtualDataSet = new VirtualDataSet(0, "ss", null, null);
        virtualDataSet.setDataSourceMap(dynamicDataSource.getIncludedDataSource());
        // virtualDataSet.setBaseQueryCount(50);
        virtualDataSet.executeQuery(sql, 800);
    }
}
