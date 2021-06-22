import me.iroohom.config.Config;
import me.iroohom.kuduApi.KuduAgent;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.junit.Test;

import java.util.Properties;

public class KuduTableDetailTest {
    @Test
    public void TestKudu() throws KuduException {
        Properties properties = Config.getProperties("conf");
        System.out.println(properties.getProperty("kudu.master"));
        KuduClient kuduClient = new KuduClient
                .KuduClientBuilder(properties.getProperty("kudu.master"))
                .build();
        KuduAgent kuduAgent = new KuduAgent();
        kuduAgent.setClient(kuduClient);

        kuduAgent.detailOpen("sa_mos-tcrm-mysql-main_tr_party_account_role_copy1_rt");

    }

}
