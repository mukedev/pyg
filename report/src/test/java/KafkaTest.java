import com.itheima.report.ReportApplication;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * @author zhangYu
 * @date 2020/10/31
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = ReportApplication.class)
public class KafkaTest {

    @Autowired
    KafkaTemplate kafkaTemplate;

    @Test
    public void sendTest() {
        for (int i = 0; i < 100; i++) {
            kafkaTemplate.send("test2", "key", "producer msg--" + i);
        }
    }


}
