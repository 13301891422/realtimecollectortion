import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import realtime.To_CK;

/**
 * @author xuweiwei
 * @version 1.0
 * @todo 2020/12/28 13:17:45
 */
public class LogTest {
    public static void main(String[] args) {
        final Logger log = LoggerFactory.getLogger(To_CK.class);
        log.error("heha");
    }
}
