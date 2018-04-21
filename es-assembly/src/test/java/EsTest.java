import com.kylin.assembly.es.ESUtil;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * @author kylin.qiuwx@foxmail.com
 * @Description: 测试类
 * @date 2018/4/13
 */
public class EsTest {


    @Test
    public void testInsert(){
        long time_millis = System.currentTimeMillis();
        String timestamp = String.valueOf(time_millis/1000);
        long time = Integer.valueOf(timestamp);

        String index="public_sentiment";
        String type="public_sentiment-2018";
        String json="{\"type\":\"mouth_quality\"," +
                "\"create_time_none\":"+time+"," +
                "\"create_time\":"+time+"," +
                "\"create_time_millis\":"+time_millis+"," +
                "\"create_time_millis_none\":"+time_millis+"}";
        ESUtil.index(index, type, json,null);
    }

   /* @Test
    public void testupdate(){
        String index="public_sentiment";
        String type="public_sentiment-2018";
        Map<String,String> map=new HashMap();
        map.put("type","mouth_quality");
        map.put("id","18782951022");
        map.put("timestamp","1524293742496");
        ESUtil.index(index, type, map.toString(),null);
    }*/


}
