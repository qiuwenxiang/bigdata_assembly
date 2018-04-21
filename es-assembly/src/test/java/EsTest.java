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
        String index="public_sentiment";
        String type="public_sentiment-2018";
        String json="{\"type\":\"mouth_quality\",\"id\":18782951022,\"sys_date1\":\"2018-04-21\",\"sys_date2\":\"1524296842035\",\"sys_date4\":1524296842}";
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
