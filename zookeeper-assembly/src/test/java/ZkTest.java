import com.kylin.assembly.common.po.User;
import org.I0Itec.zkclient.ZkClient;
import org.junit.Assert;
import org.junit.Test;
import zkclient.ZkClientApi;

/**
 * @author kylin.qiuwx@foxmail.com
 * @Description:
 * @date 2018/4/15
 */
public class ZkTest {

    private static final String PATH="/hadoop";


    @Test
    public void testCreatClient(){
        ZkClient zc = ZkClientApi.zc;
        Assert.assertNotNull(zc);
    }

    @Test
    public void testCreatClient1(){
        String node = ZkClientApi.createNode(PATH,new User(1,"zhangsan"));
        Assert.assertNotNull(node);
    }
}
