import com.kylin.assembly.common.po.User;
import org.I0Itec.zkclient.ZkClient;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import zkclient.ZkClientApi;

/**
 * @author kylin.qiuwx@foxmail.com
 * @Description:
 * @date 2018/4/15
 */
public class ZkTest {

    private static final String PATH="/hadoop";
    private static final String PATH1="/hadoop1";
    private static final String PATH2="/hadoop2";
    private static final String PATH3="/test";
    private static final String PATH4="/consumers/qwx";


    @Before
    public void init(){
       // testDeleteZNode();
    }

    @Test
    public void testCreatClient(){
        ZkClient zc = ZkClientApi.zc;
        Assert.assertNotNull(zc);
    }


    @Test
    public void testDeleteZNode(){
         ZkClientApi.delPath(PATH4);
    }

    @Test
    public void testdeleteRecursive(){
         ZkClientApi.deleteRecursive(PATH4);
    }

    @Test
    public void testCreatClient1(){
        String node = ZkClientApi.createNode(PATH,new User(1,"zhangsan"));
        String node1 = ZkClientApi.createNode(PATH1,new User(1,"zhangsan"), CreateMode.PERSISTENT_SEQUENTIAL);
        String node2 = ZkClientApi.createNode(PATH2,new User(1,"zhangsan"), CreateMode.EPHEMERAL);
        String node3 = ZkClientApi.createNode(PATH3,new User(1,"zhangsan"), CreateMode.EPHEMERAL_SEQUENTIAL);
        Assert.assertNotNull(node);
        Assert.assertNotNull(node1);
    }

    @Test
    public void  testGetData(){
        Object data = ZkClientApi.getData("/consumers/1/offsets/test/1");
        System.out.println(data);
    }

    @Test
    public void  testGetDataWatch(){
      /*  Object data = ZkClientApi.getData(PATH,new WatchedEvent());
        System.out.println(data);*/
    }
}
