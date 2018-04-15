package zkclient;

import com.kylin.assembly.common.GlobalParamValue;
import com.kylin.assembly.common.constant.ZkConstant;
import com.kylin.assembly.common.po.User;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.serialize.SerializableSerializer;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * ZkClient 开源客户端api操作
 */
public class ZkClientApi
{
    private static Logger LOGGER = LoggerFactory.getLogger(ZkClientApi.class);
    public static ZkClient zc;

    static {
        zc = new ZkClient(GlobalParamValue.get(ZkConstant.ZOOKEEPER_CONNECT),10000,10000, new SerializableSerializer());
        LOGGER.info("create client[{}] success :",GlobalParamValue.get(ZkConstant.ZOOKEEPER_CONNECT));
    }

    public static void main(String[] args)
    {
        //初始化参数，ip端口，回话过期的时间，链接超时的时间,序列化器
        zc = new ZkClient(GlobalParamValue.get(ZkConstant.ZOOKEEPER_CONNECT),10000,10000, new SerializableSerializer());
        System.out.println("connection ok");
//        createNode();
    }

    /**
     * 创建节点，路径，节点内容，持久节点
     * @return
     */
    public static String createNode(String path,Object obj)
    {
        //创建节点，路径，节点内容，持久节点
        return zc.create(path, obj, CreateMode.PERSISTENT);
    }

    /**
     * 获取节点信息和状态信息
     * @param path
     */
    public static void getDAta(String path)
    {
        //返回的是user对象，获取节点内容
        User user = zc.readData(path);

        //获取状态信息
        Stat stat = new Stat();
        zc.readData(path, stat);
        System.out.println(stat);
    }

    /**
     * 获取子节点信息
     * @param path
     */
    public static void getChildPath(String path)
    {
        //返回子节点列表
        List<String> listPath = zc.getChildren(path);
        for (String s : listPath)
        {
            System.out.println(s);
        }
    }

    /**
     *节点是否存在
     */
    public static void existsNode(String path)
    {
        boolean flag = zc.exists(path);
    }

    /**
     * 删除节点
     * @param path
     */
    public static void delPath(String path)
    {
        //删除没有子节点的路劲
        boolean x = zc.delete(path);

        //删除有子节点的路劲
        boolean y = zc.deleteRecursive(path);
    }
}
