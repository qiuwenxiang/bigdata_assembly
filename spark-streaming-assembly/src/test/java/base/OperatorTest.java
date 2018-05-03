package base;

import com.kylin.assembly.spark.operators.ActionLearning;
import com.kylin.assembly.spark.operators.KVOperatorsLearning;
import com.kylin.assembly.spark.operators.OperatorsLearning;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @description: 单元测试
 * @author: kylin.qiuwx@foxmail.com
 * @create: 2018-05-03
 **/
public class OperatorTest {

    @Test
    public void countByKeyOper(){
        KVOperatorsLearning.countByKeyOper();
    }

    @Test
    public void foreachOper(){
        ActionLearning.foreachOper();
    }

    @Test
    public void saveAsTextFileOper(){
        ActionLearning.saveAsTextFileOper();
    }
}
