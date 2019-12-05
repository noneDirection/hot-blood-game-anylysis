import org.junit.Test;
import com.qf.utils.DBCPUtil;
import org.apache.commons.dbutils.QueryRunner;
import org.junit.Before;

import java.sql.SQLException;
/**
 * @program: hotbloodgameanylysis
 * @description: 测试DBCPUtil工具类
 * @author: youzhao
 * @create: 2019-12-05 13:55
 *
 **/
public class DBCPUtilTest {
    private QueryRunner qr;

    @Before
    public void init(){
        qr = new QueryRunner(DBCPUtil.getConnectionPool());
    }


    /**
     * 新增元素
     * @throws SQLException
     */
    @Test
    public void testSave() throws SQLException {
        int cnt = qr.update("insert into test_db values(?,?)", "1","舍我其谁");
        System.out.println(cnt>0?"恭喜！操作成功！":"抱歉，操作失败！");
    }

    /**
     * 删除元素
     * @throws SQLException
     */
}
