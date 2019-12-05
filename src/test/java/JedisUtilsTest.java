/**
 * @program: hotbloodgameanylysis
 * @description: TODO
 * @author: youzhao
 * @create: 2019-12-05 15:32
 **/
import com.qf.utils.JedisUtils;
import org.junit.Test;
import redis.clients.jedis.Jedis;
public class JedisUtilsTest {
    @Test
    public void testJedis(){
        Jedis pool = JedisUtils.getJedisInstanceFromPool();
        String result = pool.set("score", "98.7");
        System.out.println(result);
    }
}
