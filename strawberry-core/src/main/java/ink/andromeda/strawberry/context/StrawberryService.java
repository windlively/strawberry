package ink.andromeda.strawberry.context;

import org.springframework.stereotype.Service;

import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 应用程序的核心服务
 */
@Service
public class StrawberryService {

    // 执行小任务的线程池
    public static final ThreadPoolExecutor SIMPLE_TASK_POOL_EXECUTOR = new ThreadPoolExecutor(
            10, 200, 60, TimeUnit.SECONDS, new SynchronousQueue<>()
    );

}
