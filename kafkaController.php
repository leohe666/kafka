<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
// use Illuminate\Support\Facades\Log;
use App\Jobs\Queue;
class kafkaController extends Controller
{
    //

    public function send()
    {
       
        $this->kafkaMsg();
        // $this->receive();
        // sleep(3);
        // Log::info('send maill', ["mail info"=>[]]);

        // $this->dispatch(new Queue([date('Y-m-d H:i:s')]));
        // return response()->json(['status'  => '0', 'message' => '测试','result'=>""]);
    }
    //低级消费者手动指定partition并自动提交offset
    public function lowLevelReceiveMsgAutoSubmitOffset()
    {
        // 此例子主要使用场景为需要“消费指定partition的处理并且对消息丢失不重视的场景”，
        // 因采用了自动提交offset的设置，有可能会出现在消息处理逻辑未处理完成时异常退出，再消费时无法再继续处理丢失的消息
        // 设置将要消费消息的主题
        $conf = new RdKafka\Conf();
        // Set the group id. This is required when storing offsets on the broker
        $conf->set('group.id', 'testgroup');
        $rk = new RdKafka\Consumer($conf);
        $rk->addBrokers("192.168.0.30");
        $topicConf = new RdKafka\TopicConf();
        //自动提交offset的频率，下列设置表示100毫秒自动提交一次
        $topicConf->set('auto.commit.interval.ms', 100);

        // Set the offset store method to 'file'
        $topicConf->set('offset.store.method', 'broker');
        // Set where to start consuming messages when there is no initial offset in
        // offset store or the desired offset is out of range.
        // 'smallest': start from the beginning
        $topicConf->set('auto.offset.reset', 'smallest');
        $topic = $rk->newTopic("test", $topicConf);
        // Start consuming partition 0,如果要指定partition则修改0的值
        $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
        $errorMessage = null;
        $time = null;
        try {
            while (true) {
                $time = date('Y-m-d H:i:s');
                $message = $topic->consume(0, 120*10000);
                if (!empty($message)) {
                    $errorMessage = $message;
                    switch ($message->err) {
                        case RD_KAFKA_RESP_ERR_NO_ERROR:
                            $payload = json_decode($message->payload,true);
                            echo "received msg in low level".$message->payload." @partition:".$message->partition
                                ." @offset:".$message->offset." @key:".$message->key." time@".$time.PHP_EOL;
                            //todo 检测消息重复消费,未被消费时处理逻辑，消费过则记录日志并提醒开发者
                            break;
                        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                            echo "No more messages; will wait for more\n";
                            break;
                        case RD_KAFKA_RESP_ERR__TIMED_OUT:
                            echo "Timed out\n";
                            break;
                        default:
                            var_dump($message);
                            throw new \Exception($message->errstr(), $message->err);
                            break;
                    }
                }
            }
        } catch (\Exception $e) {
            //todo notice someone with email or sms
            file_put_contents('/tmp/kafka_error.log', "kafka consumer detail msg error in low level: message@"
            .var_export($errorMessage, true)
            ." error:".$e->getMessage()." time@".$time.PHP_EOL, FILE_APPEND);
        } finally {
            file_put_contents('/tmp/kafka_error.log', "kafka consumer closed in low level@time:".$time.PHP_EOL, FILE_APPEND);
        }
    }
    //高级消费者自动分配partition手动提交offset
    public function HighLevelReceiveMsgSubmitOffset()
    {
        // 此例子主要使用场景为需要“自动分配partition的处理并且是手动提交offset,当消息处理异常时，当前消费都关闭，需要检测异常并手动再次启动”，
        // 设置将要消费消息的主题
        $topic = 'test';
        $host = '192.168.0.30';
        $group_id = 'testgroup';
        $conf = new \RdKafka\Conf();

        $conf->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
            switch ($err) {
                case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
                    //检测到发生了Rebalance开始时间
                    echo "setRebalanceCb Assign: \n"; 
                    $kafka->assign($partitions);
                    break;
                case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
                    //Rebalance重新分配完毕
                    echo "setRebalanceCb Revoke: \n";
                    $kafka->assign(NULL);
                    break;
                default:
                    throw new \Exception($err);
            }
        });

        // 配置groud.id 具有相同 group.id 的consumer 将会处理不同分区的消息，
        // 所以同一个组内的消费者数量如果订阅了一个topic，
        // 那么消费者进程的数量多于 多于这个topic 分区的数量是没有意义的。
        $conf->set('group.id', $group_id);

        // 添加 kafka集群服务器地址
        $conf->set('metadata.broker.list', $host); //'localhost:9092,localhost:9093,localhost:9094,localhost:9095'

        // 针对低延迟进行了优化的配置。这允许PHP进程/请求尽快发送消息并快速终止
        $conf->set('fetch.wait.max.ms', 50);
        $conf->set('socket.timeout.ms', 1050);
        //多进程和信号
        if (function_exists('pcntl_sigprocmask')) {
            pcntl_sigprocmask(SIG_BLOCK, array(SIGIO));
            $conf->set('internal.termination.signal', SIGIO);
        } else {
            $conf->set('queue.buffering.max.ms', 1);
        }
        //smallest：简单理解为从头开始消费，largest：简单理解为从最新的开始消费
        $conf->set('auto.offset.reset', 'smallest');

        // 在interval.ms的时间内自动提交确认、建议不要启动, 1是启动，0是未启动
        $conf->set('enable.auto.offset.store', 0);//关闭自动提交
        $conf->set('auto.commit.interval.ms', 0);//禁用自动提交

        $consumer = new \RdKafka\KafkaConsumer($conf);

        // 更新订阅集（自动分配partitions ）
        $consumer->subscribe([$topic]);
        //指定topic分配partitions使用那个分区,但必须要传递指定partition的offset，从传入的offset开始读取,不传则从0开始。使用场景是知道消息所在partition的offset？
        // $consumer->assign([
        //     new \RdKafka\TopicPartition($topic, 0, $partitionOffset),//partitionOffset暂时不知道如何获取，
        //     new \RdKafka\TopicPartition($topic, 1, $partitionOffset),
        // ]);
        $errorMessage = null;
        $time = null;
        try {
            while (true) {
                $time = date('Y-m-d H:i:s');
                $message = $consumer->consume(3 * 1000);//设置120s为超时
                if (!empty($message)) {
                    $errorMessage = $message;
                    switch ($message->err) {
                        case RD_KAFKA_RESP_ERR_NO_ERROR:
                            //根据kafka中不同key，调用对应方法传递处理数据*（如果有必要的话）
                            //对该条message进行处理，比如用户数据同步， 记录日志。
                            //拆解对象为数组，并根据业务需求处理数据
                            $payload = json_decode($message->payload,true);
                            echo "received msg in high level".$message->payload." @partition:".$message->partition
                                ." @offset:".$message->offset." @key:".$message->key." time@".$time.PHP_EOL;
                            //todo 检测消息重复消费,未被消费时处理逻辑交提交offset，消费过则记录日志并提醒开发者继续下一个消费
                            $consumer->commitAsync($message);//Throws RdKafka\Exception on errors.
                            break;
                        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                            //$msg = "No more messages; will wait for more\n";
                            break;
                        case RD_KAFKA_RESP_ERR__TIMED_OUT:
                            //$msg = "Timed out ##################\n";
                            break;
                        default:
                            throw new \Exception($message->errstr(), $message->err);
                            break;
                    }

                }
            }
        } catch (\Exception $e) {
            //todo notice someone with email or sms
            file_put_contents('/tmp/kafka_error.log', "kafka consumer detail msg error in high level: message@"
            .var_export($errorMessage, true)
            ." error:".$e->getMessage()." time@".$time.PHP_EOL, FILE_APPEND);
        } finally {
            file_put_contents('/tmp/kafka_error.log', "kafka consumer closed in high level@time:".$time.PHP_EOL, FILE_APPEND);
            //如果有异常则将消费者关闭
            $consumer->close();
        }
    }

    //发送消息
    public function kafkaMsg()
    {
        $conf = new \RdKafka\Conf();
        $conf->set('metadata.broker.list', '192.168.0.30:9092,192.168.0.31:9092,192.168.0.32:9092');

        //If you need to produce exactly once and want to keep the original produce order, uncomment the line below
        //$conf->set('enable.idempotence', 'true');

        $producer = new \RdKafka\Producer($conf);

        $topic = $producer->newTopic("test");

        for ($i = 0; $i < 7; $i++) {
            $key =null;
            if (in_array($i, [0,2,3])) {
                $key = 'same partition key';
            } 
            if ($i == 5) {
                $key = '5';
            }
            $topic->produce(RD_KAFKA_PARTITION_UA, 0, "Message1 $i ".date('Y-m-d H:i:s'), $key);
            $producer->poll(0);
        }

        for ($flushRetries = 0; $flushRetries < 2; $flushRetries++) {
            $result = $producer->flush(10000);
            if (RD_KAFKA_RESP_ERR_NO_ERROR === $result) {
                break;
            }
        }

        if (RD_KAFKA_RESP_ERR_NO_ERROR !== $result) {
            throw new \RuntimeException('Was unable to flush, messages might be lost!');
        }
        echo "done\n";
    }
}
