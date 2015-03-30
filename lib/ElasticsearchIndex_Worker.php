<?php
/* vim: set expandtab tabstop=4 shiftwidth=4 softtabstop=4: */
/**
 * Elasticsearch Indexer - Worker
 * 
 * 
 * 
 * 
 * 
 * 
 * 
 * PHP 5.4 Later
 * 
 * @final
 * @category   lib
 * @package    ElasticsearchIndex
 * @author     Junyong Park
 * @copyright  2013
 * @version    SVN: $Id:$
 */

require_once('ElasticsearchIndex_Helper.php');
require_once('Elasticsearch_Requester.php');


final class ElasticsearchIndex_Worker
{
    /**
     * ES_RETRY
     * 
     * @access const
     * @var    integer
     */
    const ES_RETRY = 5;

    /**
     * ES_RETRY_TERM
     * (msec)
     * 
     * @access const
     * @var    integer
     */
    const ES_RETRY_TERM = 500;

    /**
     * _socket
     * 
     * @access private
     * @var    array
     */
    private $_socket = [];

    /**
     * _taskType
     * 
     * @access private
     * @var    string
     */
    private $_taskType = NULL;

    /**
     * _queue
     * 
     * @access private
     * @var    integer
     */
    private $_queue = NULL;

    /**
     * _workerNumber
     * 
     * @access private
     * @var    mixed
     */
    private $_workerNumber = NULL;


    /**
     * Construct
     * 
     * @access public
     * @param  integer $queue
     * @param  integer $workerNumber
     * @param  string $taskType [Optional]
     * @throws UnexpectedValueException
     */
    public function __construct( $queue, $workerNumber, $taskType=NULL )
    {
        // check parameter
        if( !is_int($queue) ) throw new UnexpectedValueException('invalid queue format');
        if( !is_int($workerNumber) ) throw new UnexpectedValueException('invalid worker number format');
        if( !is_null($taskType) && !is_scalar($taskType) ) throw new UnexpectedValueException('invalid task_type format');
 
        // set parameter
        $this->_queue = $queue;
        $this->_workerNumber = $workerNumber;
        $this->_taskType = $taskType;
    }

    /**
     * open
     * 
     * @access public
     * @param  array $zmqDsn
     * @throws LogicException
     * @throws ZMQContextException
     * @throws ZMQSocketException
     * @return boolean 
     */
    public function open( array $zmqDsn )
    {
        // create context instance
        $ctx = new ZMQContext();

        // create socket instance
        $this->_socket['ventilator'] = new ZMQSocket($ctx, ZMQ::SOCKET_PULL);
        $this->_socket['sink'] = new ZMQSocket($ctx, ZMQ::SOCKET_PUSH);
        $this->_socket['controller'] = new ZMQSocket($ctx, ZMQ::SOCKET_SUB);

        // connect
        $this->_socket['ventilator']->connect(@$zmqDsn['ventilator']);
        $this->_socket['sink']->connect(@$zmqDsn['sink']);
        $this->_socket['controller']->connect(@$zmqDsn['controller']);

        // set socket option
//         $this->_socket['sink']->setSockOpt(ZMQ::SOCKOPT_LINGER, 0);
        $this->_socket['controller']->setSockOpt(ZMQ::SOCKOPT_SUBSCRIBE, NULL);

        return TRUE;
    }

    /**
     * run
     * 
     * @access public
     * @param  Elasticsearch_Dsn $esDsnObj
     * @param  BaseMapper $mapper
     * @throws InvalidArgumentException
     * @throws ZMQSocketException
     * @throws ZMQPollException
     * @return boolean
     */
    public function run( Elasticsearch_Dsn $esDsnObj, BaseMapper $mapper )
    {
        // check socket
        if( !@$this->_socket['ventilator'] instanceof ZMQSocket ||
            !@$this->_socket['sink'] instanceof ZMQSocket ||
            !@$this->_socket['controller'] instanceof ZMQSocket )
            throw new InvalidArgumentException('Socket did not open'); 

        // process error message from receiver and controller 
        $poll = new ZMQPoll();
        $poll->add($this->_socket['ventilator'], ZMQ::POLL_IN);
        $poll->add($this->_socket['controller'], ZMQ::POLL_IN);
        $readable = $writable = [];
        $status = TRUE;

        // requester
        $requester = new Elasticsearch_Requester;

        // Process tasks
        while( 1 )
        {
//             $events = $poll->poll($readable, $writable, 500);
            $events = $poll->poll($readable, $writable);
            if( $events > 0 )
            {
                foreach( $readable as $socket )
                {
                    // from ventilator
                    if( $socket === $this->_socket['ventilator'] )
                    {
                        // receive data from ventilator
                        $string = $this->_socket['ventilator']->recv();

                        // start
                        if( $string == ElasticsearchIndex_Helper::MSG_TASK_START )
                        {
                            // initialize
                            $kvQueue = [];
                            $idx = 0;

                            // open requester
                            $requester->open();
                            $requester->setOptions([CURLOPT_TIMEOUT => 200, CURLOPT_PUT => 1]);

                            // send data to sink
                            $this->_displayMsg('Tasks start');
                            $this->_socket['sink']->send($string);
                        }
                        // end
                        elseif( $string == ElasticsearchIndex_Helper::MSG_TASK_END )
                        {
                            // send remained data in queue to elasticsearch
                            if( count($kvQueue) )
                            {
                                // send data to elasticsearch 
                                $ret = $this->_send2Es($esDsnObj, $requester, $kvQueue);

                                // error
                                if( !$ret )
                                {
                                    // send data to sink
                                    $this->_displayMsg('Tasks aborted');
                                    $this->_socket['sink']->send(ElasticsearchIndex_Helper::MSG_TASK_ERROR);
                                    usleep(5000);
                                    $this->_socket['sink']->send($string);
                                    $status = FALSE;

                                    // free
                                    unset($kvQueue);
                                    $requester->close();
                                    break 2;
                                }
                            }

                            // send data to sink
                            $this->_displayMsg('Tasks end');
                            $this->_socket['sink']->send($string);

                            // free
                            unset($kvQueue);
                            $requester->close();
                            break 2;
                        }
                        // normal
                        else
                        {
                            $idx++;

                            // unserialize, store data in queue
                            $lineArr = unserialize($string);
                            unset($string);
                            $kvQueue += $mapper->convert($lineArr);

                            // check size of queue and send to elasticsearch
                            if( !($idx % $this->_queue) )
                            {
                                // send data to elasticsearch 
                                $ret = $this->_send2Es($esDsnObj, $requester, $kvQueue);

                                // initialize
                                $kvQueue = [];
                                $idx = 0;

                                // error
                                if( !$ret )
                                {
                                    // send data to sink
                                    $this->_displayMsg('Tasks aborted');
                                    $this->_socket['sink']->send(ElasticsearchIndex_Helper::MSG_TASK_ERROR);
                                    usleep(5000);
                                    $this->_socket['sink']->send(ElasticsearchIndex_Helper::MSG_TASK_END);
                                    $status = FALSE;

                                    // free
                                    $requester->close();
                                    break 2;
                                }
                            }

                            // send data to sink
                            $this->_socket['sink']->send('');
                        }
                    }
                    // from sink
                    elseif( $socket === $this->_socket['controller'] )
                    {
                        // receive data from sink
                        $string = $this->_socket['controller']->recv();

                        // kill(error)
                        if( $string == ElasticsearchIndex_Helper::MSG_TASK_KILL )
                        {
                            $this->_displayMsg('Tasks aborted by controller');
                            $this->_socket['sink']->send(ElasticsearchIndex_Helper::MSG_TASK_END);

                            // free
                            unset($kvQueue);
                            $requester->close();
                            break 2;
                        }
                    }
                }
            }
        }

        return $status;
    }

    /**
     * close
     * 
     * @access public
     * @return boolean
     */
    public function close()
    {
        // close
        unset($this->_socket['ventilator'], $this->_socket['sink'], $this->_socket['controller']);

        return TRUE;
    }

    /**
     * _send2Es
     * 
     * @access private
     * @param  Elasticsearch_Dsn $esDsnObj
     * @param  Elasticsearch_Requester $requester
     * @param  array &$data
     * @return boolean|number
     */
    private function _send2Es( Elasticsearch_Dsn $esDsnObj, Elasticsearch_Requester $requester, array &$data )
    {
        $retryCnt = 0;
        $requester->setOptions([CURLOPT_URL => 'http://'.$esDsnObj->getHost($this->_workerNumber-1).'/'.$esDsnObj->getIndex().'/_bulk/'.$esDsnObj->getParam()]);

        while( $retryCnt < self::ES_RETRY )
        {
            // convert array to string
            $content = implode(PHP_EOL, $data) . PHP_EOL;
            $contentLen = strlen($content);

            // store in memory
            $fm = fopen('php://memory','r+');
            fwrite($fm, $content);
            rewind($fm);

            // free
            unset($content);

            // request
            $res = $requester->request([CURLOPT_INFILESIZE => $contentLen, CURLOPT_INFILE => $fm], FALSE);

            // free
            unset($contentLen);
            fclose($fm);

            // check respose
            if( $res === FALSE )
            {
                $this->_displayMsg('Failed to connect elasticsearch');

                return FALSE;
            }

            /*
             * v1.0.0-
             */
/*
            // parse result
            $ret = json_decode($res, TRUE);
            unset($res);

            // check response
            foreach( $ret['items'] as $value )
            {
                $type = key($value);
                if( isset($value[$type]['ok']) || ($type == 'update' || $type == 'delete') ) 
                    unset($data[$value[$type]['_type'].$value[$type]['_id']]);
            }
*/
            /*
             * v1.0.0+
             * 
             * example of response.
             * {
             *     "took":8,
             *     "errors":false,
             *     "items":[
             *         {"index":{"_index":"category.1","_type":"old","_id":"7070","_version":1,"status":201}},
             *         {"index":{"_index":"category.1","_type":"old","_id":"7074","_version":1,"status":201}},
             *         {"index":{"_index":"category.1","_type":"old","_id":"7078","_version":1,"status":201}}
             *     ]
             * }
             */
            // parse result 
            $ret = json_decode($res, TRUE);
            unset($res);

            // check response
            if( !isset($ret['error']) || !$ret['error'] )
            {
                unset($data);
                $data = [];
            }
            elseif( isset($ret['items']) )
            {
                foreach( $ret['items'] as $value )
                {
                    $type = key($value);
                    if( isset($value[$type]['status']) && (
                        $value[$type]['status'] == 201 ||
                        $value[$type]['status'] == 200 ||
                        $value[$type]['status'] == 404 ||
                        $value[$type]['status'] == 409) )
                        unset($data[$value[$type]['_type'].$value[$type]['_id']]);
                }
            }

            // free
            unset($ret);

            // check data
            if( !count($data) ) break;

            $retryCnt++;
            usleep(self::ES_RETRY_TERM * 1000);
        }

        // check retry count
        if( $retryCnt >= self::ES_RETRY )
        {
            $this->_displayMsg("$retryCnt retried");
            $this->_displayMsg('DEBUG - '.implode(',', array_keys($data)));

            return FALSE;
        }
        elseif( $retryCnt > 0 ) $this->_displayMsg("$retryCnt retried");

        return TRUE;
    }

    /**
     * _displayMsg
     * 
     * @access private
     * @param  string $msg
     * @return void
     */
    private function _displayMsg( $msg )
    {
        ElasticsearchIndex_Helper::displayMsg($this->_taskType.'::worker'.$this->_workerNumber.'::'.$msg);
    }
}
